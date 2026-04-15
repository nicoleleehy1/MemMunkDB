package com.lsmtree.store;

import com.lsmtree.compaction.Compactor;
import com.lsmtree.memtable.MemTable;
import com.lsmtree.sstable.SSTable;
import com.lsmtree.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lsmtree.sstable.SSTableEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSMStore — a LevelDB-style Log-Structured Merge-Tree key-value store.
 *
 * <p>Write path: put/delete → WAL → MemTable → (threshold) → SSTable flush
 * <p>Read path:  MemTable → immutable MemTable (if flushing) → SSTables (newest first)
 * <p>Compaction: background size-tiered merge, run explicitly via {@link #compact()}.
 */
public class LSMStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LSMStore.class);

    /** Byte marker written to SSTables / WAL for tombstone entries. */
    public static final String TOMBSTONE = "\u0000__DELETED__\u0000";

    /** Flush the MemTable once it reaches this many entries. */
    private static final int MEMTABLE_FLUSH_THRESHOLD = 1_000;

    private final Path dataDir;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean closed = false;

    private MemTable memTable;
    private WriteAheadLog wal;
    /** Ordered newest-first: index 0 = most recent flush. */
    private final List<SSTable> ssTables = new ArrayList<>();
    private final Compactor compactor;

    // -------------------------------------------------------------------------
    // Construction / recovery
    // -------------------------------------------------------------------------

    public LSMStore(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        this.compactor = new Compactor(dataDir, ssTables, lock);
        recover();
    }

    /**
     * Replay the WAL and load existing SSTables from disk.
     */
    private void recover() throws IOException {
        // Load SSTables that were already flushed (sorted by sequence number desc)
        List<SSTable> loaded = SSTable.loadAll(dataDir);
        ssTables.addAll(loaded);
        log.info("Recovered {} SSTables from {}", loaded.size(), dataDir);

        // Replay WAL into a fresh MemTable
        this.wal = new WriteAheadLog(dataDir);
        this.memTable = new MemTable();
        wal.replay(memTable);
        log.info("WAL replayed, memtable has {} entries", memTable.size());
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** Insert or overwrite a key-value pair. */
    public void put(String key, String value) throws IOException {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("key must not be empty");
        if (value == null) throw new IllegalArgumentException("value must not be null — use delete()");

        lock.writeLock().lock();
        try {
            wal.append(key, value);
            memTable.put(key, value);
            maybeFlush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Delete a key (writes a tombstone). */
    public void delete(String key) throws IOException {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("key must not be empty");

        lock.writeLock().lock();
        try {
            wal.appendDelete(key);
            memTable.delete(key);
            maybeFlush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Look up a key.
     *
     * @return the value, or {@link Optional#empty()} if not found / deleted.
     */
    public Optional<String> get(String key) throws IOException {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("key must not be empty");

        lock.readLock().lock();
        try {
            // 1. Check MemTable (most recent writes)
            Optional<String> result = memTable.get(key);
            if (result.isPresent()) {
                return isTombstone(result.get()) ? Optional.empty() : result;
            }

            // 2. Walk SSTables newest-first
            for (SSTable sst : ssTables) {
                Optional<String> sstResult = sst.get(key);
                if (sstResult.isPresent()) {
                    return isTombstone(sstResult.get()) ? Optional.empty() : sstResult;
                }
            }

            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Explicitly trigger compaction (normally called by a background thread or
     * after a batch of writes). Merges SSTables that share the same level tier.
     */
    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            compactor.compact();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Flush MemTable immediately, even if below the threshold. */
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            flushMemTable();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Number of entries currently in the MemTable. */
    public int memTableSize() {
        lock.readLock().lock();
        try {
            return memTable.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return all live key-value pairs whose key starts with {@code prefix},
     * in ascending key order.  An empty prefix returns every live entry.
     *
     * <p>Merges MemTable on top of SSTables (newest wins); tombstones are excluded.
     */
    public SortedMap<String, String> scan(String prefix) throws IOException {
        final String p = (prefix == null) ? "" : prefix;

        lock.readLock().lock();
        try {
            // Accumulate oldest → newest so later puts overwrite earlier ones.
            TreeMap<String, String> merged = new TreeMap<>();

            // Walk SSTables oldest-first
            for (int i = ssTables.size() - 1; i >= 0; i--) {
                for (SSTableEntry entry : ssTables.get(i).readAll()) {
                    if (entry.key().startsWith(p)) {
                        merged.put(entry.key(), entry.value());
                    }
                }
            }

            // MemTable is newest — overwrites everything
            for (Map.Entry<String, String> e : memTable.entries().entrySet()) {
                if (e.getKey().startsWith(p)) {
                    merged.put(e.getKey(), e.getValue());
                }
            }

            // Strip tombstones
            merged.entrySet().removeIf(e -> isTombstone(e.getValue()));
            return Collections.unmodifiableSortedMap(merged);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Number of on-disk SSTables. */
    public int ssTableCount() {
        lock.readLock().lock();
        try {
            return ssTables.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a JSON string describing the store's current state, suitable for
     * serving from an HTTP /debug endpoint.
     */
    public String debugJson() throws IOException {
        lock.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{");

            // ── memtable ──────────────────────────────────────────────────────
            sb.append("\"memtable\":{");
            sb.append("\"size\":").append(memTable.size()).append(",");
            sb.append("\"entries\":[");
            boolean firstEntry = true;
            for (Map.Entry<String, String> e : memTable.entries().entrySet()) {
                if (!firstEntry) sb.append(",");
                firstEntry = false;
                boolean tomb = isTombstone(e.getValue());
                sb.append("{\"key\":").append(jsonStr(e.getKey())).append(",");
                if (tomb) {
                    sb.append("\"tombstone\":true}");
                } else {
                    sb.append("\"tombstone\":false,\"value\":").append(jsonStr(e.getValue())).append("}");
                }
            }
            sb.append("]},");

            // ── sstables ──────────────────────────────────────────────────────
            int[] tierCounts = new int[Compactor.TIER_COUNT];
            sb.append("\"sstables\":[");
            for (int i = 0; i < ssTables.size(); i++) {
                if (i > 0) sb.append(",");
                SSTable sst   = ssTables.get(i);
                long bytes    = Files.size(sst.dataPath());
                int  entries  = sst.countEntries();
                int  tier     = Compactor.tierOf(bytes);
                tierCounts[tier]++;

                sb.append("{");
                sb.append("\"seq\":").append(sst.sequenceNumber()).append(",");
                sb.append("\"sizeBytes\":").append(bytes).append(",");
                sb.append("\"sizeHuman\":").append(jsonStr(humanBytes(bytes))).append(",");
                sb.append("\"entries\":").append(entries).append(",");
                sb.append("\"tier\":").append(tier).append(",");
                sb.append("\"tierLabel\":").append(jsonStr(Compactor.TIER_LABELS[tier])).append(",");
                sb.append("\"firstKey\":").append(jsonStr(sst.firstKey())).append(",");
                sb.append("\"lastKey\":").append(jsonStr(sst.lastKey()));
                sb.append("}");
            }
            sb.append("],");

            // ── compaction ────────────────────────────────────────────────────
            sb.append("\"compaction\":{");
            sb.append("\"threshold\":").append(Compactor.TIER_COMPACTION_THRESHOLD).append(",");
            sb.append("\"tiers\":[");
            for (int t = 0; t < Compactor.TIER_COUNT; t++) {
                if (t > 0) sb.append(",");
                int count = tierCounts[t];
                sb.append("{");
                sb.append("\"tier\":").append(t).append(",");
                sb.append("\"label\":").append(jsonStr(Compactor.TIER_LABELS[t])).append(",");
                sb.append("\"files\":").append(count).append(",");
                sb.append("\"readyToCompact\":").append(count >= Compactor.TIER_COMPACTION_THRESHOLD);
                sb.append("}");
            }
            sb.append("]}");

            sb.append("}");
            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Print a human-readable snapshot of the store's current state to stdout.
     *
     * <p>Output includes:
     * <ul>
     *   <li>MemTable contents (key → value, tombstones flagged)</li>
     *   <li>Each SSTable: sequence number, file size, entry count, compaction tier</li>
     *   <li>Per-tier summary showing how many files exist vs. the compaction threshold</li>
     * </ul>
     */
    public void debug() throws IOException {
        lock.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            String hr = "─".repeat(72);

            sb.append("\n╔══ LSMStore Debug ").append("═".repeat(54)).append("╗\n");
            sb.append(String.format("  Data directory : %s%n", dataDir.toAbsolutePath()));

            // ── MemTable ──────────────────────────────────────────────────────
            sb.append(hr).append('\n');
            sb.append(String.format("[MemTable]  %d entr%s%n",
                    memTable.size(), memTable.size() == 1 ? "y" : "ies"));

            for (Map.Entry<String, String> e : memTable.entries().entrySet()) {
                boolean tomb = isTombstone(e.getValue());
                sb.append(String.format("  %-30s → %s%n",
                        quote(e.getKey()),
                        tomb ? "[TOMBSTONE]" : quote(e.getValue())));
            }
            if (memTable.isEmpty()) sb.append("  (empty)\n");

            // ── SSTables ──────────────────────────────────────────────────────
            sb.append(hr).append('\n');
            sb.append(String.format("[SSTables]  %d file%s (newest → oldest)%n",
                    ssTables.size(), ssTables.size() == 1 ? "" : "s"));

            // Track per-tier counts for the summary below
            int[] tierCounts = new int[Compactor.TIER_COUNT];

            for (int i = 0; i < ssTables.size(); i++) {
                SSTable sst = ssTables.get(i);
                long bytes    = Files.size(sst.dataPath());
                int  entries  = sst.countEntries();
                int  tier     = Compactor.tierOf(bytes);
                tierCounts[tier]++;

                sb.append(String.format(
                        "  #%-2d  seq=%-16d  %8s  %5d entr%s  tier=%d (%s)%n",
                        i,
                        sst.sequenceNumber(),
                        humanBytes(bytes),
                        entries,
                        entries == 1 ? "y " : "ies",
                        tier,
                        Compactor.TIER_LABELS[tier]));

                if (sst.firstKey() != null) {
                    sb.append(String.format("       keys: %s … %s%n",
                            quote(sst.firstKey()), quote(sst.lastKey())));
                }
            }
            if (ssTables.isEmpty()) sb.append("  (none)\n");

            // ── Compaction tier summary ───────────────────────────────────────
            sb.append(hr).append('\n');
            sb.append(String.format("[Compaction]  threshold=%d files per tier%n",
                    Compactor.TIER_COMPACTION_THRESHOLD));

            for (int t = 0; t < Compactor.TIER_COUNT; t++) {
                int count   = tierCounts[t];
                int needed  = Math.max(0, Compactor.TIER_COMPACTION_THRESHOLD - count);
                String status = count >= Compactor.TIER_COMPACTION_THRESHOLD
                        ? "*** READY TO COMPACT ***"
                        : (needed == 1 ? "needs 1 more file" : "needs " + needed + " more files");
                sb.append(String.format("  Tier %d (%s):%3d file%s  — %s%n",
                        t,
                        Compactor.TIER_LABELS[t],
                        count,
                        count == 1 ? " " : "s",
                        status));
            }

            sb.append("╚").append("═".repeat(71)).append("╝\n");
            System.out.print(sb);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (closed) return;
            closed = true;
            if (memTable.size() > 0) {
                flushMemTable();
            }
            wal.close();
            for (SSTable sst : ssTables) {
                sst.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void maybeFlush() throws IOException {
        if (memTable.size() >= MEMTABLE_FLUSH_THRESHOLD) {
            flushMemTable();
        }
    }

    /**
     * Freeze the current MemTable, write it to an SSTable, then swap in a fresh one.
     * The WAL is truncated once the SSTable is durably written.
     */
    private void flushMemTable() throws IOException {
        if (memTable.size() == 0) return;

        log.info("Flushing MemTable ({} entries) to SSTable", memTable.size());
        MemTable frozen = memTable;
        this.memTable = new MemTable();

        SSTable sst = SSTable.flush(dataDir, frozen);
        // Prepend so index 0 is always newest
        ssTables.add(0, sst);

        wal.truncate();
        log.info("Flush complete — {} SSTables on disk", ssTables.size());
    }

    private static boolean isTombstone(String value) {
        return TOMBSTONE.equals(value);
    }

    /** Produce a JSON string literal (with surrounding quotes and proper escaping). */
    public static String jsonStr(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\")
                       .replace("\"", "\\\"")
                       .replace("\n", "\\n")
                       .replace("\r", "\\r")
                       .replace("\t", "\\t") + "\"";
    }

    private static String quote(String s) {
        if (s == null) return "null";
        String display = s.length() > 40 ? s.substring(0, 37) + "…" : s;
        return "\"" + display + "\"";
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024)               return bytes + " B";
        if (bytes < 1024 * 1024)        return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
