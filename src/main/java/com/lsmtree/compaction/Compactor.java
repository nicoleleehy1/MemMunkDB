package com.lsmtree.compaction;

import com.lsmtree.sstable.SSTable;
import com.lsmtree.sstable.SSTableEntry;
import com.lsmtree.store.LSMStore;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Size-tiered compaction strategy.
 *
 * <p>SSTables are grouped into tiers by approximate size.  When a tier has
 * {@value #TIER_COMPACTION_THRESHOLD} or more files, those files are merged
 * into a single new SSTable (with tombstones dropped) and the originals deleted.
 *
 * <p>This is a simplified version of Cassandra-style size-tiered compaction:
 * <ul>
 *   <li>Tier 0: files ≤ 256 KB</li>
 *   <li>Tier 1: files ≤ 4 MB</li>
 *   <li>Tier 2: files ≤ 64 MB</li>
 *   <li>Tier 3: everything larger</li>
 * </ul>
 */
public class Compactor {

    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    public static final int TIER_COMPACTION_THRESHOLD = 4;
    public static final int TIER_COUNT = 4;

    public static final long[] TIER_SIZES = {
        256L * 1024,           // Tier 0: ≤ 256 KB
        4L * 1024 * 1024,      // Tier 1: ≤ 4 MB
        64L * 1024 * 1024,     // Tier 2: ≤ 64 MB
        Long.MAX_VALUE         // Tier 3: > 64 MB
    };

    public static final String[] TIER_LABELS = {
        "≤256 KB",
        "≤4 MB",
        "≤64 MB",
        ">64 MB"
    };

    /**
     * Return the tier number (0–3) for a data file of {@code fileSizeBytes}.
     */
    public static int tierOf(long fileSizeBytes) {
        for (int i = 0; i < TIER_SIZES.length; i++) {
            if (fileSizeBytes <= TIER_SIZES[i]) return i;
        }
        return TIER_SIZES.length - 1;
    }

    private final Path dataDir;
    private final List<SSTable> ssTables;   // shared reference from LSMStore
    @SuppressWarnings("unused")
    private final ReadWriteLock lock;       // held by caller when compact() is invoked

    public Compactor(Path dataDir, List<SSTable> ssTables, ReadWriteLock lock) {
        this.dataDir  = dataDir;
        this.ssTables = ssTables;
        this.lock     = lock;
    }

    /**
     * Check each size tier and merge any tier that has reached the threshold.
     * Caller must hold the write lock.
     */
    public void compact() throws IOException {
        for (int tier = 0; tier < TIER_SIZES.length; tier++) {
            List<SSTable> candidates = collectTier(tier);
            if (candidates.size() >= TIER_COMPACTION_THRESHOLD) {
                log.info("Compacting tier {} ({} SSTables)", tier, candidates.size());
                mergeTier(candidates);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private List<SSTable> collectTier(int tier) throws IOException {
        long maxBytes = TIER_SIZES[tier];
        long minBytes = tier == 0 ? 0 : TIER_SIZES[tier - 1];

        List<SSTable> result = new ArrayList<>();
        for (SSTable sst : ssTables) {
            long size = Files.size(sst.dataPath());
            if (size > minBytes && size <= maxBytes) {
                result.add(sst);
            }
        }
        return result;
    }

    /**
     * Merge {@code inputs} into a single new SSTable; tombstones are dropped.
     * The originals are removed from the shared list and deleted from disk.
     */
    private void mergeTier(List<SSTable> inputs) throws IOException {
        // k-way merge: collect all entries, deduplicate (newest wins), sort
        // "Newest" is defined by the highest sequence number (most recent flush).
        inputs.sort(Comparator.comparingLong(SSTable::sequenceNumber).reversed());

        // Use a LinkedHashMap in iteration order to keep first-seen (newest) value per key
        Map<String, String> merged = new LinkedHashMap<>();
        for (SSTable sst : inputs) {
            for (SSTableEntry entry : sst.readAll()) {
                merged.putIfAbsent(entry.key(), entry.value());
            }
        }

        // Sort by key
        List<Map.Entry<String, String>> sorted = new ArrayList<>(merged.entrySet());
        sorted.sort(Map.Entry.comparingByKey());

        // Drop tombstones — they are no longer needed after compaction
        // (older files that might hold the same key have been merged in)
        sorted.removeIf(e -> LSMStore.TOMBSTONE.equals(e.getValue()));

        if (sorted.isEmpty()) {
            log.info("Compaction produced empty output — deleting {} input files", inputs.size());
            removeAndDelete(inputs);
            return;
        }

        // Write new SSTable
        long seq = System.currentTimeMillis();
        // Ensure uniqueness if millis collide with existing files
        while (Files.exists(dataDir.resolve(seq + ".sst"))) seq++;

        Path newDataPath = dataDir.resolve(seq + ".sst");
        SSTable newSst = writeCompacted(newDataPath, seq, sorted);

        // Swap: remove inputs, prepend new SSTable
        ssTables.removeAll(inputs);
        // Insert the new SSTable in sequence-number order (newest first)
        ssTables.add(0, newSst);

        // Delete old files
        for (SSTable old : inputs) {
            old.delete();
        }

        log.info("Compaction complete: {} → 1 SSTable (seq={})", inputs.size(), seq);
    }

    private SSTable writeCompacted(Path dataPath, long seq,
                                   List<Map.Entry<String, String>> sorted) throws IOException {
        // Delegate to SSTable.flush via an adapter MemTable would be wasteful —
        // instead we write the data file directly and build index + bloom inline.
        // We reuse the same binary format as SSTable so it can be read back normally.

        com.lsmtree.sstable.SSTableIndex index = new com.lsmtree.sstable.SSTableIndex();
        BloomFilter<String> bloom = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8), 100_000, 0.01);

        String firstKey = null, lastKey = null;
        int recordCount = 0;

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(dataPath)))) {
            for (Map.Entry<String, String> entry : sorted) {
                String key   = entry.getKey();
                String value = entry.getValue();

                long offset = dos.size();
                if (recordCount == 0) firstKey = key;
                lastKey = key;

                if (recordCount % com.lsmtree.sstable.SSTableIndex.INDEX_INTERVAL == 0) {
                    index.add(key, offset);
                }

                bloom.put(key);
                writeString(dos, key);
                writeString(dos, value);
                recordCount++;
            }
        }

        // Write index and bloom filter via the package-private helpers we replicate here
        Path base = dataPath.getParent().resolve(String.valueOf(seq));
        writeIndex(Path.of(base + ".idx"), index);
        writeBloom(Path.of(base + ".bloom"), bloom);

        // Now open it for reads by loading directly
        return loadCompacted(dataPath, seq, index, bloom, firstKey, lastKey);
    }

    // Reflection-free construction: use SSTable.loadAll after writing
    private SSTable loadCompacted(Path dataPath, long seq,
                                  com.lsmtree.sstable.SSTableIndex index,
                                  BloomFilter<String> bloom,
                                  String firstKey, String lastKey) throws IOException {
        // Reload from disk so everything is properly initialised
        return SSTable.loadAll(dataDir).stream()
                .filter(s -> s.sequenceNumber() == seq)
                .findFirst()
                .orElseThrow(() -> new IOException("Compacted SSTable not found after write: " + seq));
    }

    private void removeAndDelete(List<SSTable> inputs) throws IOException {
        ssTables.removeAll(inputs);
        for (SSTable sst : inputs) {
            sst.delete();
        }
    }

    // -------------------------------------------------------------------------
    // Binary helpers (duplicated from SSTable to keep compactor self-contained)
    // -------------------------------------------------------------------------

    private static void writeString(DataOutputStream dos, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static void writeIndex(Path path, com.lsmtree.sstable.SSTableIndex index)
            throws IOException {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(path)))) {
            dos.writeInt(index.size());
            for (int i = 0; i < index.size(); i++) {
                dos.writeLong(index.offsets().get(i));
                writeString(dos, index.keys().get(i));
            }
        }
    }

    private static void writeBloom(Path path, BloomFilter<String> bloom) throws IOException {
        try (OutputStream os = Files.newOutputStream(path)) {
            bloom.writeTo(os);
        }
    }
}
