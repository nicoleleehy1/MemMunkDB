package com.lsmtree.sstable;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.lsmtree.memtable.MemTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Immutable, on-disk sorted-string table.
 *
 * <h3>File layout</h3>
 * <pre>
 *   &lt;sequence&gt;.sst          — data file (key-value records)
 *   &lt;sequence&gt;.idx          — sparse index (key → byte offset)
 *   &lt;sequence&gt;.bloom        — serialised Bloom filter
 * </pre>
 *
 * <h3>Data record format</h3>
 * <pre>
 *   &lt;key-len:4 bytes big-endian&gt; &lt;key-bytes&gt;
 *   &lt;val-len:4 bytes big-endian&gt; &lt;val-bytes&gt;
 * </pre>
 * Keys are written in ascending order.
 *
 * <h3>Index file format</h3>
 * <pre>
 *   &lt;count:4 bytes&gt; ( &lt;offset:8 bytes&gt; &lt;key-len:4 bytes&gt; &lt;key-bytes&gt; )*
 * </pre>
 */
public class SSTable implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SSTable.class);

    private static final String EXT_DATA  = ".sst";
    private static final String EXT_INDEX = ".idx";
    private static final String EXT_BLOOM = ".bloom";

    /** Expected insertions for the Bloom filter (trades memory vs false-positive rate). */
    private static final int BLOOM_EXPECTED = 100_000;
    private static final double BLOOM_FPP    = 0.01;

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    private final Path dataPath;
    private final long sequenceNumber;
    private final SSTableIndex index;
    private final BloomFilter<String> bloomFilter;

    /** First and last key in the file — used by compaction to check key-range overlap. */
    private final String firstKey;
    private final String lastKey;

    private SSTable(Path dataPath, long sequenceNumber,
                    SSTableIndex index, BloomFilter<String> bloomFilter,
                    String firstKey, String lastKey) {
        this.dataPath = dataPath;
        this.sequenceNumber = sequenceNumber;
        this.index = index;
        this.bloomFilter = bloomFilter;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
    }

    // -------------------------------------------------------------------------
    // Static factory — flush a MemTable to disk
    // -------------------------------------------------------------------------

    /**
     * Write {@code memTable} to a new SSTable in {@code dir}.
     * Returns the opened SSTable ready for reads.
     */
    public static SSTable flush(Path dir, MemTable memTable) throws IOException {
        long seq = System.currentTimeMillis();
        Path dataPath  = dir.resolve(seq + EXT_DATA);
        Path indexPath = dir.resolve(seq + EXT_INDEX);
        Path bloomPath = dir.resolve(seq + EXT_BLOOM);

        SSTableIndex index = new SSTableIndex();
        BloomFilter<String> bloom = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8), BLOOM_EXPECTED, BLOOM_FPP);

        String firstKey = null, lastKey = null;
        int recordCount = 0;

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(dataPath)))) {

            Iterator<Map.Entry<String, String>> it = memTable.iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                String key   = entry.getKey();
                String value = entry.getValue();

                long offset = dos.size();   // byte position of this record
                if (recordCount == 0) firstKey = key;
                lastKey = key;

                // Sample every INDEX_INTERVAL-th key into the sparse index
                if (recordCount % SSTableIndex.INDEX_INTERVAL == 0) {
                    index.add(key, offset);
                }

                bloom.put(key);
                writeString(dos, key);
                writeString(dos, value);
                recordCount++;
            }
        }

        writeIndex(indexPath, index);
        writeBloom(bloomPath, bloom);

        log.info("Flushed SSTable seq={} records={} path={}", seq, recordCount, dataPath);
        return new SSTable(dataPath, seq, index, bloom, firstKey, lastKey);
    }

    // -------------------------------------------------------------------------
    // Static factory — load existing SSTables from disk
    // -------------------------------------------------------------------------

    /** Scan {@code dir} for all {@code *.sst} files and open them, newest first. */
    public static List<SSTable> loadAll(Path dir) throws IOException {
        List<SSTable> result = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + EXT_DATA)) {
            List<Path> paths = new ArrayList<>();
            stream.forEach(paths::add);
            // Sort descending by sequence number (filename prefix)
            paths.sort((a, b) -> Long.compare(seqOf(b), seqOf(a)));
            for (Path p : paths) {
                result.add(load(p));
            }
        }
        return result;
    }

    private static SSTable load(Path dataPath) throws IOException {
        long seq = seqOf(dataPath);
        Path base  = dataPath.getParent().resolve(String.valueOf(seq));
        Path indexPath = Path.of(base + EXT_INDEX);
        Path bloomPath = Path.of(base + EXT_BLOOM);

        SSTableIndex index = readIndex(indexPath);
        BloomFilter<String> bloom = readBloom(bloomPath);

        // Determine first/last key by scanning (cheap — just reads first and last record)
        String[] bounds = scanBounds(dataPath);

        log.info("Loaded SSTable seq={}", seq);
        return new SSTable(dataPath, seq, index, bloom, bounds[0], bounds[1]);
    }

    // -------------------------------------------------------------------------
    // Point lookup
    // -------------------------------------------------------------------------

    /**
     * Look up {@code key}.
     *
     * @return the raw value (may be a tombstone) or {@link Optional#empty()}.
     */
    public Optional<String> get(String key) throws IOException {
        // Bloom filter fast-path: key definitely absent
        if (!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        // Use the sparse index to find the nearest position ≤ key
        long startOffset = index.floorOffset(key);

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(dataPath)))) {

            long skipped = dis.skip(startOffset);
            if (skipped < startOffset) return Optional.empty();

            while (dis.available() > 0) {
                String k = readString(dis);
                String v = readString(dis);

                int cmp = k.compareTo(key);
                if (cmp == 0) return Optional.of(v);
                if (cmp > 0)  break;   // passed the target — not found
            }
        }
        return Optional.empty();
    }

    // -------------------------------------------------------------------------
    // Full scan (used during compaction and debug)
    // -------------------------------------------------------------------------

    /**
     * Count entries by scanning the data file without materialising values.
     * Reads only key lengths (skipping value bytes) for efficiency.
     */
    public int countEntries() throws IOException {
        int count = 0;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(dataPath)))) {
            while (dis.available() > 0) {
                int keyLen = dis.readInt();
                dis.skipNBytes(keyLen);
                int valLen = dis.readInt();
                dis.skipNBytes(valLen);
                count++;
            }
        }
        return count;
    }

    /** Read all entries in sorted order. */
    public List<SSTableEntry> readAll() throws IOException {
        List<SSTableEntry> entries = new ArrayList<>();
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(dataPath)))) {
            while (dis.available() > 0) {
                String k = readString(dis);
                String v = readString(dis);
                entries.add(new SSTableEntry(k, v));
            }
        }
        return entries;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public long sequenceNumber() { return sequenceNumber; }
    public String firstKey()      { return firstKey; }
    public String lastKey()       { return lastKey; }
    public Path dataPath()        { return dataPath; }

    /** Delete the three files backing this SSTable. */
    public void delete() throws IOException {
        Path base = dataPath.getParent().resolve(String.valueOf(sequenceNumber));
        Files.deleteIfExists(dataPath);
        Files.deleteIfExists(Path.of(base + EXT_INDEX));
        Files.deleteIfExists(Path.of(base + EXT_BLOOM));
    }

    @Override
    public void close() {
        // No file handles kept open between requests.
    }

    // -------------------------------------------------------------------------
    // Binary I/O helpers
    // -------------------------------------------------------------------------

    private static void writeString(DataOutputStream dos, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static String readString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        byte[] bytes = dis.readNBytes(len);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // Index I/O
    // -------------------------------------------------------------------------

    private static void writeIndex(Path path, SSTableIndex index) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(path)))) {
            dos.writeInt(index.size());
            for (int i = 0; i < index.size(); i++) {
                dos.writeLong(index.offsets().get(i));
                writeString(dos, index.keys().get(i));
            }
        }
    }

    private static SSTableIndex readIndex(Path path) throws IOException {
        SSTableIndex index = new SSTableIndex();
        if (!Files.exists(path)) return index;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                long offset = dis.readLong();
                String key  = readString(dis);
                index.add(key, offset);
            }
        }
        return index;
    }

    // -------------------------------------------------------------------------
    // Bloom filter I/O
    // -------------------------------------------------------------------------

    private static void writeBloom(Path path, BloomFilter<String> bloom) throws IOException {
        try (OutputStream os = Files.newOutputStream(path)) {
            bloom.writeTo(os);
        }
    }

    private static BloomFilter<String> readBloom(Path path) throws IOException {
        if (!Files.exists(path)) {
            // Return a pass-through filter that always returns mightContain=true
            return BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8), BLOOM_EXPECTED, BLOOM_FPP);
        }
        try (InputStream is = Files.newInputStream(path)) {
            return BloomFilter.readFrom(is, Funnels.stringFunnel(StandardCharsets.UTF_8));
        }
    }

    // -------------------------------------------------------------------------
    // Miscellaneous
    // -------------------------------------------------------------------------

    private static long seqOf(Path p) {
        String name = p.getFileName().toString();
        int dot = name.lastIndexOf('.');
        try {
            return Long.parseLong(dot >= 0 ? name.substring(0, dot) : name);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String[] scanBounds(Path dataPath) throws IOException {
        String first = null, last = null;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(dataPath)))) {
            while (dis.available() > 0) {
                String k = readString(dis);
                readString(dis); // skip value
                if (first == null) first = k;
                last = k;
            }
        }
        return new String[]{first, last};
    }
}
