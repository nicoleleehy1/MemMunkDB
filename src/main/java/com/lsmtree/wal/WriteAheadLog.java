package com.lsmtree.wal;

import com.lsmtree.memtable.MemTable;
import com.lsmtree.store.LSMStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Append-only Write-Ahead Log (WAL).
 *
 * <p>Every mutation is durably written here <em>before</em> being applied to the
 * MemTable.  After a successful SSTable flush the log is truncated so it stays
 * small.  On startup, any entries remaining in the WAL are replayed into a fresh
 * MemTable to restore state that was not yet flushed.
 *
 * <h3>Format</h3>
 * One record per line:
 * <pre>
 *   PUT \t &lt;key&gt; \t &lt;value&gt;
 *   DEL \t &lt;key&gt;
 * </pre>
 * Keys and values must not contain {@code \t} or {@code \n}.
 */
public class WriteAheadLog implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);

    private static final String WAL_FILENAME = "wal.log";
    private static final String OP_PUT = "PUT";
    private static final String OP_DEL = "DEL";

    private final Path walPath;
    private BufferedWriter writer;
    private boolean closed = false;

    public WriteAheadLog(Path dataDir) throws IOException {
        this.walPath = dataDir.resolve(WAL_FILENAME);
        this.writer = openWriter();
    }

    /** Append a PUT record. */
    public void append(String key, String value) throws IOException {
        writer.write(OP_PUT + "\t" + key + "\t" + value);
        writer.newLine();
        writer.flush();
    }

    /** Append a DELETE record. */
    public void appendDelete(String key) throws IOException {
        writer.write(OP_DEL + "\t" + key);
        writer.newLine();
        writer.flush();
    }

    /**
     * Replay the WAL into {@code target}.  Called once during startup after an
     * unexpected shutdown left un-flushed entries in the log.
     */
    public void replay(MemTable target) throws IOException {
        if (!Files.exists(walPath)) return;

        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(walPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] parts = line.split("\t", 3);
                if (parts.length < 2) {
                    log.warn("Skipping malformed WAL record: {}", line);
                    continue;
                }
                String op = parts[0];
                String key = parts[1];
                if (OP_PUT.equals(op) && parts.length == 3) {
                    target.put(key, parts[2]);
                    count++;
                } else if (OP_DEL.equals(op)) {
                    target.delete(key);
                    count++;
                } else {
                    log.warn("Unknown WAL op '{}', skipping", op);
                }
            }
        }
        log.info("Replayed {} WAL records", count);
    }

    /**
     * Truncate the WAL after a successful SSTable flush.  The current writer is
     * replaced with a fresh empty file.
     */
    public void truncate() throws IOException {
        writer.close();
        Files.deleteIfExists(walPath);
        this.writer = openWriter();
        log.debug("WAL truncated");
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        writer.flush();
        writer.close();
    }

    private BufferedWriter openWriter() throws IOException {
        return Files.newBufferedWriter(
                walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }
}
