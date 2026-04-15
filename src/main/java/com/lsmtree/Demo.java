package com.lsmtree;

import com.lsmtree.store.LSMStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end demo of the LSM-Tree store.
 *
 * Phases:
 *   1. Insert 500 key-value pairs, flushing every 100 inserts
 *   2. Delete 50 randomly chosen keys
 *   3. Trigger compaction
 *   4. Print debug snapshot
 */
public class Demo {

    private static final int TOTAL_WRITES   = 500;
    private static final int FLUSH_INTERVAL = 100;
    private static final int DELETE_COUNT   =  50;

    public static void main(String[] args) throws IOException {
        Path dataDir = Files.createTempDirectory("lsm-demo");
        System.out.printf("Store directory: %s%n%n", dataDir.toAbsolutePath());

        try (LSMStore store = new LSMStore(dataDir)) {

            // ── Phase 1: Inserts ──────────────────────────────────────────────
            separator("Phase 1 — Insert %d key-value pairs (flush every %d)",
                    TOTAL_WRITES, FLUSH_INTERVAL);

            List<String> allKeys = new ArrayList<>(TOTAL_WRITES);

            for (int i = 1; i <= TOTAL_WRITES; i++) {
                String key   = String.format("key:%05d", i);
                String value = String.format("value-%d", i);
                store.put(key, value);
                allKeys.add(key);

                if (i % FLUSH_INTERVAL == 0) {
                    System.out.printf("  [%3d] Flushing — memtable had %d entries%n",
                            i, store.memTableSize());
                    store.flush();
                    System.out.printf("        → %d SSTable(s) on disk%n", store.ssTableCount());
                }
            }
            System.out.printf("%nInserts complete. MemTable: %d entries, SSTables: %d%n",
                    store.memTableSize(), store.ssTableCount());

            // ── Phase 2: Deletes ──────────────────────────────────────────────
            separator("Phase 2 — Delete %d random keys", DELETE_COUNT);

            Collections.shuffle(allKeys);
            List<String> toDelete = allKeys.subList(0, DELETE_COUNT);
            Collections.sort(toDelete);   // print in deterministic order

            for (String key : toDelete) {
                store.delete(key);
                System.out.printf("  deleted %s%n", key);
            }
            System.out.printf("%nDeletes written as tombstones in MemTable (%d entries).%n",
                    store.memTableSize());

            // ── Phase 3: Compaction ───────────────────────────────────────────
            separator("Phase 3 — Compaction");

            System.out.printf("Before: %d SSTable(s)%n", store.ssTableCount());
            store.compact();
            System.out.printf("After : %d SSTable(s)%n", store.ssTableCount());

            // ── Phase 4: Debug snapshot ───────────────────────────────────────
            separator("Phase 4 — Debug snapshot");

            store.debug();
        }
    }

    private static void separator(String label, Object... args) {
        String text = String.format(label, args);
        String bar  = "═".repeat(70);
        System.out.printf("%n╔%s╗%n║  %-68s║%n╚%s╝%n%n", bar, text, bar);
    }
}
