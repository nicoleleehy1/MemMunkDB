package com.lsmtree;

import com.lsmtree.store.LSMStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LSMStoreTest {

    @TempDir
    Path tempDir;

    private LSMStore store;

    @BeforeEach
    void setup() throws IOException {
        store = new LSMStore(tempDir);
    }

    @AfterEach
    void teardown() throws IOException {
        store.close();
    }

    // -------------------------------------------------------------------------
    // Basic put / get
    // -------------------------------------------------------------------------

    @Test
    @Order(1)
    void putAndGet_basicEntry() throws IOException {
        store.put("hello", "world");
        assertEquals(Optional.of("world"), store.get("hello"));
    }

    @Test
    @Order(2)
    void get_missingKey_returnsEmpty() throws IOException {
        assertEquals(Optional.empty(), store.get("nonexistent"));
    }

    @Test
    @Order(3)
    void put_overwriteExistingKey() throws IOException {
        store.put("key", "v1");
        store.put("key", "v2");
        assertEquals(Optional.of("v2"), store.get("key"));
    }

    // -------------------------------------------------------------------------
    // Delete / tombstone
    // -------------------------------------------------------------------------

    @Test
    @Order(4)
    void delete_removesKey() throws IOException {
        store.put("toDelete", "value");
        store.delete("toDelete");
        assertEquals(Optional.empty(), store.get("toDelete"));
    }

    @Test
    @Order(5)
    void delete_nonExistentKey_doesNotThrow() {
        assertDoesNotThrow(() -> store.delete("ghost"));
    }

    // -------------------------------------------------------------------------
    // SSTable flush
    // -------------------------------------------------------------------------

    @Test
    @Order(6)
    void flush_createsSSTable() throws IOException {
        store.put("a", "1");
        store.put("b", "2");
        store.flush();

        assertEquals(1, store.ssTableCount());
        assertEquals(0, store.memTableSize());
    }

    @Test
    @Order(7)
    void get_afterFlush_readsFromSSTable() throws IOException {
        store.put("persist", "yes");
        store.flush();

        assertEquals(Optional.of("yes"), store.get("persist"));
    }

    @Test
    @Order(8)
    void delete_tombstoneInSSTable_hiddenOnRead() throws IOException {
        store.put("x", "exists");
        store.flush();
        store.delete("x");
        store.flush();

        assertEquals(Optional.empty(), store.get("x"));
    }

    // -------------------------------------------------------------------------
    // Multiple SSTables — newest-wins ordering
    // -------------------------------------------------------------------------

    @Test
    @Order(9)
    void multipleFlushes_newestValueWins() throws IOException {
        store.put("k", "old");
        store.flush();
        store.put("k", "new");
        store.flush();

        assertEquals(Optional.of("new"), store.get("k"));
    }

    @Test
    @Order(10)
    void multipleFlushes_independentKeys() throws IOException {
        for (int i = 0; i < 5; i++) {
            store.put("key" + i, "val" + i);
            store.flush();
        }
        for (int i = 0; i < 5; i++) {
            assertEquals(Optional.of("val" + i), store.get("key" + i));
        }
    }

    // -------------------------------------------------------------------------
    // WAL recovery
    // -------------------------------------------------------------------------

    @Test
    @Order(11)
    void walRecovery_restoresUnflushedWrites() throws IOException {
        store.put("survives", "crash");
        // Do NOT flush — simulate crash by closing without flushing
        store.close();

        // Reopen store — WAL should replay
        LSMStore recovered = new LSMStore(tempDir);
        try {
            assertEquals(Optional.of("crash"), recovered.get("survives"));
        } finally {
            recovered.close();
        }
    }

    // -------------------------------------------------------------------------
    // Compaction
    // -------------------------------------------------------------------------

    @Test
    @Order(12)
    void compact_mergesSSTables_dataIntact() throws IOException {
        // Write enough small SSTables to trigger size-tier compaction
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 10; j++) {
                store.put("key_" + i + "_" + j, "value_" + i + "_" + j);
            }
            store.flush();
        }
        int beforeCompact = store.ssTableCount();
        store.compact();

        // After compaction the number of SSTables should be ≤ before
        assertTrue(store.ssTableCount() <= beforeCompact,
                "Expected fewer SSTables after compaction");

        // Data must still be readable
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 10; j++) {
                assertEquals(Optional.of("value_" + i + "_" + j),
                        store.get("key_" + i + "_" + j));
            }
        }
    }

    @Test
    @Order(13)
    void compact_tombstonesDropped_keyUnreadable() throws IOException {
        store.put("gone", "here");
        store.flush();
        store.delete("gone");
        store.flush();
        store.flush(); // extra flush to get ≥ 4 SSTables
        store.flush();

        store.compact();

        assertEquals(Optional.empty(), store.get("gone"));
    }

    // -------------------------------------------------------------------------
    // Large write batch (auto-flush via threshold)
    // -------------------------------------------------------------------------

    @Test
    @Order(14)
    void largeWriteBatch_autoFlushes() throws IOException {
        int writes = 1_500; // exceeds MEMTABLE_FLUSH_THRESHOLD=1000
        for (int i = 0; i < writes; i++) {
            store.put("bulk_" + i, "v" + i);
        }
        // At least one auto-flush should have happened
        assertTrue(store.ssTableCount() >= 1, "Expected at least one auto-flush SSTable");

        // Spot-check a few values
        assertEquals(Optional.of("v0"),         store.get("bulk_0"));
        assertEquals(Optional.of("v999"),       store.get("bulk_999"));
        assertEquals(Optional.of("v1499"),      store.get("bulk_1499"));
    }

    // -------------------------------------------------------------------------
    // debug()
    // -------------------------------------------------------------------------

    @Test
    @Order(19)
    void debug_doesNotThrow_emptyStore() throws IOException {
        // Should print without error on a brand-new empty store
        assertDoesNotThrow(() -> store.debug());
    }

    @Test
    @Order(20)
    void debug_doesNotThrow_withData() throws IOException {
        store.put("alpha", "1");
        store.put("beta", "2");
        store.delete("gamma");
        store.flush();
        store.put("delta", "4");
        assertDoesNotThrow(() -> store.debug());
    }

    // -------------------------------------------------------------------------
    // Input validation
    // -------------------------------------------------------------------------

    @Test
    @Order(15)
    void put_nullKey_throws() {
        assertThrows(IllegalArgumentException.class, () -> store.put(null, "v"));
    }

    @Test
    @Order(16)
    void put_emptyKey_throws() {
        assertThrows(IllegalArgumentException.class, () -> store.put("", "v"));
    }

    @Test
    @Order(17)
    void put_nullValue_throws() {
        assertThrows(IllegalArgumentException.class, () -> store.put("k", null));
    }

    @Test
    @Order(18)
    void get_nullKey_throws() {
        assertThrows(IllegalArgumentException.class, () -> store.get(null));
    }
}
