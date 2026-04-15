package com.lsmtree.memtable;

import com.lsmtree.store.LSMStore;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * In-memory sorted buffer for recent writes.
 *
 * <p>Backed by a {@link TreeMap} so iterating produces entries in key order —
 * a requirement for writing sorted SSTables.  A deleted key is stored as a
 * tombstone value ({@link LSMStore#TOMBSTONE}) so that reads in the SSTable
 * layer know to stop searching older files.
 */
public class MemTable {

    private final TreeMap<String, String> data = new TreeMap<>();

    public void put(String key, String value) {
        data.put(key, value);
    }

    public void delete(String key) {
        data.put(key, LSMStore.TOMBSTONE);
    }

    /**
     * Returns the value (possibly a tombstone) stored for {@code key}, or
     * {@link Optional#empty()} if the key has never been written to this table.
     */
    public Optional<String> get(String key) {
        String v = data.get(key);
        return Optional.ofNullable(v);
    }

    /** Number of entries (including tombstones). */
    public int size() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    /** Iterate entries in sorted key order (used during SSTable flush). */
    public Iterator<Map.Entry<String, String>> iterator() {
        return data.entrySet().iterator();
    }

    /** Unmodifiable sorted view of all entries (used by debug output). */
    public SortedMap<String, String> entries() {
        return Collections.unmodifiableSortedMap(data);
    }
}
