package com.lsmtree.sstable;

import java.util.ArrayList;
import java.util.List;

/**
 * Sparse index that maps every {@value #INDEX_INTERVAL}-th key to its byte
 * offset in the SSTable data file.
 *
 * <p>A point lookup uses {@link #floorOffset(String)} to find the largest
 * indexed key ≤ the target, then scans forward from that position.
 */
public class SSTableIndex {

    /** Sample every N-th entry into the sparse index. */
    public static final int INDEX_INTERVAL = 16;

    private final List<String> keys = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>();

    /** Add an index entry (called during flush, every {@value #INDEX_INTERVAL} records). */
    public void add(String key, long offset) {
        keys.add(key);
        offsets.add(offset);
    }

    public int size() {
        return keys.size();
    }

    /**
     * Return the file offset to begin scanning from for {@code targetKey}, or
     * {@code 0} if the index is empty or the key is before all indexed entries.
     */
    public long floorOffset(String targetKey) {
        if (keys.isEmpty()) return 0;

        int lo = 0, hi = keys.size() - 1, result = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = keys.get(mid).compareTo(targetKey);
            if (cmp <= 0) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return offsets.get(result);
    }

    public List<String> keys() { return keys; }
    public List<Long> offsets() { return offsets; }
}
