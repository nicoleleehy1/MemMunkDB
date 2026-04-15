package com.lsmtree.sstable;

/**
 * A single key-value record inside an SSTable.
 */
public record SSTableEntry(String key, String value) implements Comparable<SSTableEntry> {

    @Override
    public int compareTo(SSTableEntry other) {
        return this.key.compareTo(other.key);
    }
}
