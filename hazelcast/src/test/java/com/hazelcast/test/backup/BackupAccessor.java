package com.hazelcast.test.backup;

/**
 * Access backup records in a given replica.
 *
 * All accessors use a partition thread to access values data hence
 * they are safe to use with HD-backed data structures.
 *
 * Use {@link TestBackupUtils} to create an accessor instance for
 * your data structure.
 *
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface BackupAccessor<K ,V> {
    /**
     * Number of existing backup entries in a given structure and replica index
     *
     * @return number of backup entries
     */
    int size();

    /**
     * Reads backup value
     *
     * @param key key of the backup entry to get
     * @return backup value or null
     */
    V get(K key);
}
