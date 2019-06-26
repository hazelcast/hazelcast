package com.hazelcast.internal.query.row;

/**
 * Interface to get a value from key-value row.
 */
public interface KeyValueRowExtractor {
    /**
     * Extract value from the key or value.
     */
    Object extract(Object key, Object val, String path);
}
