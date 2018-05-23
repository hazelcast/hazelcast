package com.hazelcast.query.impl;

import com.hazelcast.monitor.impl.InternalIndexStats;

/**
 * Provides the private index API.
 */
public interface InternalIndex extends Index {

    /**
     * Returns the index stats associated with this index.
     */
    InternalIndexStats getIndexStats();

}
