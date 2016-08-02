package com.hazelcast.map.impl.eviction;

import com.hazelcast.util.MemoryInfoAccessor;

/**
 * Returns zero for every method call.
 * Used for testing purposes.
 */
public class ZeroMemoryInfoAccessor implements MemoryInfoAccessor {

    @Override
    public long getTotalMemory() {
        return 0;
    }

    @Override
    public long getFreeMemory() {
        return 0;
    }

    @Override
    public long getMaxMemory() {
        return 0;
    }
}