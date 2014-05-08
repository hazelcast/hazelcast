package com.hazelcast.map.record;

/**
 * Flags.
 */
public final class RecordFlags {

    /**
     * Remove candidate marker.
     */
    public static final byte MARK_AS_REMOVED = 0x01;

    /**
     * Evict candidate marker.
     */
    public static final byte MARK_AS_EVICTED = 0x02;

    private RecordFlags() {
    }
}
