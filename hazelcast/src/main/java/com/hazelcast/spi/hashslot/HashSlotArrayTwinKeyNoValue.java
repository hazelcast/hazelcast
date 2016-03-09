package com.hazelcast.spi.hashslot;

import com.hazelcast.memory.MemoryManager;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;

/**
 * Twin-key hash slot array with zero-width value. Suitable for a twin-long set implementation.
 * Stores the null-sentinel value into the {@code key1} field, therefore the chosen null-sentinel value
 * is not valid as the value of {@code key1}.
 */
public class HashSlotArrayTwinKeyNoValue extends HashSlotArrayTwinKeyImpl {

    /**
     * @param nullKey1 the null-sentinel value checked against the {@code key1} field.
     */
    public HashSlotArrayTwinKeyNoValue(long nullKey1, MemoryManager mm, int initialCapacity) {
        super(nullKey1, 0L, mm, 0, initialCapacity);
    }

    /**
     * @param nullKey1 the null-sentinel value checked against the {@code key1} field.
     */
    public HashSlotArrayTwinKeyNoValue(long nullKey1, MemoryManager mm) {
        this(nullKey1, mm, DEFAULT_CAPACITY);
    }

    // Value length is always zero (not under user's control)
    @Override protected boolean valueLengthValid(int valueLength) {
        return true;
    }

    @Override public long ensure(long key1, long key2) {
        assert key1 != unassignedSentinel : "ensure() called with key1 == nullKey1 (" + unassignedSentinel + ')';
        return super.ensure0(key1, key2);
    }

    @Override public long get(long key1, long key2) {
        assert key1 != unassignedSentinel : "get() called with key1 == nullKey1 (" + unassignedSentinel + ')';
        return super.get0(key1, key2);
    }

    @Override public boolean remove(long key1, long key2) {
        assert key1 != unassignedSentinel : "remove() called with key1 == nullKey1 (" + unassignedSentinel + ')';
        return super.remove0(key1, key2);
    }
}
