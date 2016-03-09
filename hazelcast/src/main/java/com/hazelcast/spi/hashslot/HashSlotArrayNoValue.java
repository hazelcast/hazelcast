package com.hazelcast.spi.hashslot;

import com.hazelcast.memory.MemoryManager;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;

/**
 * Specialization of {@link HashSlotArrayImpl} to the case of zero-length value. Suitable for a {@code long} set
 * implementation. Unassigned sentinel is kept at the start of the slot, i.e., in the key part.
 * Therefore the sentinel value cannot be used as a key.
 */
public class HashSlotArrayNoValue extends HashSlotArrayImpl {

    public HashSlotArrayNoValue(long unassignedSentinel, MemoryManager mm, int valueLength, int initialCapacity) {
        super(unassignedSentinel, 0L, mm, valueLength, initialCapacity);
    }

    public HashSlotArrayNoValue(long unassignedSentinel, MemoryManager mm, int valueLength) {
        super(unassignedSentinel, 0L, mm, valueLength, DEFAULT_CAPACITY);
    }
}
