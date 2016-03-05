package com.hazelcast.jet.memory.impl.binarystorage.hsa;


import com.hazelcast.internal.memory.MemoryManager;

import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
 * Specialization of {@link com.hazelcast.internal.util.hashslot.impl.HashSlotArray8byteKeyImpl} to the case of zero-length value. Suitable for a {@code long} set
 * implementation. Unassigned sentinel is kept at the start of the slot, i.e., in the key part.
 * Therefore the sentinel value cannot be used as a key.
 */
public abstract class HashSlotArray8byteKeyNoValue extends HashSlotArray8byteKeyImpl {

    public HashSlotArray8byteKeyNoValue(long unassignedSentinel, MemoryManager mm,
                                        int initialCapacity, float loadFactor) {
        super(unassignedSentinel, 0L, mm, 0, initialCapacity, loadFactor);
    }

    public HashSlotArray8byteKeyNoValue(long unassignedSentinel, MemoryManager mm) {
        super(unassignedSentinel, 0L, mm, 0, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }
}
