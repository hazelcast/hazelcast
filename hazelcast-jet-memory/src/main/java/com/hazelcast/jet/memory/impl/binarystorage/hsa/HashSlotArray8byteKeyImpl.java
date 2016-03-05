package com.hazelcast.jet.memory.impl.binarystorage.hsa;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;

import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.util.HashUtil.fastLongMix;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;

/**
 * Implementation of {@link HashSlotArray8byteKey} as a restriction of {@link com.hazelcast.internal.util.hashslot.impl.HashSlotArrayBase}
 * to a case where just {@code key1} is used.
 * <p>
 * This class uses the first 8 bytes of the value block for the unassigned sentinel.
 * <strong>It is the responsibility of the caller to ensure that the unassigned sentinel
 * is overwritten with a non-sentinel value as soon as a new slot is assigned (after calling
 * {@link #ensure(long)} and getting a positive return value).</strong>
 * For the same reason this class must not be instantiated with zero value length. Use
 * {@link HashSlotArray8byteKeyNoValue} as a zero-length key implementation.
 */
public abstract class HashSlotArray8byteKeyImpl extends HashSlotArrayBase implements HashSlotArray8byteKey {
    protected static final int KEY_SIZE = 8;

    public HashSlotArray8byteKeyImpl(long unassignedSentinel, MemoryManager mm, int valueLength,
                                     int initialCapacity, float loadFactor) {
        this(unassignedSentinel, KEY_SIZE, mm, valueLength, initialCapacity, loadFactor);
        assert valueLength > 0 : "Attempted to instantiate HashSlotArrayImpl with zero value length";
    }

    public HashSlotArray8byteKeyImpl(long unassignedSentinel, MemoryManager mm, int valueLength) {
        this(unassignedSentinel, mm, valueLength, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
        assert valueLength > 0 : "Attempted to instantiate HashSlotArrayImpl with zero value length";
    }

    protected HashSlotArray8byteKeyImpl(long unassignedSentinel, long offsetOfUnassignedSentinel,
                                        MemoryManager mm, int valueLength, int initialCapacity, float loadFactor
    ) {
        super(unassignedSentinel, offsetOfUnassignedSentinel, mm, null, KEY_SIZE, valueLength,
                initialCapacity, loadFactor);
        assert modPowerOfTwo(valueLength, VALUE_SIZE_GRANULARITY) == 0
                : "Value size must be a positive multiple of 8, but was " + valueLength;
    }

    @Override
    public int keySize() {
        return KEY_SIZE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Whenever this method returns a positive value, the caller must ensure that the null-sentinel value
     * at the returned address is overwritten with a non-sentinel value.
     */
    @Override
    public long ensure(long key) {
        return super.ensure0(key, 0);
    }

    @Override
    public long get(long key) {
        return super.get0(key, 0);
    }

    @Override
    public boolean remove(long key) {
        return super.remove0(key, 0);
    }

    @Override
    public HashSlotCursor8byteKey cursor() {
        return new Cursor();
    }

    @Override
    protected long key2OfSlot(long baseAddress, long slot) {
        return 0;
    }

    @Override
    protected void putKey(long baseAddress, long slot, long key, long ignored) {
        mem.putLong(slotBase(baseAddress, slot) + KEY_1_OFFSET, key);
    }

    @Override
    protected long keyHash(long key, long ignored) {
        return fastLongMix(key);
    }
}
