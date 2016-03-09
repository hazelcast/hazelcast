package com.hazelcast.spi.hashslot;

import com.hazelcast.memory.MemoryManager;

import static com.hazelcast.spi.hashslot.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.util.HashUtil.fastLongMix;

/**
 * Implementation of {@link HashSlotArray} as a restriction of {@link HashSlotArrayBase}
 * to a case where just {@code key1} is used.
 * <p>
 * This class uses the first 8 bytes of the value block for the unassigned sentinel.
 * <strong>It is the responsibility of the caller to ensure that the unassigned sentinel
 * is overwritten with a non-sentinel value as soon as a new slot is assigned (after calling
 * {@link #ensure(long)} and getting a positive return value).</strong>
 * For the same reason this class must not be instantiated with zero value length. Use
 * {@link HashSlotArrayNoValue} as a zero-length key implementation.
 */
public class HashSlotArrayImpl extends HashSlotArrayBase implements HashSlotArray {

    private static final int KEY_LENGTH = 8;

    public HashSlotArrayImpl(long unassignedSentinel, MemoryManager mm, int valueLength, int initialCapacity) {
        this(unassignedSentinel, KEY_LENGTH, mm, valueLength, initialCapacity);
        assert valueLength > 0 : "Attempted to instantiate HashSlotArrayImpl with zero value length";
    }

    public HashSlotArrayImpl(long unassignedSentinel, MemoryManager mm, int valueLength) {
        this(unassignedSentinel, mm, valueLength, DEFAULT_CAPACITY);
        assert valueLength > 0 : "Attempted to instantiate HashSlotArrayImpl with zero value length";
    }

    protected HashSlotArrayImpl(long unassignedSentinel, long offsetOfUnassignedSentinel,
                                MemoryManager mm, int valueLength, int initialCapacity
    ) {
        super(unassignedSentinel, offsetOfUnassignedSentinel, mm, KEY_LENGTH, valueLength, initialCapacity);
    }

    /**
     * {@inheritDoc}
     *
     * Whenever this method returns a positive value, the caller must ensure that the null-sentinel value
     * at the returned address is overwritten with a non-sentinel value.
     */
    @Override public long ensure(long key) {
        return super.ensure0(key, 0);
    }

    @Override public long get(long key) {
        return super.get0(key, 0);
    }

    @Override public boolean remove(long key) {
        return super.remove0(key, 0);
    }

    @Override public HashSlotCursor cursor() {
        return new Cursor();
    }

    @Override protected long key2OfSlot(long baseAddress, long slot) {
        return 0;
    }

    @Override protected long key2OfSlot(long slot) {
        return 0;
    }

    @Override protected long hash(long key, long ignored) {
        return fastLongMix(key);
    }

    @Override protected void putKey(long slot, long key, long ignored) {
        mem.putLong(slotBase(slot) + KEY_1_OFFSET, key);
    }
}
