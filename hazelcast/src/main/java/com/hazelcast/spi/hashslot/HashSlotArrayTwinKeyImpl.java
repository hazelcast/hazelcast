package com.hazelcast.spi.hashslot;

import com.hazelcast.memory.MemoryManager;

import static com.hazelcast.elastic.CapacityUtil.DEFAULT_CAPACITY;

/**
 * Implementation of {@link HashSlotArrayTwinKey}.
 * <p>
 * This class uses the first 8 bytes of the value block for the unassigned sentinel.
 * <strong>It is the responsibility of the caller to ensure that the unassigned sentinel
 * is overwritten with a non-sentinel value as soon as a new slot is assigned (after calling
 * {@link #ensure(long, long)} and getting a positive return value).</strong>
 * For the same reason this class must not be instantiated with zero value length. Use
 * {@link HashSlotArrayTwinKeyNoValue} as a zero-length key implementation.
 */
public class HashSlotArrayTwinKeyImpl extends HashSlotArrayBase implements HashSlotArrayTwinKey {

    private static final int KEY_LENGTH = 16;

    public HashSlotArrayTwinKeyImpl(long nullSentinel, MemoryManager mm, int valueLength, int initialCapacity) {
        this(nullSentinel, KEY_LENGTH, mm, valueLength, initialCapacity);
        assert valueLengthValid(valueLength) : "Invalid value length: " + valueLength;
    }

    public HashSlotArrayTwinKeyImpl(long nullSentinel, MemoryManager mm, int valueLength) {
        this(nullSentinel, mm, valueLength, DEFAULT_CAPACITY);
    }

    protected HashSlotArrayTwinKeyImpl(long nullSentinel, long offsetOfNullSentinel, MemoryManager mm,
                                       int valueLength, int initialCapacity
    ) {
        super(nullSentinel, offsetOfNullSentinel, mm, KEY_LENGTH, valueLength, initialCapacity);
    }

    /**
     * {@inheritDoc}
     *
     * Whenever this method returns a positive value, the caller must ensure that the null-sentinel value
     * at the returned address is overwritten with a non-null-sentinel value.
     */
    @Override public long ensure(long key1, long key2) {
        return super.ensure0(key1, key2);
    }

    @Override public long get(long key1, long key2) {
        return super.get0(key1, key2);
    }

    @Override public boolean remove(long key1, long key2) {
        return super.remove0(key1, key2);
    }

    @Override public HashSlotCursorTwinKey cursor() {
        return new Cursor();
    }

    protected boolean valueLengthValid(int valueLength) {
        return valueLength > 0;
    }
}
