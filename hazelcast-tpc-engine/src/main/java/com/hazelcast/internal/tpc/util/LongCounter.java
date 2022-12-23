package com.hazelcast.internal.tpc.util;


import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class LongCounter {

    private final static Unsafe UNSAFE = UnsafeUtil.UNSAFE;
    private static final long OFFSET;

    static {
        Field field = null;
        try {
            field = LongCounter.class.getDeclaredField("value");
        } catch (NoSuchFieldException ignore) {
            Util.ignore(ignore);
        }
        OFFSET = UNSAFE.objectFieldOffset(field);
    }

    private volatile long value;

    public LongCounter() {
    }

    @SuppressWarnings("checkstyle:innerassignment")
    public long inc() {
        final long newLocalValue = value + 1;
        UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        return newLocalValue;
    }

    @SuppressWarnings("checkstyle:innerassignment")
    public long inc(long amount) {
        final long newLocalValue = value + amount;
        UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        return newLocalValue;
    }

    public long get() {
        return value;
    }

    @Override
    public String toString() {
        return "" + value;
    }
}
