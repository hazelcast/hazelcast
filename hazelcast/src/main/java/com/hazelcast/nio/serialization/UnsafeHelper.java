package com.hazelcast.nio.serialization;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
* @mdogan 6/14/13
*/
final class UnsafeHelper {

    static final Unsafe UNSAFE;
    static final long BYTE_ARRAY_BASE_OFFSET;
    static final long FLOAT_ARRAY_BASE_OFFSET;
    static final long DOUBLE_ARRAY_BASE_OFFSET;
    static final long INT_ARRAY_BASE_OFFSET;
    static final long LONG_ARRAY_BASE_OFFSET;
    static final long SHORT_ARRAY_BASE_OFFSET;
    static final long CHAR_ARRAY_BASE_OFFSET;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            CHAR_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(char[].class);
            SHORT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
            INT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
            FLOAT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
            LONG_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
            DOUBLE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
        } catch (Throwable e) {
            throw new HazelcastSerializationException(e);
        }
    }
}
