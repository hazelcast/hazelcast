package com.hazelcast.nio.serialization;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
* @mdogan 6/14/13
*/
final class UnsafeHelper {

    static final Unsafe UNSAFE;

    static final long BYTE_ARRAY_BASE_OFFSET;
    static final long SHORT_ARRAY_BASE_OFFSET;
    static final long CHAR_ARRAY_BASE_OFFSET;
    static final long INT_ARRAY_BASE_OFFSET;
    static final long FLOAT_ARRAY_BASE_OFFSET;
    static final long LONG_ARRAY_BASE_OFFSET;
    static final long DOUBLE_ARRAY_BASE_OFFSET;

    static final int BYTE_ARRAY_INDEX_SCALE;
    static final int SHORT_ARRAY_INDEX_SCALE;
    static final int CHAR_ARRAY_INDEX_SCALE;
    static final int INT_ARRAY_INDEX_SCALE;
    static final int FLOAT_ARRAY_INDEX_SCALE;
    static final int LONG_ARRAY_INDEX_SCALE;
    static final int DOUBLE_ARRAY_INDEX_SCALE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);

            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
            SHORT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(short[].class);
            CHAR_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(char[].class);
            INT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(int[].class);
            FLOAT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(float[].class);
            LONG_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(long[].class);
            DOUBLE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(double[].class);

            BYTE_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(byte[].class);
            SHORT_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(short[].class);
            CHAR_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(char[].class);
            INT_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(int[].class);
            FLOAT_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(float[].class);
            LONG_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(long[].class);
            DOUBLE_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(double[].class);

            // test if unsafe has required methods...
            byte[] buffer = new byte[8];
            unsafe.putChar(buffer, BYTE_ARRAY_BASE_OFFSET, '0');
            unsafe.putShort(buffer, BYTE_ARRAY_BASE_OFFSET, (short) 1);
            unsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET, 2);
            unsafe.putFloat(buffer, BYTE_ARRAY_BASE_OFFSET, 3f);
            unsafe.putLong(buffer, BYTE_ARRAY_BASE_OFFSET, 4L);
            unsafe.putDouble(buffer, BYTE_ARRAY_BASE_OFFSET, 5d);
            unsafe.copyMemory(new byte[8], BYTE_ARRAY_BASE_OFFSET, buffer, BYTE_ARRAY_BASE_OFFSET, buffer.length);

            UNSAFE = unsafe;
        } catch (Throwable e) {
            throw new HazelcastSerializationException(e);
        }
    }
}
