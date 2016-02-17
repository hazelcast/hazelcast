package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.UnsafeDependentMemoryAccessorTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import sun.misc.Unsafe;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public abstract class BaseMemoryAccessorTest extends UnsafeDependentMemoryAccessorTest {

    protected final Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    private MemoryAccessor memoryAccessor;

    abstract protected MemoryAccessor createMemoryAccessor();

    @Before
    public void setup() {
        memoryAccessor = createMemoryAccessor();
    }

    protected long allocateMemory(long size) {
        return UNSAFE.allocateMemory(size);
    }

    protected void freeMemory(long address) {
        UNSAFE.freeMemory(address);
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_getObjectFieldOffset() throws NoSuchFieldException {
        assertEquals(SampleObject.BOOLEAN_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("booleanValue")));
        assertEquals(SampleObject.BYTE_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("byteValue")));
        assertEquals(SampleObject.CHAR_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("charValue")));
        assertEquals(SampleObject.SHORT_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("shortValue")));
        assertEquals(SampleObject.INT_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("intValue")));
        assertEquals(SampleObject.FLOAT_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("floatValue")));
        assertEquals(SampleObject.LONG_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("longValue")));
        assertEquals(SampleObject.DOUBLE_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("doubleValue")));
        assertEquals(SampleObject.OBJECT_VALUE_OFFSET,
                     memoryAccessor.objectFieldOffset(
                        SampleObject.class.getDeclaredField("objectValue")));
    }

    @Test
    public void test_getArrayBaseOffset() {
        assertEquals(UNSAFE.arrayBaseOffset(boolean[].class),
                     memoryAccessor.arrayBaseOffset(boolean[].class));
        assertEquals(UNSAFE.arrayBaseOffset(byte[].class),
                     memoryAccessor.arrayBaseOffset(byte[].class));
        assertEquals(UNSAFE.arrayBaseOffset(char[].class),
                     memoryAccessor.arrayBaseOffset(char[].class));
        assertEquals(UNSAFE.arrayBaseOffset(short[].class),
                     memoryAccessor.arrayBaseOffset(short[].class));
        assertEquals(UNSAFE.arrayBaseOffset(int[].class),
                     memoryAccessor.arrayBaseOffset(int[].class));
        assertEquals(UNSAFE.arrayBaseOffset(float[].class),
                     memoryAccessor.arrayBaseOffset(float[].class));
        assertEquals(UNSAFE.arrayBaseOffset(long[].class),
                     memoryAccessor.arrayBaseOffset(long[].class));
        assertEquals(UNSAFE.arrayBaseOffset(double[].class),
                     memoryAccessor.arrayBaseOffset(double[].class));
        assertEquals(UNSAFE.arrayBaseOffset(Object[].class),
                     memoryAccessor.arrayBaseOffset(Object[].class));
    }

    @Test
    public void test_getArrayIndexScale() {
        assertEquals(UNSAFE.arrayIndexScale(boolean[].class),
                     memoryAccessor.arrayIndexScale(boolean[].class));
        assertEquals(UNSAFE.arrayIndexScale(byte[].class),
                     memoryAccessor.arrayIndexScale(byte[].class));
        assertEquals(UNSAFE.arrayIndexScale(char[].class),
                     memoryAccessor.arrayIndexScale(char[].class));
        assertEquals(UNSAFE.arrayIndexScale(short[].class),
                     memoryAccessor.arrayIndexScale(short[].class));
        assertEquals(UNSAFE.arrayIndexScale(int[].class),
                     memoryAccessor.arrayIndexScale(int[].class));
        assertEquals(UNSAFE.arrayIndexScale(float[].class),
                     memoryAccessor.arrayIndexScale(float[].class));
        assertEquals(UNSAFE.arrayIndexScale(long[].class),
                     memoryAccessor.arrayIndexScale(long[].class));
        assertEquals(UNSAFE.arrayIndexScale(double[].class),
                     memoryAccessor.arrayIndexScale(double[].class));
        assertEquals(UNSAFE.arrayIndexScale(Object[].class),
                     memoryAccessor.arrayIndexScale(Object[].class));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_copyMemory_whenAligned() {
        do_test_copyMemory(true);
    }

    @Test
    public void test_copyMemory_whenUnaligned() {
        do_test_copyMemory(false);
    }

    private void do_test_copyMemory(boolean aligned) {
        final int COPY_LENGTH = 8;

        long sourceAddress = 0;
        long destinationAddress = 0;
        try {
            sourceAddress = allocateMemory(2 * COPY_LENGTH);
            destinationAddress = allocateMemory(2 * COPY_LENGTH);

            long accessSourceAddress = aligned ? sourceAddress : sourceAddress + 1;
            long accessDestinationAddress = aligned ? destinationAddress : destinationAddress + 1;

            for (int i = 0; i < COPY_LENGTH; i++) {
                memoryAccessor.putByte(accessSourceAddress + i, (byte) (i * i));
            }

            memoryAccessor.copyMemory(accessSourceAddress, accessDestinationAddress, COPY_LENGTH);

            for (int i = 0; i < COPY_LENGTH; i++) {
                assertEquals((byte) (i * i), memoryAccessor.getByte(accessDestinationAddress + i));
            }

            byte[] src = new byte[] {0x11, 0x22, 0x33, 0x44};
            byte[] dest = new byte[src.length];

            memoryAccessor.copyMemory(src, MemoryAccessor.ARRAY_BYTE_BASE_OFFSET,
                                      dest, MemoryAccessor.ARRAY_BYTE_BASE_OFFSET,
                                      src.length);

            assertArrayEquals(src, dest);
        } finally {
            if (sourceAddress != 0) {
                freeMemory(sourceAddress);
            }
            if (destinationAddress != 0) {
                freeMemory(destinationAddress);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_setMemory_whenAligned() {
        do_test_setMemory(true);
    }

    @Test
    public void test_setMemory_whenUnaligned() {
        do_test_setMemory(false);
    }

    private void do_test_setMemory(boolean aligned) {
        final int SET_LENGTH = 8;

        long address = 0;
        try {
            address = allocateMemory(2 * SET_LENGTH);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.setMemory(accessAddress, SET_LENGTH, (byte) 0x01);

            for (int i = 0; i < SET_LENGTH; i++) {
                assertEquals((byte) 0x01, memoryAccessor.getByte(accessAddress + i));
            }
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetBoolean() {
        long address = 0;
        try {
            address = allocateMemory(16);

            memoryAccessor.putBoolean(address, true);
            assertEquals(true, memoryAccessor.getBoolean(address));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putBooleanVolatile(null, address, false);
            assertEquals(false, memoryAccessor.getBooleanVolatile(null, address));

            SampleObject obj = new SampleObject();

            memoryAccessor.putBoolean(obj, SampleObject.BOOLEAN_VALUE_OFFSET, true);
            assertEquals(true, memoryAccessor.getBoolean(obj, SampleObject.BOOLEAN_VALUE_OFFSET));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putBooleanVolatile(obj, SampleObject.BOOLEAN_VALUE_OFFSET, false);
            assertEquals(false, memoryAccessor.getBooleanVolatile(obj, SampleObject.BOOLEAN_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetByte() {
        long address = 0;
        try {
            address = allocateMemory(16);

            memoryAccessor.putByte(address, (byte) 1);
            assertEquals(1, memoryAccessor.getByte(address));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putByteVolatile(null, address, (byte) 2);
            assertEquals(2, memoryAccessor.getByteVolatile(null, address));

            SampleObject obj = new SampleObject();

            memoryAccessor.putByte(obj, SampleObject.BYTE_VALUE_OFFSET, (byte) 3);
            assertEquals(3, memoryAccessor.getByte(obj, SampleObject.BYTE_VALUE_OFFSET));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putByteVolatile(obj, SampleObject.BYTE_VALUE_OFFSET, (byte) 4);
            assertEquals(4, memoryAccessor.getByteVolatile(obj, SampleObject.BYTE_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetChar_whenAligned() {
        do_test_putGetChar(true);
    }

    @Test
    public void test_putGetChar_whenUnaligned() {
        do_test_putGetChar(false);
    }

    private void do_test_putGetChar(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putChar(accessAddress, 'A');
            assertEquals('A', memoryAccessor.getChar(accessAddress));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putCharVolatile(null, accessAddress, 'B');
            assertEquals('B', memoryAccessor.getCharVolatile(null, accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putChar(obj, SampleObject.CHAR_VALUE_OFFSET, 'C');
            assertEquals('C', memoryAccessor.getChar(obj, SampleObject.CHAR_VALUE_OFFSET));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putCharVolatile(obj, SampleObject.CHAR_VALUE_OFFSET, 'D');
            assertEquals('D', memoryAccessor.getCharVolatile(obj, SampleObject.CHAR_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetShort_whenAligned() {
        do_test_putGetShort(true);
    }

    @Test
    public void test_putGetShort_whenUnaligned() {
        do_test_putGetShort(false);
    }

    private void do_test_putGetShort(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putShort(accessAddress, (short) 1);
            assertEquals((short) 1, memoryAccessor.getShort(accessAddress));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putShortVolatile(null, accessAddress, (short) 2);
            assertEquals((short) 2, memoryAccessor.getShortVolatile(null, accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putShort(obj, SampleObject.SHORT_VALUE_OFFSET, (short) 3);
            assertEquals((short) 3, memoryAccessor.getShort(obj, SampleObject.SHORT_VALUE_OFFSET));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putShortVolatile(obj, SampleObject.SHORT_VALUE_OFFSET, (short) 4);
            assertEquals((short) 4, memoryAccessor.getShortVolatile(obj, SampleObject.SHORT_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetInt_whenAligned() {
        do_test_putGetInt(true);
    }

    @Test
    public void test_putGetInt_whenUnaligned() {
        do_test_putGetInt(false);
    }

    private void do_test_putGetInt(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putInt(accessAddress, 1);
            assertEquals(1, memoryAccessor.getInt(accessAddress));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putIntVolatile(null, accessAddress, 2);
            assertEquals(2, memoryAccessor.getIntVolatile(null, accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putInt(obj, SampleObject.INT_VALUE_OFFSET, 3);
            assertEquals(3, memoryAccessor.getInt(obj, SampleObject.INT_VALUE_OFFSET));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putIntVolatile(obj, SampleObject.INT_VALUE_OFFSET, 4);
            assertEquals(4, memoryAccessor.getIntVolatile(obj, SampleObject.INT_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetFloat_whenAligned() {
        do_test_putGetFloat(true);
    }

    @Test
    public void test_putGetFloat_whenUnaligned() {
        do_test_putGetFloat(false);
    }

    private void do_test_putGetFloat(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putFloat(accessAddress, 11.2F);
            assertEquals(11.2F, memoryAccessor.getFloat(accessAddress), 0.0F);

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putFloatVolatile(null, accessAddress, 33.4F);
            assertEquals(33.4F, memoryAccessor.getFloatVolatile(null, accessAddress), 0.0F);

            SampleObject obj = new SampleObject();

            memoryAccessor.putFloat(obj, SampleObject.FLOAT_VALUE_OFFSET, 55.6F);
            assertEquals(55.6F, memoryAccessor.getFloat(obj, SampleObject.FLOAT_VALUE_OFFSET), 0.0F);

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putFloatVolatile(obj, SampleObject.FLOAT_VALUE_OFFSET, 77.8F);
            assertEquals(77.8F, memoryAccessor.getFloatVolatile(obj, SampleObject.FLOAT_VALUE_OFFSET), 0.0F);
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetLong_whenAligned() {
        do_test_putGetLong(true);
    }

    @Test
    public void test_putGetLong_whenUnaligned() {
        do_test_putGetLong(false);
    }

    private void do_test_putGetLong(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putLong(accessAddress, 1L);
            assertEquals(1L, memoryAccessor.getLong(accessAddress));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putLongVolatile(null, accessAddress, 2L);
            assertEquals(2L, memoryAccessor.getLongVolatile(null, accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putLong(obj, SampleObject.LONG_VALUE_OFFSET, 3L);
            assertEquals(3L, memoryAccessor.getLong(obj, SampleObject.LONG_VALUE_OFFSET));

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putLongVolatile(obj, SampleObject.LONG_VALUE_OFFSET, 4L);
            assertEquals(4L, memoryAccessor.getLongVolatile(obj, SampleObject.LONG_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetDouble_whenAligned() {
        do_test_putGetDouble(true);
    }

    @Test
    public void test_putGetDouble_whenUnaligned() {
        do_test_putGetDouble(false);
    }

    private void do_test_putGetDouble(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putDouble(accessAddress, 11.2);
            assertEquals(11.2, memoryAccessor.getDouble(accessAddress), 0.0);

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putDoubleVolatile(null, accessAddress, 33.4);
            assertEquals(33.4, memoryAccessor.getDoubleVolatile(null, accessAddress), 0.0);

            SampleObject obj = new SampleObject();

            memoryAccessor.putDouble(obj, SampleObject.DOUBLE_VALUE_OFFSET, 55.6);
            assertEquals(55.6, memoryAccessor.getDouble(obj, SampleObject.DOUBLE_VALUE_OFFSET), 0.0);

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really accesses memory as volatile. Does it worth???
            memoryAccessor.putDoubleVolatile(obj, SampleObject.DOUBLE_VALUE_OFFSET, 77.8);
            assertEquals(77.8, memoryAccessor.getDoubleVolatile(obj, SampleObject.DOUBLE_VALUE_OFFSET), 0.0);
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetObject_whenAligned() {
        do_test_putGetObject(true);
    }

    protected void do_test_putGetObject(boolean aligned) {
        long offset = SampleObject.OBJECT_VALUE_OFFSET;
        SampleObject obj = new SampleObject();
        long accessOffset = aligned ? offset : offset + 1;

        String str1 = "String Object 1";
        memoryAccessor.putObject(obj, accessOffset, str1);
        assertEquals(str1, memoryAccessor.getObject(obj, accessOffset));

        // TODO Do we really need to concurrency test to verify that
        // memory accessor really accesses memory as volatile. Does it worth???
        String str2 = "String Object 2";
        memoryAccessor.putObjectVolatile(obj, accessOffset, str2);
        assertEquals(str2, memoryAccessor.getObjectVolatile(obj, accessOffset));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_compareAndSwapInt_whenAligned() {
        do_test_compareAndSwapInt(true);
    }

    @Test
    public void test_compareAndSwapInt_whenUnaligned() {
        do_test_compareAndSwapInt(false);
    }

    private void do_test_compareAndSwapInt(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putInt(accessAddress, 1);
            assertEquals(1, memoryAccessor.getInt(accessAddress));

            assertFalse(memoryAccessor.compareAndSwapInt(null, accessAddress, 0, 2));
            assertTrue(memoryAccessor.compareAndSwapInt(null, accessAddress, 1, 2));

            assertEquals(2, memoryAccessor.getInt(accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putInt(obj, SampleObject.INT_VALUE_OFFSET, 1);
            assertEquals(1, memoryAccessor.getInt(obj, SampleObject.INT_VALUE_OFFSET));

            assertFalse(memoryAccessor.compareAndSwapInt(obj, SampleObject.INT_VALUE_OFFSET, 0, 2));
            assertTrue(memoryAccessor.compareAndSwapInt(obj, SampleObject.INT_VALUE_OFFSET, 1, 2));

            assertEquals(2, memoryAccessor.getInt(obj, SampleObject.INT_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_compareAndSwapLong_whenAligned() {
        do_test_compareAndSwapLong(true);
    }

    @Test
    public void test_compareAndSwapLong_whenUnaligned() {
        do_test_compareAndSwapLong(false);
    }

    private void do_test_compareAndSwapLong(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            memoryAccessor.putLong(accessAddress, 1L);
            assertEquals(1L, memoryAccessor.getLong(accessAddress));

            assertFalse(memoryAccessor.compareAndSwapLong(null, accessAddress, 0L, 2L));
            assertTrue(memoryAccessor.compareAndSwapLong(null, accessAddress, 1L, 2L));

            assertEquals(2L, memoryAccessor.getLong(accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putLong(obj, SampleObject.LONG_VALUE_OFFSET, 1L);
            assertEquals(1L, memoryAccessor.getLong(obj, SampleObject.LONG_VALUE_OFFSET));

            assertFalse(memoryAccessor.compareAndSwapLong(obj, SampleObject.LONG_VALUE_OFFSET, 0L, 2L));
            assertTrue(memoryAccessor.compareAndSwapLong(obj, SampleObject.LONG_VALUE_OFFSET, 1L, 2L));

            assertEquals(2L, memoryAccessor.getLong(obj, SampleObject.LONG_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_compareAndSwapObject_whenAligned() {
        do_test_compareAndSwapObject(true);
    }

    protected void do_test_compareAndSwapObject(boolean aligned) {
        long offset = SampleObject.OBJECT_VALUE_OFFSET;
        SampleObject obj = new SampleObject();
        long accessOffset = aligned ? offset : offset + 1;

        String str1 = "String Object 1";
        memoryAccessor.putObject(obj, accessOffset, str1);
        assertEquals(str1, memoryAccessor.getObject(obj, accessOffset));

        String str2 = "String Object 2";
        assertFalse(memoryAccessor.compareAndSwapObject(obj, accessOffset, null, str2));
        assertTrue(memoryAccessor.compareAndSwapObject(obj, accessOffset, str1, str2));

        assertEquals(str2, memoryAccessor.getObject(obj, accessOffset));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putOrderedInt_whenAligned() {
        do_test_putOrderedInt(true);
    }

    @Test
    public void test_putOrderedInt_whenUnaligned() {
        do_test_putOrderedInt(false);
    }

    private void do_test_putOrderedInt(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really applies writes as ordered. Does it worth???
            memoryAccessor.putOrderedInt(null, accessAddress, 1);
            assertEquals(1, memoryAccessor.getInt(accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putOrderedInt(obj, SampleObject.INT_VALUE_OFFSET, 1);
            assertEquals(1, memoryAccessor.getInt(obj, SampleObject.INT_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putOrderedLong_whenAligned() {
        do_test_putOrderedLong(true);
    }

    @Test
    public void test_putOrderedLong_whenUnaligned() {
        do_test_putOrderedLong(false);
    }

    private void do_test_putOrderedLong(boolean aligned) {
        long address = 0;
        try {
            address = allocateMemory(16);
            long accessAddress = aligned ? address : address + 1;

            // TODO Do we really need to concurrency test to verify that
            // memory accessor really applies writes as ordered. Does it worth???
            memoryAccessor.putOrderedLong(null, accessAddress, 1L);
            assertEquals(1L, memoryAccessor.getLong(accessAddress));

            SampleObject obj = new SampleObject();

            memoryAccessor.putOrderedLong(obj, SampleObject.LONG_VALUE_OFFSET, 1L);
            assertEquals(1L, memoryAccessor.getLong(obj, SampleObject.LONG_VALUE_OFFSET));
        } finally {
            if (address != 0) {
                freeMemory(address);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putOrderedObject_whenAligned() {
        do_test_putOrderedObject(true);
    }

    protected void do_test_putOrderedObject(boolean aligned) {
        long offset = SampleObject.OBJECT_VALUE_OFFSET;
        SampleObject obj = new SampleObject();
        long accessOffset = aligned ? offset : offset + 1;

        // TODO Do we really need to concurrency test to verify that
        // memory accessor really applies writes as ordered. Does it worth???
        String str = "String Object";
        memoryAccessor.putOrderedObject(obj, accessOffset, str);
        assertEquals(str, memoryAccessor.getObject(obj, accessOffset));
    }

    ////////////////////////////////////////////////////////////////////////////////

    private static long getSampleObjectFieldOffset(String fieldName) {
        try {
            return UnsafeUtil.UNSAFE.objectFieldOffset(SampleObject.class.getDeclaredField(fieldName));
        } catch (NoSuchFieldException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static class SampleObject {

        private static final long BYTE_VALUE_OFFSET = getSampleObjectFieldOffset("byteValue");
        private static final long BOOLEAN_VALUE_OFFSET = getSampleObjectFieldOffset("booleanValue");
        private static final long CHAR_VALUE_OFFSET = getSampleObjectFieldOffset("charValue");
        private static final long SHORT_VALUE_OFFSET = getSampleObjectFieldOffset("shortValue");
        private static final long INT_VALUE_OFFSET = getSampleObjectFieldOffset("intValue");
        private static final long FLOAT_VALUE_OFFSET = getSampleObjectFieldOffset("floatValue");
        private static final long LONG_VALUE_OFFSET = getSampleObjectFieldOffset("longValue");
        private static final long DOUBLE_VALUE_OFFSET = getSampleObjectFieldOffset("doubleValue");
        private static final long OBJECT_VALUE_OFFSET = getSampleObjectFieldOffset("objectValue");

        private byte byteValue;
        private boolean booleanValue;
        private char charValue;
        private short shortValue;
        private int intValue;
        private float floatValue;
        private long longValue;
        private double doubleValue;
        private Object objectValue;

    }
}
