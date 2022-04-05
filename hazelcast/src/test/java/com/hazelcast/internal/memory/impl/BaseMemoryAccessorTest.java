/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import sun.misc.Unsafe;

import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public abstract class BaseMemoryAccessorTest extends AbstractUnsafeDependentMemoryAccessorTest {

    private static final int ALLOCATED_BLOCK_SIZE = 16;

    private static final int CHAR_MISALIGNMENT = 1;
    private static final int SHORT_MISALIGNMENT = 1;
    private static final int INT_MISALIGNMENT = 2;
    private static final int FLOAT_MISALIGNMENT = 2;
    private static final int LONG_MISALIGNMENT = 4;
    private static final int DOUBLE_MISALIGNMENT = 4;
    private static final int OBJECT_MISALIGNMENT = 2;

    protected final Unsafe unsafe = UnsafeUtil.UNSAFE;

    private GlobalMemoryAccessor memoryAccessor;

    private final SampleObject sampleObject = new SampleObject();

    private long baseAddress1;

    private long baseAddress2;

    @Before
    public void setup() {
        baseAddress1 = allocate();
        baseAddress2 = allocate();
        memoryAccessor = getMemoryAccessor();
    }

    @After
    public void tearDown() {
        free(baseAddress1);
        free(baseAddress2);
    }

    private long allocate() {
        return unsafe.allocateMemory(ALLOCATED_BLOCK_SIZE);
    }

    private void free(long address) {
        if (address != 0) {
            unsafe.freeMemory(address);
        }
    }

    protected abstract GlobalMemoryAccessor getMemoryAccessor();

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_getObjectFieldOffset() throws NoSuchFieldException {
        final Class<SampleObjectBase> klass = SampleObjectBase.class;
        assertEquals(SampleObject.BOOLEAN_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("booleanValue")));
        assertEquals(SampleObject.BYTE_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("byteValue")));
        assertEquals(SampleObject.CHAR_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("charValue")));
        assertEquals(SampleObject.SHORT_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("shortValue")));
        assertEquals(SampleObject.INT_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("intValue")));
        assertEquals(SampleObject.FLOAT_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("floatValue")));
        assertEquals(SampleObject.LONG_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("longValue")));
        assertEquals(SampleObject.DOUBLE_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("doubleValue")));
        assertEquals(SampleObject.OBJECT_VALUE_OFFSET,
                memoryAccessor.objectFieldOffset(klass.getDeclaredField("objectValue")));
    }

    @Test
    public void test_getArrayBaseOffset() {
        assertEquals(unsafe.arrayBaseOffset(boolean[].class),
                memoryAccessor.arrayBaseOffset(boolean[].class));
        assertEquals(unsafe.arrayBaseOffset(byte[].class),
                memoryAccessor.arrayBaseOffset(byte[].class));
        assertEquals(unsafe.arrayBaseOffset(char[].class),
                memoryAccessor.arrayBaseOffset(char[].class));
        assertEquals(unsafe.arrayBaseOffset(short[].class),
                memoryAccessor.arrayBaseOffset(short[].class));
        assertEquals(unsafe.arrayBaseOffset(int[].class),
                memoryAccessor.arrayBaseOffset(int[].class));
        assertEquals(unsafe.arrayBaseOffset(float[].class),
                memoryAccessor.arrayBaseOffset(float[].class));
        assertEquals(unsafe.arrayBaseOffset(long[].class),
                memoryAccessor.arrayBaseOffset(long[].class));
        assertEquals(unsafe.arrayBaseOffset(double[].class),
                memoryAccessor.arrayBaseOffset(double[].class));
        assertEquals(unsafe.arrayBaseOffset(Object[].class),
                memoryAccessor.arrayBaseOffset(Object[].class));
    }

    @Test
    public void test_getArrayIndexScale() {
        assertEquals(unsafe.arrayIndexScale(boolean[].class),
                memoryAccessor.arrayIndexScale(boolean[].class));
        assertEquals(unsafe.arrayIndexScale(byte[].class),
                memoryAccessor.arrayIndexScale(byte[].class));
        assertEquals(unsafe.arrayIndexScale(char[].class),
                memoryAccessor.arrayIndexScale(char[].class));
        assertEquals(unsafe.arrayIndexScale(short[].class),
                memoryAccessor.arrayIndexScale(short[].class));
        assertEquals(unsafe.arrayIndexScale(int[].class),
                memoryAccessor.arrayIndexScale(int[].class));
        assertEquals(unsafe.arrayIndexScale(float[].class),
                memoryAccessor.arrayIndexScale(float[].class));
        assertEquals(unsafe.arrayIndexScale(long[].class),
                memoryAccessor.arrayIndexScale(long[].class));
        assertEquals(unsafe.arrayIndexScale(double[].class),
                memoryAccessor.arrayIndexScale(double[].class));
        assertEquals(unsafe.arrayIndexScale(Object[].class),
                memoryAccessor.arrayIndexScale(Object[].class));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_endianness() {
        final long address = baseAddress1;

        memoryAccessor.putInt(address, 0x01000009);
        if (memoryAccessor.isBigEndian()) {
            assertEquals(0x01, memoryAccessor.getByte(address));
        } else {
            assertEquals(0x09, memoryAccessor.getByte(address));
        }
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

    void do_test_copyMemory(boolean aligned) {
        final int copyLength = ALLOCATED_BLOCK_SIZE / 2;

        long sourceAddress = aligned ? baseAddress1 : baseAddress1 + 1;
        long destinationAddress = aligned ? baseAddress2 : baseAddress2 + 3;

        for (int i = 0; i < copyLength; i++) {
            memoryAccessor.putByte(sourceAddress + i, (byte) (i * i));
        }

        memoryAccessor.copyMemory(sourceAddress, destinationAddress, copyLength);

        for (int i = 0; i < copyLength; i++) {
            assertEquals((byte) (i * i), memoryAccessor.getByte(destinationAddress + i));
        }

        byte[] src = new byte[]{0x11, 0x22, 0x33, 0x44};
        byte[] dest = new byte[src.length];

        memoryAccessor.copyMemory(src, ARRAY_BYTE_BASE_OFFSET, dest, ARRAY_BYTE_BASE_OFFSET, src.length);

        assertArrayEquals(src, dest);
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

    void do_test_setMemory(boolean aligned) {
        final int setLength = ALLOCATED_BLOCK_SIZE / 2;

        long address = aligned ? baseAddress1 : baseAddress1 + 1;

        memoryAccessor.setMemory(address, setLength, (byte) 0x01);

        for (int i = 0; i < setLength; i++) {
            assertEquals((byte) 0x01, memoryAccessor.getByte(address + i));
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_copyFromByteArray() {
        final byte[] ary = {2, 3};
        memoryAccessor.copyFromByteArray(ary, 0, baseAddress1, 2);
        for (int i = 0; i < ary.length; i++) {
            assertEquals(ary[i], memoryAccessor.getByte(baseAddress1 + i));
        }
    }

    @Test
    public void test_copyToByteArray() {
        final byte[] ary = new byte[2];
        for (int i = 0; i < ary.length; i++) {
            memoryAccessor.putByte(baseAddress1 + i, (byte) i);
        }
        memoryAccessor.copyToByteArray(baseAddress1, ary, 0, 2);
        for (int i = 0; i < ary.length; i++) {
            assertEquals(ary[i], memoryAccessor.getByte(baseAddress1 + i));
        }
    }

    @Test
    public void test_putGetBoolean() {
        final long address = baseAddress1;

        memoryAccessor.putBoolean(address, true);
        assertTrue(memoryAccessor.getBoolean(address));

        memoryAccessor.putBooleanVolatile(address, false);
        assertFalse(memoryAccessor.getBooleanVolatile(address));

        memoryAccessor.putBoolean(sampleObject, SampleObject.BOOLEAN_VALUE_OFFSET, true);
        assertTrue(memoryAccessor.getBoolean(sampleObject, SampleObject.BOOLEAN_VALUE_OFFSET));

        memoryAccessor.putBooleanVolatile(sampleObject, SampleObject.BOOLEAN_VALUE_OFFSET, false);
        assertFalse(memoryAccessor.getBooleanVolatile(sampleObject, SampleObject.BOOLEAN_VALUE_OFFSET));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetByte() {
        final long address = baseAddress1;

        memoryAccessor.putByte(address, (byte) 1);
        assertEquals(1, memoryAccessor.getByte(address));

        memoryAccessor.putByteVolatile(address, (byte) 2);
        assertEquals(2, memoryAccessor.getByteVolatile(address));

        memoryAccessor.putByte(sampleObject, SampleObject.BYTE_VALUE_OFFSET, (byte) 3);
        assertEquals(3, memoryAccessor.getByte(sampleObject, SampleObject.BYTE_VALUE_OFFSET));

        memoryAccessor.putByteVolatile(sampleObject, SampleObject.BYTE_VALUE_OFFSET, (byte) 4);
        assertEquals(4, memoryAccessor.getByteVolatile(sampleObject, SampleObject.BYTE_VALUE_OFFSET));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetChar_whenAligned() {
        do_test_putGetChar(true);
        do_test_putGetCharVolatile();
    }


    void do_test_putGetChar(boolean aligned) {
        long address = aligned ? baseAddress1 : baseAddress1 + CHAR_MISALIGNMENT;

        memoryAccessor.putChar(address, 'A');
        assertEquals('A', memoryAccessor.getChar(address));

        memoryAccessor.putChar(sampleObject, SampleObject.CHAR_VALUE_OFFSET, 'B');
        assertEquals('B', memoryAccessor.getChar(sampleObject, SampleObject.CHAR_VALUE_OFFSET));
    }

    void do_test_putGetCharVolatile() {
        long address = baseAddress1;

        memoryAccessor.putCharVolatile(address, 'C');
        assertEquals('C', memoryAccessor.getCharVolatile(address));

        memoryAccessor.putCharVolatile(sampleObject, SampleObject.CHAR_VALUE_OFFSET, 'D');
        assertEquals('D', memoryAccessor.getCharVolatile(sampleObject, SampleObject.CHAR_VALUE_OFFSET));
    }

    void assert_putGetCharVolatileUnalignedFails() {
        long address = baseAddress1 + 1;
        long objectOffset = SampleObject.CHAR_VALUE_OFFSET + CHAR_MISALIGNMENT;

        try {
            memoryAccessor.putCharVolatile(address, 'A');
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getCharVolatile(address);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.putCharVolatile(sampleObject, objectOffset, 'A');
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getCharVolatile(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }


    ////////////////////////////////////////////////////////////////////


    @Test
    public void test_putGetShort_whenAligned() {
        do_test_putGetShort(true);
        do_test_putGetShortVolatile();
    }

    void do_test_putGetShort(boolean aligned) {
        long address = aligned ? baseAddress1 : baseAddress1 + SHORT_MISALIGNMENT;

        memoryAccessor.putShort(address, (short) 1);
        assertEquals(1, memoryAccessor.getShort(address));

        memoryAccessor.putShort(sampleObject, SampleObject.SHORT_VALUE_OFFSET, (short) 2);
        assertEquals(2, memoryAccessor.getShort(sampleObject, SampleObject.SHORT_VALUE_OFFSET));
    }

    void do_test_putGetShortVolatile() {
        long address = baseAddress1;

        memoryAccessor.putShortVolatile(address, (short) 3);
        assertEquals(3, memoryAccessor.getShortVolatile(address));

        memoryAccessor.putShortVolatile(sampleObject, SampleObject.SHORT_VALUE_OFFSET, (short) 4);
        assertEquals(4, memoryAccessor.getShortVolatile(sampleObject, SampleObject.SHORT_VALUE_OFFSET));
    }

    void assert_putGetShortVolatileUnalignedFails() {
        long address = baseAddress1 + SHORT_MISALIGNMENT;
        long objectOffset = SampleObject.SHORT_VALUE_OFFSET + SHORT_MISALIGNMENT;

        try {
            memoryAccessor.putShortVolatile(address, (short) 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getShortVolatile(address);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.putShortVolatile(sampleObject, objectOffset, (short) 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getShortVolatile(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    ////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetInt_whenAligned() {
        do_test_putGetInt(true);
        do_test_putGetIntVolatile();
    }

    void do_test_putGetInt(boolean aligned) {
        long address = aligned ? baseAddress1 : baseAddress1 + 2;

        memoryAccessor.putInt(address, 1);
        assertEquals(1, memoryAccessor.getInt(address));

        memoryAccessor.putInt(sampleObject, SampleObject.INT_VALUE_OFFSET, 2);
        assertEquals(2, memoryAccessor.getInt(sampleObject, SampleObject.INT_VALUE_OFFSET));
    }

    void do_test_putGetIntVolatile() {
        long address = baseAddress1;

        memoryAccessor.putIntVolatile(address, 3);
        assertEquals(3, memoryAccessor.getIntVolatile(address));

        memoryAccessor.putIntVolatile(sampleObject, SampleObject.INT_VALUE_OFFSET, 4);
        assertEquals(4, memoryAccessor.getIntVolatile(sampleObject, SampleObject.INT_VALUE_OFFSET));
    }

    void assert_putGetIntVolatileUnalignedFails() {
        long address = baseAddress1 + 2;
        long objectOffset = SampleObject.INT_VALUE_OFFSET + INT_MISALIGNMENT;

        try {
            memoryAccessor.putIntVolatile(address, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getIntVolatile(address);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.putIntVolatile(sampleObject, objectOffset, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getIntVolatile(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }


    ////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetFloat_whenAligned() {
        do_test_putGetFloat(true);
        do_test_putGetFloatVolatile();
    }

    void do_test_putGetFloat(boolean aligned) {
        long address = aligned ? baseAddress1 : baseAddress1 + FLOAT_MISALIGNMENT;

        memoryAccessor.putFloat(address, 1f);
        assertEquals(1f, memoryAccessor.getFloat(address), 0f);

        memoryAccessor.putFloat(sampleObject, SampleObject.FLOAT_VALUE_OFFSET, 2f);
        assertEquals(2f, memoryAccessor.getFloat(sampleObject, SampleObject.FLOAT_VALUE_OFFSET), 0f);
    }

    void do_test_putGetFloatVolatile() {
        long address = baseAddress1;

        memoryAccessor.putFloatVolatile(address, 3f);
        assertEquals(3f, memoryAccessor.getFloatVolatile(address), 0f);

        memoryAccessor.putFloatVolatile(sampleObject, SampleObject.FLOAT_VALUE_OFFSET, 4f);
        assertEquals(4f, memoryAccessor.getFloatVolatile(sampleObject, SampleObject.FLOAT_VALUE_OFFSET), 0f);
    }

    void assert_putGetFloatVolatileUnalignedFails() {
        long address = baseAddress1 + FLOAT_MISALIGNMENT;
        long objectOffset = SampleObject.FLOAT_VALUE_OFFSET + FLOAT_MISALIGNMENT;

        try {
            memoryAccessor.putFloatVolatile(address, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getFloatVolatile(address);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.putFloatVolatile(sampleObject, objectOffset, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getFloatVolatile(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }


    ////////////////////////////////////////////////////////////////////


    @Test
    public void test_putGetLong_whenAligned() {
        do_test_putGetLong(true);
        do_test_putGetLongVolatile();
    }

    void do_test_putGetLong(boolean aligned) {
        long address = aligned ? baseAddress1 : baseAddress1 + LONG_MISALIGNMENT;

        memoryAccessor.putLong(address, 1);
        assertEquals(1, memoryAccessor.getLong(address));

        memoryAccessor.putLong(sampleObject, SampleObject.LONG_VALUE_OFFSET, 2);
        assertEquals(2, memoryAccessor.getLong(sampleObject, SampleObject.LONG_VALUE_OFFSET));
    }

    void do_test_putGetLongVolatile() {
        long address = baseAddress1;

        memoryAccessor.putLongVolatile(address, 3);
        assertEquals(3, memoryAccessor.getLongVolatile(address));

        memoryAccessor.putLongVolatile(sampleObject, SampleObject.LONG_VALUE_OFFSET, 4);
        assertEquals(4, memoryAccessor.getLongVolatile(sampleObject, SampleObject.LONG_VALUE_OFFSET));
    }

    void assert_putGetLongVolatileUnalignedFails() {
        long address = baseAddress1 + LONG_MISALIGNMENT;
        long objectOffset = SampleObject.LONG_VALUE_OFFSET + LONG_MISALIGNMENT;
        try {
            memoryAccessor.putLongVolatile(address, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getLongVolatile(address);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.putLongVolatile(sampleObject, objectOffset, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getLongVolatile(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }


    ////////////////////////////////////////////////////////////////////


    @Test
    public void test_putGetDouble_whenAligned() {
        do_test_putGetDouble(true);
        do_test_putGetDoubleVolatile();
    }

    void do_test_putGetDouble(boolean aligned) {
        long address = aligned ? baseAddress1 : baseAddress1 + DOUBLE_MISALIGNMENT;

        memoryAccessor.putDouble(address, 1f);
        assertEquals(1f, memoryAccessor.getDouble(address), 0f);

        memoryAccessor.putDouble(sampleObject, SampleObject.DOUBLE_VALUE_OFFSET, 2f);
        assertEquals(2f, memoryAccessor.getDouble(sampleObject, SampleObject.DOUBLE_VALUE_OFFSET), 0f);
    }

    void do_test_putGetDoubleVolatile() {
        long address = baseAddress1;

        memoryAccessor.putDoubleVolatile(address, 3f);
        assertEquals(3f, memoryAccessor.getDoubleVolatile(address), 0f);

        memoryAccessor.putDoubleVolatile(sampleObject, SampleObject.DOUBLE_VALUE_OFFSET, 4f);
        assertEquals(4f, memoryAccessor.getDoubleVolatile(sampleObject, SampleObject.DOUBLE_VALUE_OFFSET), 0f);
    }

    void assert_putGetDoubleVolatileUnalignedFails() {
        long address = baseAddress1 + DOUBLE_MISALIGNMENT;
        long objectOffset = SampleObject.DOUBLE_VALUE_OFFSET + DOUBLE_MISALIGNMENT;

        try {
            memoryAccessor.putDoubleVolatile(address, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getDoubleVolatile(address);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.putDoubleVolatile(sampleObject, objectOffset, 1);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getDoubleVolatile(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putGetObject_whenAligned() {
        final String value1 = "a";
        memoryAccessor.putObject(sampleObject, SampleObject.OBJECT_VALUE_OFFSET, value1);
        assertEquals(value1, memoryAccessor.getObject(sampleObject, SampleObject.OBJECT_VALUE_OFFSET));

        final String value2 = "b";
        memoryAccessor.putObjectVolatile(sampleObject, SampleObject.OBJECT_VALUE_OFFSET, value2);
        assertEquals(value2, memoryAccessor.getObjectVolatile(sampleObject, SampleObject.OBJECT_VALUE_OFFSET));
    }

    void assert_putGetObjectUnalignedFails() {
        long objectOffset = SampleObject.OBJECT_VALUE_OFFSET + OBJECT_MISALIGNMENT;
        final String value = "a";

        try {
            memoryAccessor.putObjectVolatile(sampleObject, objectOffset, value);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getObjectVolatile(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.putObject(sampleObject, objectOffset, value);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            memoryAccessor.getObject(sampleObject, objectOffset);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }


    ////////////////////////////////////////////////////////////////////////////////


    @Test
    public void test_compareAndSwapInt_whenAligned() {
        do_test_compareAndSwapInt(true);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_compareAndSwapInt_whenUnaligned() {
        do_test_compareAndSwapInt(false);
    }

    void do_test_compareAndSwapInt(boolean aligned) {
        long accessAddress = aligned ? baseAddress1 : baseAddress1 + 1;

        memoryAccessor.putInt(accessAddress, 1);
        assertEquals(1, memoryAccessor.getInt(accessAddress));

        assertFalse(memoryAccessor.compareAndSwapInt(accessAddress, 0, 2));
        assertTrue(memoryAccessor.compareAndSwapInt(accessAddress, 1, 2));

        assertEquals(2, memoryAccessor.getInt(accessAddress));

        memoryAccessor.putInt(sampleObject, SampleObject.INT_VALUE_OFFSET, 1);
        assertEquals(1, memoryAccessor.getInt(sampleObject, SampleObject.INT_VALUE_OFFSET));

        assertFalse(memoryAccessor.compareAndSwapInt(sampleObject, SampleObject.INT_VALUE_OFFSET, 0, 2));
        assertTrue(memoryAccessor.compareAndSwapInt(sampleObject, SampleObject.INT_VALUE_OFFSET, 1, 2));

        assertEquals(2, memoryAccessor.getInt(sampleObject, SampleObject.INT_VALUE_OFFSET));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_compareAndSwapLong_whenAligned() {
        do_test_compareAndSwapLong(true);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_compareAndSwapLong_whenUnaligned() {
        do_test_compareAndSwapLong(false);
    }

    void do_test_compareAndSwapLong(boolean aligned) {
        long accessAddress = aligned ? baseAddress1 : baseAddress1 + 1;

        memoryAccessor.putLong(accessAddress, 1L);
        assertEquals(1L, memoryAccessor.getLong(accessAddress));

        assertFalse(memoryAccessor.compareAndSwapLong(accessAddress, 0L, 2L));
        assertTrue(memoryAccessor.compareAndSwapLong(accessAddress, 1L, 2L));

        assertEquals(2L, memoryAccessor.getLong(accessAddress));

        memoryAccessor.putLong(sampleObject, SampleObject.LONG_VALUE_OFFSET, 1L);
        assertEquals(1L, memoryAccessor.getLong(sampleObject, SampleObject.LONG_VALUE_OFFSET));

        assertFalse(memoryAccessor.compareAndSwapLong(sampleObject, SampleObject.LONG_VALUE_OFFSET, 0L, 2L));
        assertTrue(memoryAccessor.compareAndSwapLong(sampleObject, SampleObject.LONG_VALUE_OFFSET, 1L, 2L));

        assertEquals(2L, memoryAccessor.getLong(sampleObject, SampleObject.LONG_VALUE_OFFSET));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_compareAndSwapObject_whenAligned() {
        do_test_compareAndSwapObject(true);
    }

    protected void do_test_compareAndSwapObject(boolean aligned) {
        long offset = SampleObject.OBJECT_VALUE_OFFSET;
        long accessOffset = aligned ? offset : offset + 1;

        String str1 = "String Object 1";
        memoryAccessor.putObject(sampleObject, accessOffset, str1);
        assertEquals(str1, memoryAccessor.getObject(sampleObject, accessOffset));

        String str2 = "String Object 2";
        assertFalse(memoryAccessor.compareAndSwapObject(sampleObject, accessOffset, null, str2));
        assertTrue(memoryAccessor.compareAndSwapObject(sampleObject, accessOffset, str1, str2));

        assertEquals(str2, memoryAccessor.getObject(sampleObject, accessOffset));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putOrderedInt_whenAligned() {
        do_test_putOrderedInt(true);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putOrderedInt_whenUnaligned() {
        do_test_putOrderedInt(false);
    }

    void do_test_putOrderedInt(boolean aligned) {
        long accessAddress = aligned ? baseAddress1 : baseAddress1 + 1;

        memoryAccessor.putOrderedInt(accessAddress, 1);
        assertEquals(1, memoryAccessor.getInt(accessAddress));

        memoryAccessor.putOrderedInt(sampleObject, SampleObject.INT_VALUE_OFFSET, 1);
        assertEquals(1, memoryAccessor.getInt(sampleObject, SampleObject.INT_VALUE_OFFSET));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putOrderedLong_whenAligned() {
        do_test_putOrderedLong(true);
    }

    @Test
    @RequiresUnalignedMemoryAccessSupport
    public void test_putOrderedLong_whenUnaligned() {
        do_test_putOrderedLong(false);
    }

    void do_test_putOrderedLong(boolean aligned) {
        long accessAddress = aligned ? baseAddress1 : baseAddress1 + 1;

        memoryAccessor.putOrderedLong(accessAddress, 1L);
        assertEquals(1L, memoryAccessor.getLong(accessAddress));

        memoryAccessor.putOrderedLong(sampleObject, SampleObject.LONG_VALUE_OFFSET, 1L);
        assertEquals(1L, memoryAccessor.getLong(sampleObject, SampleObject.LONG_VALUE_OFFSET));
    }

    ////////////////////////////////////////////////////////////////////////////////

    @Test
    public void test_putOrderedObject_whenAligned() {
        do_test_putOrderedObject(true);
    }

    protected void do_test_putOrderedObject(boolean aligned) {
        long offset = SampleObject.OBJECT_VALUE_OFFSET;
        long accessOffset = aligned ? offset : offset + 1;

        String str = "String Object";
        memoryAccessor.putOrderedObject(sampleObject, accessOffset, str);
        assertEquals(str, memoryAccessor.getObject(sampleObject, accessOffset));
    }

    ////////////////////////////////////////////////////////////////////////////////

    private static long getSampleObjectFieldOffset(String fieldName) {
        try {
            return UnsafeUtil.UNSAFE.objectFieldOffset(SampleObjectBase.class.getDeclaredField(fieldName));
        } catch (NoSuchFieldException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @SuppressWarnings("unused")
    private static class SampleObjectBase {

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

    @SuppressWarnings("unused")
    private static class SampleObject extends SampleObjectBase {

        private static final long BYTE_VALUE_OFFSET = getSampleObjectFieldOffset("byteValue");
        private static final long BOOLEAN_VALUE_OFFSET = getSampleObjectFieldOffset("booleanValue");
        private static final long CHAR_VALUE_OFFSET = getSampleObjectFieldOffset("charValue");
        private static final long SHORT_VALUE_OFFSET = getSampleObjectFieldOffset("shortValue");
        private static final long INT_VALUE_OFFSET = getSampleObjectFieldOffset("intValue");
        private static final long FLOAT_VALUE_OFFSET = getSampleObjectFieldOffset("floatValue");
        private static final long LONG_VALUE_OFFSET = getSampleObjectFieldOffset("longValue");
        private static final long DOUBLE_VALUE_OFFSET = getSampleObjectFieldOffset("doubleValue");
        private static final long OBJECT_VALUE_OFFSET = getSampleObjectFieldOffset("objectValue");

        // ensures additional allocated space at the end of the object, which may be needed to test
        // unaligned access without causing heap corruption
        private long padding;
    }
}
