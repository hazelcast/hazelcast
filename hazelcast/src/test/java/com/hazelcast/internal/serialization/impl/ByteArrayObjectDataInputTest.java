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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.EOFException;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.hazelcast.internal.nio.IOUtil.readData;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * ByteArrayObjectDataInput Tester.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ByteArrayObjectDataInputTest extends HazelcastTestSupport {

    static final byte[] INIT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1};

    protected InternalSerializationService mockSerializationService;
    protected ByteArrayObjectDataInput in;
    protected ByteOrder byteOrder;

    @Before
    public void before() {
        mockSerializationService = mock(InternalSerializationService.class);
        initDataInput();
    }

    @After
    public void after() {
        in.close();
    }

    protected void initDataInput() {
        byteOrder = BIG_ENDIAN;
        in = createDataInput(byteOrder);
    }

    protected ByteArrayObjectDataInput createDataInput(ByteOrder bo) {
        return new ByteArrayObjectDataInput(INIT_DATA, mockSerializationService, bo);
    }

    @Test
    public void testInit() {
        in.init(INIT_DATA, 2);
        assertArrayEquals(INIT_DATA, in.data);
        assertEquals(INIT_DATA.length, in.size);
        assertEquals(2, in.pos);
    }

    @Test
    public void testInit_null() {
        in.init(null, 0);
        assertNull(in.data);
        assertEquals(0, in.size);
        assertEquals(0, in.pos);
    }

    @Test
    public void testClear() {
        in.clear();
        assertNull(in.data);
        assertEquals(0, in.size);
        assertEquals(0, in.pos);
        assertEquals(0, in.mark);
    }

    @Test
    public void testRead() throws Exception {
        for (int i = 0; i < in.size; i++) {
            int readValidPos = in.read();
            // Compare the unsigned byte range equivalents of
            // the initial values with the values we read, as
            // the contract of the read() says that the values
            // need to be in that range.
            assertEquals(INIT_DATA[i] & 0xFF, readValidPos);

        }
        //try to read an invalid position should return -1
        assertEquals(-1, in.read());
    }

    @Test
    public void testReadPosition() throws Exception {
        int read = in.read(1);
        int readUnsigned = in.read(INIT_DATA.length - 1);
        int readEnd = in.read(INIT_DATA.length);
        assertEquals(1, read);
        // Map the expected negative value to unsigned byte range
        assertEquals(-1 & 0xFF, readUnsigned);
        assertEquals(-1, readEnd);
    }

    @Test
    public void testReadForBOffLen() throws Exception {
        int read = in.read(INIT_DATA, 0, 5);
        assertEquals(5, read);
    }

    @Test(expected = NullPointerException.class)
    public void testReadForBOffLen_null_array() throws Exception {
        in.read(null, 0, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadForBOffLen_negativeLen() throws Exception {
        in.read(INIT_DATA, 0, -11);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadForBOffLen_negativeOffset() throws Exception {
        in.read(INIT_DATA, -10, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadForBOffLen_Len_LT_Bytes() throws Exception {
        in.read(INIT_DATA, 0, INIT_DATA.length + 1);
    }

    @Test
    public void testReadForBOffLen_pos_gt_size() throws Exception {
        in.pos = 100;
        int read = in.read(INIT_DATA, 0, 1);
        assertEquals(-1, read);
    }

    @Test
    public void testReadBoolean() throws Exception {
        boolean read1 = in.readBoolean();
        boolean read2 = in.readBoolean();
        assertFalse(read1);
        assertTrue(read2);
    }

    @Test
    public void testReadBooleanPosition() throws Exception {
        boolean read1 = in.readBoolean(0);
        boolean read2 = in.readBoolean(1);
        assertFalse(read1);
        assertTrue(read2);
    }

    @Test(expected = EOFException.class)
    public void testReadBoolean_EOF() throws Exception {
        in.pos = INIT_DATA.length + 1;
        in.readBoolean();
    }

    @Test(expected = EOFException.class)
    public void testReadBooleanPosition_EOF() throws Exception {
        in.readBoolean(INIT_DATA.length + 1);
    }

    @Test
    public void testReadByte() throws Exception {
        int read = in.readByte();
        assertEquals(0, read);
    }

    @Test
    public void testReadBytePosition() throws Exception {
        int read = in.readByte(1);
        assertEquals(1, read);
    }

    @Test(expected = EOFException.class)
    public void testReadByte_EOF() throws Exception {
        in.pos = INIT_DATA.length + 1;
        in.readByte();
    }

    @Test(expected = EOFException.class)
    public void testReadBytePosition_EOF() throws Exception {
        in.readByte(INIT_DATA.length + 1);
    }

    @Test
    public void testReadChar() throws Exception {
        char c = in.readChar();
        char expected = Bits.readChar(INIT_DATA, 0, byteOrder == BIG_ENDIAN);
        assertEquals(expected, c);
    }

    @Test
    public void testReadCharPosition() throws Exception {
        char c = in.readChar(0);
        char expected = Bits.readChar(INIT_DATA, 0, byteOrder == BIG_ENDIAN);
        assertEquals(expected, c);
    }

    @Test
    public void testReadDouble() throws Exception {
        double readDouble = in.readDouble();
        long longB = Bits.readLong(INIT_DATA, 0, byteOrder == BIG_ENDIAN);
        double aDouble = Double.longBitsToDouble(longB);
        assertEquals(aDouble, readDouble, 0);
    }

    @Test
    public void testReadDoublePosition() throws Exception {
        double readDouble = in.readDouble(2);
        long longB = Bits.readLong(INIT_DATA, 2, byteOrder == BIG_ENDIAN);
        double aDouble = Double.longBitsToDouble(longB);
        assertEquals(aDouble, readDouble, 0);
    }

    @Test
    public void testReadDoubleByteOrder() throws Exception {
        double readDouble = in.readDouble(LITTLE_ENDIAN);
        long longB = Bits.readLong(INIT_DATA, 0, false);
        double aDouble = Double.longBitsToDouble(longB);
        assertEquals(aDouble, readDouble, 0);
    }

    @Test
    public void testReadDoubleForPositionByteOrder() throws Exception {
        double readDouble = in.readDouble(2, LITTLE_ENDIAN);
        long longB = Bits.readLong(INIT_DATA, 2, false);
        double aDouble = Double.longBitsToDouble(longB);
        assertEquals(aDouble, readDouble, 0);
    }

    @Test
    public void testReadFloat() throws Exception {
        double readFloat = in.readFloat();
        int intB = Bits.readInt(INIT_DATA, 0, byteOrder == BIG_ENDIAN);
        double aFloat = Float.intBitsToFloat(intB);
        assertEquals(aFloat, readFloat, 0);
    }

    @Test
    public void testReadFloatPosition() throws Exception {
        double readFloat = in.readFloat(2);
        int intB = Bits.readInt(INIT_DATA, 2, byteOrder == BIG_ENDIAN);
        double aFloat = Float.intBitsToFloat(intB);
        assertEquals(aFloat, readFloat, 0);
    }

    @Test
    public void testReadFloatByteOrder() throws Exception {
        double readFloat = in.readFloat(LITTLE_ENDIAN);
        int intB = Bits.readIntL(INIT_DATA, 0);
        double aFloat = Float.intBitsToFloat(intB);
        assertEquals(aFloat, readFloat, 0);
    }

    @Test
    public void testReadFloatForPositionByteOrder() throws Exception {
        double readFloat = in.readFloat(2, LITTLE_ENDIAN);
        int intB = Bits.readIntL(INIT_DATA, 2);
        double aFloat = Float.intBitsToFloat(intB);
        assertEquals(aFloat, readFloat, 0);
    }

    @Test
    public void testReadFullyB() throws Exception {
        byte[] readFull = new byte[INIT_DATA.length];
        in.readFully(readFull);
        assertArrayEquals(readFull, in.data);
    }

    @Test(expected = EOFException.class)
    public void testReadFullyB_EOF() throws Exception {
        in.position(INIT_DATA.length);
        byte[] readFull = new byte[INIT_DATA.length];
        in.readFully(readFull);
    }

    @Test
    public void testReadFullyForBOffLen() throws Exception {
        byte[] readFull = new byte[10];
        in.readFully(readFull, 0, 5);
        for (int i = 0; i < 5; i++) {
            assertEquals(readFull[i], in.data[i]);
        }
    }

    @Test(expected = EOFException.class)
    public void testReadFullyForBOffLen_EOF() throws Exception {
        in.position(INIT_DATA.length);
        byte[] readFull = new byte[INIT_DATA.length];
        in.readFully(readFull, 0, readFull.length);
    }

    @Test
    public void testReadInt() throws Exception {
        int readInt = in.readInt();
        int theInt = Bits.readInt(INIT_DATA, 0, byteOrder == BIG_ENDIAN);
        assertEquals(theInt, readInt);
    }

    @Test
    public void testReadIntPosition() throws Exception {
        int readInt = in.readInt(2);
        int theInt = Bits.readInt(INIT_DATA, 2, byteOrder == BIG_ENDIAN);
        assertEquals(theInt, readInt);
    }

    @Test
    public void testReadIntByteOrder() throws Exception {
        int readInt = in.readInt(LITTLE_ENDIAN);
        int theInt = Bits.readIntL(INIT_DATA, 0);
        assertEquals(theInt, readInt);
    }

    @Test
    public void testReadIntForPositionByteOrder() throws Exception {
        int readInt1 = in.readInt(1, BIG_ENDIAN);
        int readInt2 = in.readInt(5, LITTLE_ENDIAN);
        int theInt1 = Bits.readInt(INIT_DATA, 1, true);
        int theInt2 = Bits.readInt(INIT_DATA, 5, false);
        assertEquals(theInt1, readInt1);
        assertEquals(theInt2, readInt2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadLine() throws Exception {
        in.readLine();
    }

    @Test
    public void testReadLong() throws Exception {
        long readLong = in.readLong();
        long expected = Bits.readLong(INIT_DATA, 0, byteOrder == BIG_ENDIAN);
        assertEquals(expected, readLong);
    }

    @Test
    public void testReadLongPosition() throws Exception {
        long readLong = in.readLong(2);
        long longB = Bits.readLong(INIT_DATA, 2, byteOrder == BIG_ENDIAN);
        assertEquals(longB, readLong);
    }

    @Test
    public void testReadLongByteOrder() throws Exception {
        long readLong = in.readLong(LITTLE_ENDIAN);
        long longB = Bits.readLongL(INIT_DATA, 0);
        assertEquals(longB, readLong);
    }

    @Test
    public void testReadLongForPositionByteOrder() throws Exception {
        long readLong1 = in.readLong(0, LITTLE_ENDIAN);
        long readLong2 = in.readLong(2, BIG_ENDIAN);
        long longB1 = Bits.readLong(INIT_DATA, 0, false);
        long longB2 = Bits.readLong(INIT_DATA, 2, true);
        assertEquals(longB1, readLong1);
        assertEquals(longB2, readLong2);
    }

    @Test
    public void testReadShort() throws Exception {
        short read = in.readShort();
        short val = Bits.readShort(INIT_DATA, 0, byteOrder == BIG_ENDIAN);
        assertEquals(val, read);
    }

    @Test
    public void testReadShortPosition() throws Exception {
        short read = in.readShort(1);
        short val = Bits.readShort(INIT_DATA, 1, byteOrder == BIG_ENDIAN);
        assertEquals(val, read);
    }

    @Test
    public void testReadShortByteOrder() throws Exception {
        short read = in.readShort(LITTLE_ENDIAN);
        short val = Bits.readShortL(INIT_DATA, 0);
        assertEquals(val, read);
    }

    @Test
    public void testReadShortForPositionByteOrder() throws Exception {
        short read1 = in.readShort(1, LITTLE_ENDIAN);
        short read2 = in.readShort(3, BIG_ENDIAN);
        short val1 = Bits.readShort(INIT_DATA, 1, false);
        short val2 = Bits.readShort(INIT_DATA, 3, true);
        assertEquals(val1, read1);
        assertEquals(val2, read2);
    }

    @Test
    public void testReadByteArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 1, 9, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, (byte) 1, 9, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(10);
        byte[] theNullArray = in.readByteArray();
        in.position(0);
        byte[] theZeroLenghtArray = in.readByteArray();
        in.position(4);
        byte[] bytes = in.readByteArray();

        assertNull(theNullArray);
        assertArrayEquals(new byte[0], theZeroLenghtArray);
        assertArrayEquals(new byte[]{1}, bytes);
    }

    @Test
    public void testReadBooleanArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 1, 9, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 9, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(10);
        boolean[] theNullArray = in.readBooleanArray();
        in.position(0);
        boolean[] theZeroLenghtArray = in.readBooleanArray();
        in.position(4);
        boolean[] booleanArray = in.readBooleanArray();

        assertNull(theNullArray);
        assertArrayEquals(new boolean[0], theZeroLenghtArray);
        assertTrue(Arrays.equals(new boolean[]{true}, booleanArray));
    }

    @Test
    public void testReadCharArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(10);
        char[] theNullArray = in.readCharArray();
        in.position(0);
        char[] theZeroLenghtArray = in.readCharArray();
        in.position(4);
        char[] booleanArray = in.readCharArray();

        assertNull(theNullArray);
        assertArrayEquals(new char[0], theZeroLenghtArray);
        assertArrayEquals(new char[]{1}, booleanArray);
    }

    @Test
    public void testReadIntArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(12);
        int[] theNullArray = in.readIntArray();
        in.position(0);
        int[] theZeroLenghtArray = in.readIntArray();
        in.position(4);
        int[] bytes = in.readIntArray();

        assertNull(theNullArray);
        assertArrayEquals(new int[0], theZeroLenghtArray);
        assertArrayEquals(new int[]{1}, bytes);
    }

    @Test
    public void testReadLongArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(bytesLE.length - 4);
        long[] theNullArray = in.readLongArray();
        in.position(0);
        long[] theZeroLenghtArray = in.readLongArray();
        in.position(4);
        long[] bytes = in.readLongArray();

        assertNull(theNullArray);
        assertArrayEquals(new long[0], theZeroLenghtArray);
        assertArrayEquals(new long[]{1}, bytes);
    }

    @Test
    public void testReadDoubleArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(bytesLE.length - 4);
        double[] theNullArray = in.readDoubleArray();
        in.position(0);
        double[] theZeroLenghtArray = in.readDoubleArray();
        in.position(4);
        double[] doubles = in.readDoubleArray();

        assertNull(theNullArray);
        assertArrayEquals(new double[0], theZeroLenghtArray, 0);
        assertArrayEquals(new double[]{Double.longBitsToDouble(1)}, doubles, 0);

    }

    @Test
    public void testReadFloatArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(bytesLE.length - 4);
        float[] theNullArray = in.readFloatArray();
        in.position(0);
        float[] theZeroLenghtArray = in.readFloatArray();
        in.position(4);
        float[] floats = in.readFloatArray();

        assertNull(theNullArray);
        assertArrayEquals(new float[0], theZeroLenghtArray, 0);
        assertArrayEquals(new float[]{Float.intBitsToFloat(1)}, floats, 0);
    }

    @Test
    public void testReadShortArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(bytesLE.length - 4);
        short[] theNullArray = in.readShortArray();
        in.position(0);
        short[] theZeroLenghtArray = in.readShortArray();
        in.position(4);
        short[] booleanArray = in.readShortArray();

        assertNull(theNullArray);
        assertArrayEquals(new short[0], theZeroLenghtArray);
        assertArrayEquals(new short[]{1}, booleanArray);
    }

    @Test
    public void testReadUTFArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 32, 9, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 32, 9, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(bytesLE.length - 4);
        String[] theNullArray = in.readStringArray();
        in.position(0);
        String[] theZeroLenghtArray = in.readStringArray();
        in.position(4);
        String[] bytes = in.readStringArray();

        assertNull(theNullArray);
        assertArrayEquals(new String[0], theZeroLenghtArray);
        assertArrayEquals(new String[]{" "}, bytes);
    }

    @Test
    public void testReadUnsignedByte() throws Exception {
        byte[] bytesBE = {-1, -1, -1, -1};
        byte[] bytesLE = {-1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        int unsigned = in.readUnsignedByte();
        int expexted = 0xff;
        assertEquals(expexted, unsigned);
    }

    @Test
    public void testReadUnsignedShort() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1, bytes1.length - 4);
        int unsigned = in.readUnsignedShort();
        assertEquals(0xFFFF, unsigned);
    }

    public void testReadUTF() {
        //EXTENDED TEST ELSEWHERE: StringSerializationTest
    }

    @Test
    public void testReadObject() throws Exception {
        in.readObject();
        verify(mockSerializationService).readObject(in);
    }

    @Test
    public void testReadData() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 8, -1, -1, -1, -1, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 8, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        in.position(bytesLE.length - 4);
        Data nullData = readData(in);
        in.position(0);
        Data theZeroLenghtArray = readData(in);
        in.position(4);
        Data data = readData(in);

        assertNull(nullData);
        assertEquals(0, theZeroLenghtArray.getType());
        assertArrayEquals(new byte[0], theZeroLenghtArray.toByteArray());
        assertArrayEquals(new byte[]{-1, -1, -1, -1, 0, 0, 0, 0}, data.toByteArray());
    }

    @Test
    public void testSkip() {
        long s1 = in.skip(-1);
        long s2 = in.skip(Integer.MAX_VALUE);
        long s3 = in.skip(1);

        assertEquals(0, s1);
        assertEquals(0, s2);
        assertEquals(1, s3);
    }

    @Test
    public void testSkipBytes() {
        int s1 = in.skipBytes(-1);
        int s2 = in.skipBytes(1);

        in.position(0);
        int maxSkipBytes = in.available();
        int s3 = in.skipBytes(INIT_DATA.length);

        assertEquals(0, s1);
        assertEquals(1, s2);
        //skipBytes skips at most available bytes
        assertEquals(maxSkipBytes, s3);
    }

    @Test
    public void testPosition() {
        assertEquals(0, in.position());
    }

    @Test
    public void testPositionNewPos() {
        in.position(INIT_DATA.length - 1);
        assertEquals(INIT_DATA.length - 1, in.position());
    }

    @Test
    public void testPositionNewPos_mark() {
        in.position(INIT_DATA.length - 1);
        in.mark(0);
        int firstMarked = in.mark;
        in.position(1);
        assertEquals(INIT_DATA.length - 1, firstMarked);
        assertEquals(1, in.position());
        assertEquals(-1, in.mark);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionNewPos_HighNewPos() {
        in.position(INIT_DATA.length + 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionNewPos_negativeNewPos() {
        in.position(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckAvailable() throws Exception {
        in.checkAvailable(-1, INIT_DATA.length);
    }

    @Test(expected = EOFException.class)
    public void testCheckAvailable_EOF() throws Exception {
        in.checkAvailable(0, INIT_DATA.length + 1);
    }

    @Test
    public void testAvailable() {
        assertEquals(in.size - in.pos, in.available());
    }

    @Test
    public void testMarkSupported() {
        assertTrue(in.markSupported());
    }

    @Test
    public void testMark() {
        in.position(1);
        in.mark(1);
        assertEquals(1, in.mark);
    }

    @Test
    public void testReset() {
        in.position(1);
        in.mark(1);
        in.reset();
        assertEquals(1, in.pos);
    }

    @Test
    public void testClose() {
        in.close();
        assertNull(in.data);
        assertNull(in.charBuffer);
    }

    @Test
    public void testGetClassLoader() {
        in.getClassLoader();
        verify(mockSerializationService).getClassLoader();

    }

    @Test
    public void testGetByteOrder() {
        ByteArrayObjectDataInput input = createDataInput(LITTLE_ENDIAN);

        assertEquals(LITTLE_ENDIAN, input.getByteOrder());
        assertEquals(byteOrder, in.getByteOrder());
    }

    @Test
    public void testToString() {
        assertNotNull(in.toString());
    }
}
