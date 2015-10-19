package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.EOFException;
import java.nio.ByteOrder;

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
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ByteArrayObjectDataInputTest {

    private SerializationService mockSerializationService;

    private ByteArrayObjectDataInput in;

    private final static byte[] INIT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    @Before
    public void before() throws Exception {
        mockSerializationService = mock(SerializationService.class);
        in = new ByteArrayObjectDataInput(INIT_DATA, mockSerializationService, ByteOrder.BIG_ENDIAN);
    }

    @After
    public void after() throws Exception {
        in.close();
    }

    @Test
    public void testInit() throws Exception {
        in.init(INIT_DATA, 2);
        assertArrayEquals(INIT_DATA, in.data);
        assertEquals(INIT_DATA.length, in.size);
        assertEquals(2, in.pos);
    }

    @Test
    public void testClear() throws Exception {
        in.clear();
        assertNull(in.data);
        assertEquals(0, in.size);
        assertEquals(0, in.pos);
        assertEquals(0, in.mark);
    }

    @Test
    public void testRead() throws Exception {
        int read = in.read();
        assertEquals(0, read);
    }

    @Test
    public void testReadPosition() throws Exception {
        int read = in.read(1);
        assertEquals(1, read);
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
        assertEquals(1, c);
    }

    @Test
    public void testReadCharPosition() throws Exception {
        char c = in.readChar(0);
        assertEquals(1, c);
    }

    @Test
    public void testReadDouble() throws Exception {
        double readDouble = in.readDouble();
        long longB = Bits.readLongB(INIT_DATA, 0);
        double aDouble = Double.longBitsToDouble(longB);
        assertEquals(aDouble, readDouble, 0);
    }

    @Test
    public void testReadDoublePosition() throws Exception {
        double readDouble = in.readDouble(2);
        long longB = Bits.readLongB(INIT_DATA, 2);
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
        int intB = Bits.readIntB(INIT_DATA, 0);
        double aFloat = Float.intBitsToFloat(intB);
        assertEquals(aFloat, readFloat, 0);
    }

    @Test
    public void testReadFloatPosition() throws Exception {
        double readFloat = in.readFloat(2);
        int intB = Bits.readIntB(INIT_DATA, 2);
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
        int theInt = Bits.readIntB(INIT_DATA, 0);
        assertEquals(theInt, readInt);
    }

    @Test
    public void testReadIntPosition() throws Exception {
        int readInt = in.readInt(2);
        int theInt = Bits.readIntB(INIT_DATA, 2);
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
        int readInt = in.readInt(3, LITTLE_ENDIAN);
        int theInt = Bits.readIntL(INIT_DATA, 3);
        assertEquals(theInt, readInt);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadLine() throws Exception {
        in.readLine();
    }

    @Test
    public void testReadLong() throws Exception {
        long readLong = in.readLong();
        long longB = Bits.readLongB(INIT_DATA, 0);
        assertEquals(longB, readLong);
    }

    @Test
    public void testReadLongPosition() throws Exception {
        long readLong = in.readLong(2);
        long longB = Bits.readLongB(INIT_DATA, 2);
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
        long readLong = in.readLong(2, LITTLE_ENDIAN);
        long longB = Bits.readLongL(INIT_DATA, 2);
        assertEquals(longB, readLong);
    }

    @Test
    public void testReadShort() throws Exception {
        short read = in.readShort();
        short val = Bits.readShortB(INIT_DATA, 0);
        assertEquals(val, read);
    }

    @Test
    public void testReadShortPosition() throws Exception {
        short read = in.readShort(1);
        short val = Bits.readShortB(INIT_DATA, 1);
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
        short read = in.readShort(1, LITTLE_ENDIAN);
        short val = Bits.readShortL(INIT_DATA, 1);
        assertEquals(val, read);
    }

    @Test
    public void testReadByteArray() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 8, 9, -1, -1, -1, -1};
        in.init(bytes1,0);

        in.position(10);
        byte[] theNullArray = in.readByteArray();
        in.position(0);
        byte[] theZeroLenghtArray = in.readByteArray();
        in.position(4);
        byte[] bytes = in.readByteArray();

        assertNull(theNullArray);
        assertArrayEquals(new byte[0], theZeroLenghtArray);
        assertArrayEquals(new byte[]{8}, bytes);
    }

    @Test
    public void testReadBooleanArray() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 8, 9, -1, -1, -1, -1};
        in.init(bytes1,0);

        in.position(10);
        boolean[] theNullArray = in.readBooleanArray();
        in.position(0);
        boolean[] theZeroLenghtArray = in.readBooleanArray();
        in.position(4);
        boolean[] booleanArray = in.readBooleanArray();

        assertNull(theNullArray);
        assertArrayEquals(new boolean[0], theZeroLenghtArray);
        assertArrayEquals(new boolean[]{true}, booleanArray);
    }

    @Test
    public void testReadCharArray() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 1,  -1, -1, -1, -1};
        in.init(bytes1,0);

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
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1,  -1, -1, -1, -1};
        in.init(bytes1,0);

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
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1,0);

        in.position(bytes1.length-4);
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
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1,0);
        double[] theZeroLenghtArray = in.readDoubleArray();
        assertArrayEquals(new double[0], theZeroLenghtArray, 0);
    }

    @Test
    public void testReadFloatArray() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1,0);
        float[] theZeroLenghtArray = in.readFloatArray();
        assertArrayEquals(new float[0], theZeroLenghtArray, 0);
    }

    @Test
    public void testReadShortArray() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 1,  -1, -1, -1, -1};
        in.init(bytes1,0);

        in.position(10);
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
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1,0);
        String[] theZeroLenghtArray = in.readUTFArray();
        assertArrayEquals(new String[0], theZeroLenghtArray);
    }

    @Test
    public void testReadUnsignedByte() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1,bytes1.length-4);
        int unsigned = in.readUnsignedByte();
        assertEquals(0xFF, unsigned);
    }

    @Test
    public void testReadUnsignedShort() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1,bytes1.length-4);
        int unsigned = in.readUnsignedShort();
        assertEquals(0xFFFF, unsigned);
    }

    @Test
    public void testReadUTF() throws Exception {
        //EXTENDED TEST ELSEWHERE: StringSerializationTest
    }

    @Test
    public void testReadObject() throws Exception {
        in.readObject();
        verify(mockSerializationService).readObject(in);
    }

    @Test
    public void testReadData() throws Exception {
        byte[] bytes1 = {0, 0, 0, 0, 0, 0, 0, 8,  -1, -1, -1, -1, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        in.init(bytes1,0);

        in.position(bytes1.length-4);
        Data nullData = in.readData();
        in.position(0);
        Data theZeroLenghtArray = in.readData();
        in.position(4);
        Data data = in.readData();

        assertNull(nullData);
        assertEquals(0, theZeroLenghtArray.getType());
        assertArrayEquals(new byte[0], theZeroLenghtArray.toByteArray());
        assertArrayEquals(new byte[]{ -1, -1, -1, -1, 0, 0, 0, 0}, data.toByteArray());
    }

    @Test
    public void testSkip() throws Exception {
        long s1 = in.skip(-1);
        long s2 = in.skip(Integer.MAX_VALUE);
        long s3 = in.skip(1);

        assertEquals(0, s1);
        assertEquals(0, s2);
        assertEquals(1, s3);
    }

    @Test
    public void testSkipBytes() throws Exception {
        long s1 = in.skipBytes(-1);
        long s3 = in.skipBytes(1);

        assertEquals(0, s1);
        assertEquals(1, s3);
    }

    @Test
    public void testPosition() throws Exception {
        assertEquals(0, in.position());
    }

    @Test
    public void testPositionNewPos() throws Exception {
        in.position(INIT_DATA.length -1);
        assertEquals(INIT_DATA.length -1, in.position());
    }

    @Test
    public void testPositionNewPos_mark() throws Exception {
        in.position(INIT_DATA.length -1);
        in.mark(0);
        int firstMarked = in.mark;
        in.position(1);
        assertEquals(INIT_DATA.length -1, firstMarked);
        assertEquals(1, in.position());
        assertEquals(-1, in.mark);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionNewPos_HighNewPos() throws Exception {
        in.position(INIT_DATA.length + 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionNewPos_negativeNewPos() throws Exception {
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
    public void testAvailable() throws Exception {
        assertEquals(in.size - in.pos, in.available());
    }

    @Test
    public void testMarkSupported() throws Exception {
        assertTrue(in.markSupported());
    }

    @Test
    public void testMark() throws Exception {
        in.position(1);
        in.mark(1);
        assertEquals(1, in.mark);
    }

    @Test
    public void testReset() throws Exception {
        in.position(1);
        in.mark(1);
        in.reset();
        assertEquals(1, in.pos);
    }

    @Test
    public void testClose() throws Exception {
        in.close();
        assertNull(in.data);
        assertNull(in.charBuffer);
    }

    @Test
    public void testGetClassLoader() throws Exception {
        in.getClassLoader();
        verify(mockSerializationService).getClassLoader();

    }

    @Test
    public void testGetByteOrder() throws Exception {
        ByteArrayObjectDataInput input = new ByteArrayObjectDataInput(INIT_DATA, mockSerializationService,
                ByteOrder.LITTLE_ENDIAN);
        assertEquals(BIG_ENDIAN, in.getByteOrder());
        assertEquals(LITTLE_ENDIAN, input.getByteOrder());
    }

    @Test
    public void testToString() throws Exception {
        assertNotNull(in.toString());
    }

} 
