package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Bits;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;
import java.util.Arrays;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * ByteArrayObjectDataOutput Tester.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ByteArrayObjectDataOutputTest {

    private InternalSerializationService mockSerializationService;
    private ByteArrayObjectDataOutput out;

    private static byte[] TEST_DATA = new byte[]{1, 2, 3};

    @Before
    public void before() throws Exception {
        mockSerializationService = mock(InternalSerializationService.class);
        out = new ByteArrayObjectDataOutput(10, mockSerializationService, ByteOrder.BIG_ENDIAN);
    }

    @After
    public void after() throws Exception {
        out.close();
    }

    @Test
    public void testWriteForPositionB() throws Exception {
        out.write(1, 5);
        assertEquals(5, out.buffer[1]);
    }

    @Test
    public void testWriteForBOffLen() throws Exception {
        byte[] zeroBytes = new byte[20];
        out.write(zeroBytes, 0, 20);
        byte[] bytes = Arrays.copyOfRange(out.buffer, 0, 20);

        assertArrayEquals(zeroBytes, bytes);
        assertEquals(20, out.pos);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testWriteForBOffLen_negativeOff() throws Exception {
        out.write(TEST_DATA, -1, 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testWriteForBOffLen_negativeLen() throws Exception {
        out.write(TEST_DATA, 0, -3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testWriteForBOffLen_OffLenHigherThenSize() throws Exception {
        out.write(TEST_DATA, 0, -3);
    }

    @Test
    public void testWriteBooleanForPositionV() throws Exception {
        out.writeBoolean(0, true);
        out.writeBoolean(1, false);

        assertEquals(1, out.buffer[0]);
        assertEquals(0, out.buffer[1]);
    }

    @Test
    public void testWriteByteForPositionV() throws Exception {
        out.writeByte(0, 10);

        assertEquals(10, out.buffer[0]);
    }

    @Test
    public void testWriteDoubleForPositionV() throws Exception {
        double v = 1.1d;
        out.writeDouble(1, v);
        long theLong = Double.doubleToLongBits(v);
        long readLongB = Bits.readLongB(out.buffer, 1);

        assertEquals(theLong, readLongB);
    }

    @Test
    public void testWriteDoubleForVByteOrder() throws Exception {
        double v = 1.1d;
        out.writeDouble(v, LITTLE_ENDIAN);
        long theLong = Double.doubleToLongBits(v);
        long readLongB = Bits.readLongL(out.buffer, 0);

        assertEquals(theLong, readLongB);
    }

    @Test
    public void testWriteDoubleForPositionVByteOrder() throws Exception {
        double v = 1.1d;
        out.writeDouble(1, v, LITTLE_ENDIAN);
        long theLong = Double.doubleToLongBits(v);
        long readLongB = Bits.readLongL(out.buffer, 1);

        assertEquals(theLong, readLongB);
    }

    @Test
    public void testWriteFloatV() throws Exception {
        float v = 1.1f;
        out.writeFloat(v);
        int expected = Float.floatToIntBits(v);
        int actual = Bits.readIntB(out.buffer, 0);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteFloatForPositionV() throws Exception {
        float v = 1.1f;
        out.writeFloat(1, v);
        int expected = Float.floatToIntBits(v);
        int actual = Bits.readIntB(out.buffer, 1);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteFloatForVByteOrder() throws Exception {
        float v = 1.1f;
        out.writeFloat(v, LITTLE_ENDIAN);
        int expected = Float.floatToIntBits(v);
        int actual = Bits.readIntL(out.buffer, 0);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteFloatForPositionVByteOrder() throws Exception {
        float v = 1.1f;
        out.writeFloat(1, v, LITTLE_ENDIAN);
        int expected = Float.floatToIntBits(v);
        int actual = Bits.readIntL(out.buffer, 1);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteIntV() throws Exception {
        int expected = 100;
        out.writeInt(expected);
        int actual = Bits.readIntB(out.buffer, 0);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteIntForPositionV() throws Exception {
        int expected = 100;
        out.writeInt(1, expected);
        int actual = Bits.readIntB(out.buffer, 1);

        assertEquals(actual, expected);

    }

    @Test
    public void testWriteIntForVByteOrder() throws Exception {
        int expected = 100;
        out.writeInt(expected, LITTLE_ENDIAN);
        int actual = Bits.readIntL(out.buffer, 0);

        assertEquals(actual, expected);

    }

    @Test
    public void testWriteIntForPositionVByteOrder() throws Exception {
        int expected = 100;
        out.writeInt(2, expected, LITTLE_ENDIAN);
        int actual = Bits.readIntL(out.buffer, 2);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteLongV() throws Exception {
        long expected = 100;
        out.writeLong(expected);
        long actual = Bits.readLongB(out.buffer, 0);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteLongForPositionV() throws Exception {
        long expected = 100;
        out.writeLong(2, expected);
        long actual = Bits.readLongB(out.buffer, 2);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteLongForVByteOrder() throws Exception {
        long expected = 100;
        out.writeLong(2, expected, LITTLE_ENDIAN);
        long actual = Bits.readLongL(out.buffer, 2);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteLongForPositionVByteOrder() throws Exception {
        long expected = 100;
        out.writeLong(2, expected, LITTLE_ENDIAN);
        long actual = Bits.readLongL(out.buffer, 2);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteShortV() throws Exception {
        short expected = 100;
        out.writeShort(expected);
        short actual = Bits.readShortB(out.buffer, 0);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteShortForPositionV() throws Exception {
        short expected = 100;
        out.writeShort(2, expected);
        short actual = Bits.readShortB(out.buffer, 2);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteShortForVByteOrder() throws Exception {
        short expected = 100;
        out.writeShort(2, expected, LITTLE_ENDIAN);
        short actual = Bits.readShortL(out.buffer, 2);

        assertEquals(actual, expected);
    }

    @Test
    public void testWriteShortForPositionVByteOrder() throws Exception {
        short expected = 100;
        out.writeShort(2, expected, LITTLE_ENDIAN);
        short actual = Bits.readShortL(out.buffer, 2);

        assertEquals(actual, expected);
    }

    @Test
    public void testEnsureAvailable() throws Exception {
        out.buffer = null;
        out.ensureAvailable(5);

        assertEquals(10, out.buffer.length);
    }

    @Test
    public void testEnsureAvailable_smallLen() throws Exception {
        out.buffer = null;
        out.ensureAvailable(1);

        assertEquals(10, out.buffer.length);
    }

    @Test
    public void testWriteObject() throws Exception {
        out.writeObject("TEST");
        verify(mockSerializationService).writeObject(out, "TEST");
    }

    @Test
    public void testPosition() throws Exception {
        out.pos = 21;
        assertEquals(21, out.position());
    }

    @Test
    public void testPositionNewPos() throws Exception {
        out.position(1);
        assertEquals(1, out.pos);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionNewPos_negativePos() throws Exception {
        out.position(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionNewPos_highPos() throws Exception {
        out.position(out.buffer.length + 1);
    }

    @Test
    public void testAvailable() throws Exception {
        int available = out.available();
        out.buffer = null;
        int availableWhenBufferNull = out.available();

        assertEquals(10, available);
        assertEquals(0, availableWhenBufferNull);
    }

    @Test
    public void testToByteArray() throws Exception {
        byte[] arrayWhenPosZero = out.toByteArray();
        out.buffer = null;
        byte[] arrayWhenBufferNull = out.toByteArray();

        assertArrayEquals(new byte[0], arrayWhenPosZero);
        assertArrayEquals(new byte[0], arrayWhenBufferNull);
    }

    @Test
    public void testClear() throws Exception {
        out.clear();
        assertEquals(0, out.position());
        assertEquals(10, out.available());
    }

    @Test
    public void testClear_bufferNull() throws Exception {
        out.buffer = null;
        out.clear();
        assertNull(out.buffer);
    }

    @Test
    public void testClear_bufferLen_lt_initX8() throws Exception {
        out.ensureAvailable(10 * 10);
        out.clear();
        assertEquals(10 * 8, out.available());
    }

    @Test
    public void testClose() throws Exception {
        out.close();
        assertEquals(0, out.position());
        assertNull(out.buffer);
    }

    @Test
    public void testGetByteOrder() throws Exception {
        ByteArrayObjectDataOutput outLE = new ByteArrayObjectDataOutput(10, mockSerializationService, LITTLE_ENDIAN);
        ByteArrayObjectDataOutput outBE = new ByteArrayObjectDataOutput(10, mockSerializationService, BIG_ENDIAN);

        assertEquals(LITTLE_ENDIAN, outLE.getByteOrder());
        assertEquals(BIG_ENDIAN, outBE.getByteOrder());
    }

    @Test
    public void testToString() throws Exception {
        assertNotNull(out.toString());
    }

} 
