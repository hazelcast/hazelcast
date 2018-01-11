/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ObjectDataInputStream.class})
@Category({QuickTest.class, ParallelTest.class})
public class ObjectDataInputStreamFinalMethodsTest {

    static final byte[] INIT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private InternalSerializationService mockSerializationService;
    private ObjectDataInputStream in;
    private DataInputStream dataInputSpy;
    private InitableByteArrayInputStream inputStream;
    private ByteOrder byteOrder;

    @Before
    public void before() throws Exception {
        byteOrder = BIG_ENDIAN;
        mockSerializationService = mock(InternalSerializationService.class);
        when(mockSerializationService.getByteOrder()).thenReturn(byteOrder);

        inputStream = new InitableByteArrayInputStream(INIT_DATA);
        in = new ObjectDataInputStream(inputStream, mockSerializationService);

        Field field = ObjectDataInputStream.class.getDeclaredField("dataInput");
        field.setAccessible(true);
        DataInputStream dataInput = (DataInputStream) field.get(in);

        dataInputSpy = spy(dataInput);

        field.set(in, dataInputSpy);
    }

    @Test
    public void testRead() throws Exception {
        in.read();
        verify(dataInputSpy).readByte();
    }

    @Test
    public void testReadB() throws Exception {
        byte[] someInput = new byte[0];
        in.read(someInput);
        verify(dataInputSpy).read(someInput);
    }

    @Test
    public void testReadForBOffLen() throws Exception {
        byte[] someInput = new byte[1];
        in.read(someInput, 0, 1);
        verify(dataInputSpy).read(someInput, 0, 1);
    }

    @Test
    public void testReadFullyB() throws Exception {
        byte[] someInput = new byte[1];
        in.readFully(someInput);
        verify(dataInputSpy).readFully(someInput);
    }

    @Test
    public void testReadFullyForBOffLen() throws Exception {
        byte[] someInput = new byte[1];
        in.readFully(someInput, 0, 1);
        verify(dataInputSpy).readFully(someInput, 0, 1);
    }

    @Test
    public void testSkipBytes() throws Exception {
        int someInput = new Random().nextInt();
        in.skipBytes(someInput);
        verify(dataInputSpy).skipBytes(someInput);
    }

    @Test
    public void testReadBoolean() throws Exception {
        in.readBoolean();
        verify(dataInputSpy).readBoolean();
    }

    @Test
    public void testReadByte() throws Exception {
        in.readByte();
        verify(dataInputSpy).readByte();
    }

    @Test
    public void testReadUnsignedByte() throws Exception {
        in.readUnsignedByte();
        verify(dataInputSpy).readUnsignedByte();
    }

    @Test
    public void testReadUnsignedShort() throws Exception {
        in.readUnsignedShort();
        verify(dataInputSpy).readUnsignedShort();
    }

    @Test
    public void testReadByteArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 1, 9, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, (byte) 1, 9, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(10);
        byte[] theNullArray = in.readByteArray();
        inputStream.position(0);
        byte[] theZeroLenghtArray = in.readByteArray();
        inputStream.position(4);
        byte[] bytes = in.readByteArray();

        assertNull(theNullArray);
        assertArrayEquals(new byte[0], theZeroLenghtArray);
        assertArrayEquals(new byte[]{1}, bytes);
    }

    @Test
    public void testReadBooleanArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 1, 9, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 9, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(10);
        boolean[] theNullArray = in.readBooleanArray();
        inputStream.position(0);
        boolean[] theZeroLenghtArray = in.readBooleanArray();
        inputStream.position(4);
        boolean[] booleanArray = in.readBooleanArray();

        assertNull(theNullArray);
        assertArrayEquals(new boolean[0], theZeroLenghtArray);
        assertTrue(Arrays.equals(new boolean[]{true}, booleanArray));
    }

    @Test
    public void testReadCharArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(10);
        char[] theNullArray = in.readCharArray();
        inputStream.position(0);
        char[] theZeroLenghtArray = in.readCharArray();
        inputStream.position(4);
        char[] booleanArray = in.readCharArray();

        assertNull(theNullArray);
        assertArrayEquals(new char[0], theZeroLenghtArray);
        assertArrayEquals(new char[]{1}, booleanArray);
    }

    @Test
    public void testReadIntArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(12);
        int[] theNullArray = in.readIntArray();
        inputStream.position(0);
        int[] theZeroLenghtArray = in.readIntArray();
        inputStream.position(4);
        int[] bytes = in.readIntArray();

        assertNull(theNullArray);
        assertArrayEquals(new int[0], theZeroLenghtArray);
        assertArrayEquals(new int[]{1}, bytes);
    }

    @Test
    public void testReadLongArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(bytesLE.length - 4);
        long[] theNullArray = in.readLongArray();
        inputStream.position(0);
        long[] theZeroLenghtArray = in.readLongArray();
        inputStream.position(4);
        long[] bytes = in.readLongArray();

        assertNull(theNullArray);
        assertArrayEquals(new long[0], theZeroLenghtArray);
        assertArrayEquals(new long[]{1}, bytes);
    }

    @Test
    public void testReadDoubleArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(bytesLE.length - 4);
        double[] theNullArray = in.readDoubleArray();
        inputStream.position(0);
        double[] theZeroLenghtArray = in.readDoubleArray();
        inputStream.position(4);
        double[] doubles = in.readDoubleArray();

        assertNull(theNullArray);
        assertArrayEquals(new double[0], theZeroLenghtArray, 0);
        assertArrayEquals(new double[]{Double.longBitsToDouble(1)}, doubles, 0);

    }

    @Test
    public void testReadFloatArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(bytesLE.length - 4);
        float[] theNullArray = in.readFloatArray();
        inputStream.position(0);
        float[] theZeroLenghtArray = in.readFloatArray();
        inputStream.position(4);
        float[] floats = in.readFloatArray();

        assertNull(theNullArray);
        assertArrayEquals(new float[0], theZeroLenghtArray, 0);
        assertArrayEquals(new float[]{Float.intBitsToFloat(1)}, floats, 0);
    }

    @Test
    public void testReadShortArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 1, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(bytesLE.length - 4);
        short[] theNullArray = in.readShortArray();
        inputStream.position(0);
        short[] theZeroLenghtArray = in.readShortArray();
        inputStream.position(4);
        short[] booleanArray = in.readShortArray();

        assertNull(theNullArray);
        assertArrayEquals(new short[0], theZeroLenghtArray);
        assertArrayEquals(new short[]{1}, booleanArray);
    }

    @Test
    public void testReadUTFArray() throws Exception {
        byte[] bytesBE = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 32, 9, -1, -1, -1, -1};
        byte[] bytesLE = {0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 32, 9, -1, -1, -1, -1};
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(bytesLE.length - 4);
        String[] theNullArray = in.readUTFArray();
        inputStream.position(0);
        String[] theZeroLenghtArray = in.readUTFArray();
        inputStream.position(4);
        String[] bytes = in.readUTFArray();

        assertNull(theNullArray);
        assertArrayEquals(new String[0], theZeroLenghtArray);
        assertArrayEquals(new String[]{" "}, bytes);
    }

    @Test
    public void testReadLine() throws Exception {
        in.readLine();
        verify(dataInputSpy).readLine();
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
        inputStream.init((byteOrder == BIG_ENDIAN ? bytesBE : bytesLE), 0);

        inputStream.position(bytesLE.length - 4);
        Data nullData = in.readData();
        inputStream.position(0);
        Data theZeroLenghtArray = in.readData();
        inputStream.position(4);
        Data data = in.readData();

        assertNull(nullData);
        assertEquals(0, theZeroLenghtArray.getType());
        assertArrayEquals(new byte[0], theZeroLenghtArray.toByteArray());
        assertArrayEquals(new byte[]{-1, -1, -1, -1, 0, 0, 0, 0}, data.toByteArray());
    }

    @Test
    public void testGetClassLoader() throws Exception {
        in.getClassLoader();
        verify(mockSerializationService).getClassLoader();
    }

    @Test
    public void testGetByteOrder() throws Exception {
        ByteOrder byteOrderActual = in.getByteOrder();
        assertEquals(byteOrder, byteOrderActual);
    }

    private class InitableByteArrayInputStream extends ByteArrayInputStream {

        public InitableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        public void init(byte[] buf, int offset) {
            this.buf = buf;
            this.count = buf != null ? buf.length : 0;
            this.pos = offset;
        }

        public final void position(int newPos) {
            if ((newPos > count) || (newPos < 0)) {
                throw new IllegalArgumentException();
            }
            pos = newPos;
            if (mark > pos) {
                mark = -1;
            }
        }

    }
}
