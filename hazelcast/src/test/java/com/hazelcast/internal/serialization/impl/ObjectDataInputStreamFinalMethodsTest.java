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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.nio.ByteOrder;

import static com.hazelcast.internal.nio.IOUtil.readData;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObjectDataInputStreamFinalMethodsTest {

    private InternalSerializationService mockSerializationService;
    private ObjectDataInputStream in;
    private InitableByteArrayInputStream inputStream;
    private ByteOrder byteOrder;
    private final byte[] initData = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    @Before
    public void before() throws Exception {
        byteOrder = BIG_ENDIAN;
        mockSerializationService = mock(InternalSerializationService.class);
        when(mockSerializationService.getByteOrder()).thenReturn(byteOrder);

        inputStream = new InitableByteArrayInputStream(initData);
        in = new ObjectDataInputStream(inputStream, mockSerializationService);
    }

    @Test
    public void testRead() throws Exception {
        assertThat(in.read()).isZero();
        assertThat(in.read()).isOne();
    }

    @Test
    public void testReadB() throws Exception {
        byte[] someInput = new byte[3];
        int readBytes = in.read(someInput);
        assertThat(readBytes).isEqualTo(3);
        assertArrayEquals(new byte[]{0, 1, 2}, someInput);

    }

    @Test
    public void testReadForBOffLen() throws Exception {
        byte[] someInput = new byte[5];
        int readBytes = in.read(someInput, 2, 3);
        assertThat(readBytes).isEqualTo(3);
        assertArrayEquals(new byte[]{0, 0, 0, 1, 2}, someInput);
    }

    @Test
    public void testReadFullyB() throws Exception {
        byte[] someInput = new byte[5];
        in.readFully(someInput);
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, someInput);

    }

    @Test
    public void testReadFullyForBOffLen() throws Exception {
        byte[] someInput = new byte[5];
        in.readFully(someInput, 2, 2);
        assertArrayEquals(new byte[]{0, 0, 0, 1, 0}, someInput);
    }

    @Test
    public void testSkipBytes() throws Exception {
        assertThat(in.read()).isZero();
        in.skipBytes(3);
        assertThat(in.read()).isEqualTo(4);
    }

    @Test
    public void testReadBoolean() throws Exception {
        assertThat(in.readBoolean()).isFalse(); //0
        assertThat(in.readBoolean()).isTrue(); //1
        assertThat(in.readBoolean()).isTrue(); //2
    }

    @Test
    public void testReadByte() throws Exception {
        assertThat(in.readByte()).isEqualTo((byte) 0);
        assertThat(in.readByte()).isEqualTo((byte) 1);
        assertThat(in.readByte()).isEqualTo((byte) 2);
    }

    @Test
    public void testReadUnsignedByte() throws Exception {
        initData[1] = -56;
        assertThat(in.readUnsignedByte()).isZero();
        assertThat(in.readUnsignedByte()).isEqualTo(200);
        assertThat(in.readUnsignedByte()).isEqualTo(2);
    }

    @Test
    public void testReadUnsignedShort() throws Exception {
        initData[0] = 0;
        initData[1] = 1;
        initData[2] = 1;
        initData[3] = 2;
        assertThat(in.readUnsignedShort()).isEqualTo(1);
        assertThat(in.readUnsignedShort()).isEqualTo(258);
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
        assertArrayEquals(new boolean[]{true}, booleanArray);
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
        String[] theNullArray = in.readStringArray();
        inputStream.position(0);
        String[] theZeroLenghtArray = in.readStringArray();
        inputStream.position(4);
        String[] bytes = in.readStringArray();

        assertNull(theNullArray);
        assertArrayEquals(new String[0], theZeroLenghtArray);
        assertArrayEquals(new String[]{" "}, bytes);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadLine() {
        in.readLine();
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
        Data nullData = readData(in);
        inputStream.position(0);
        Data theZeroLenghtArray = readData(in);
        inputStream.position(4);
        Data data = readData(in);

        assertNull(nullData);
        assertEquals(0, theZeroLenghtArray.getType());
        assertArrayEquals(new byte[0], theZeroLenghtArray.toByteArray());
        assertArrayEquals(new byte[]{-1, -1, -1, -1, 0, 0, 0, 0}, data.toByteArray());
    }

    @Test
    public void testGetClassLoader() {
        in.getClassLoader();
        verify(mockSerializationService).getClassLoader();
    }

    @Test
    public void testGetByteOrder() {
        ByteOrder byteOrderActual = in.getByteOrder();
        assertEquals(byteOrder, byteOrderActual);
    }

    private static class InitableByteArrayInputStream extends ByteArrayInputStream {

        InitableByteArrayInputStream(byte[] buf) {
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
