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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collection;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class AbstractDataStreamIntegrationTest<O extends ObjectDataOutput, I extends ObjectDataInput> {

    @Parameter
    public ByteOrder byteOrder;

    protected O out;
    protected I input;
    protected InternalSerializationService serializationService;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{BIG_ENDIAN}, {LITTLE_ENDIAN}});
    }

    @Before
    public void setup() {
        assumptions();
        serializationService = new DefaultSerializationServiceBuilder().setByteOrder(byteOrder).build();
        out = getDataOutput(serializationService);
    }

    @Test
    public void testByte() throws IOException {
        out.write(Byte.MAX_VALUE);
        out.write(Byte.MIN_VALUE);
        out.write(0);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(Byte.MAX_VALUE, in.readByte());
        assertEquals(Byte.MIN_VALUE, in.readByte());
        assertEquals(0, in.readByte());
    }

    @Test
    public void testByteArray() throws IOException {
        byte[] arr = new byte[] {Byte.MIN_VALUE, Byte.MAX_VALUE, 0};
        out.writeByteArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readByteArray());
    }

    @Test
    public void testUnsignedByte() throws IOException {
        out.write(0xff);
        out.write(-1);
        out.write(Byte.MAX_VALUE + 1);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(0xff, in.readUnsignedByte());
        assertEquals(0xff, in.readUnsignedByte());
        assertEquals(Byte.MAX_VALUE + 1, in.readUnsignedByte());
    }

    @Test
    public void testShort() throws IOException {
        // negative shorts
        out.writeShort(Short.MIN_VALUE);
        out.writeShort(-250);
        // positive shorts
        out.writeShort(132);
        out.writeShort(Short.MAX_VALUE);

        ObjectDataInput input = getDataInputFromOutput();

        assertEquals(Short.MIN_VALUE, input.readShort());
        assertEquals(-250, input.readShort());
        assertEquals(132, input.readShort());
        assertEquals(Short.MAX_VALUE, input.readShort());
    }

    @Test
    public void testShortArray() throws IOException {
        short[] arr = new short[] {Short.MIN_VALUE, -250, 132, Short.MAX_VALUE};
        out.writeShortArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readShortArray());
    }

    @Test
    public void testUnsignedShort() throws IOException {
        // unsigned short, out of range of signed short
        out.writeShort(Short.MAX_VALUE + 200);
        out.writeShort(Short.MAX_VALUE + 1);
        out.writeShort(0xFFFF);

        ObjectDataInput input = getDataInputFromOutput();

        assertEquals(Short.MAX_VALUE + 200, input.readUnsignedShort());
        assertEquals(Short.MAX_VALUE + 1, input.readUnsignedShort());
        assertEquals(0xFFFF, input.readUnsignedShort());
    }

    @Test
    public void testInt() throws IOException {
        out.writeInt(Integer.MAX_VALUE);
        out.writeInt(Integer.MIN_VALUE);
        out.writeInt(-1);
        out.writeInt(132);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(Integer.MAX_VALUE, in.readInt());
        assertEquals(Integer.MIN_VALUE, in.readInt());
        assertEquals(-1, in.readInt());
        assertEquals(132, in.readInt());
    }

    @Test
    public void testIntArray() throws IOException {
        int[] arr = new int[] {Integer.MIN_VALUE, -250, 132, Integer.MAX_VALUE};
        out.writeIntArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readIntArray());
    }

    @Test
    public void testBoolean() throws IOException {
        out.writeBoolean(false);
        out.writeBoolean(true);

        ObjectDataInput in = getDataInputFromOutput();
        assertFalse(in.readBoolean());
        assertTrue(in.readBoolean());
    }

    @Test
    public void testBooleanArray() throws IOException {
        boolean[] arr = new boolean[] {true, false};
        out.writeBooleanArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readBooleanArray());
    }

    @Test
    public void testChar() throws IOException {
        out.writeChar(Character.MIN_VALUE);
        out.writeChar(Character.MAX_VALUE);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(Character.MIN_VALUE, in.readChar());
        assertEquals(Character.MAX_VALUE, in.readChar());
    }

    @Test
    public void testCharArray() throws IOException {
        char[] arr = new char[] {Character.MIN_VALUE, Character.MAX_VALUE};
        out.writeCharArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readCharArray());
    }

    @Test
    public void testDouble() throws IOException {
        out.writeDouble(Double.MIN_VALUE);
        out.writeDouble(-1);
        out.writeDouble(Math.PI);
        out.writeDouble(Double.MAX_VALUE);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(Double.MIN_VALUE, in.readDouble(), 1e-10D);
        assertEquals(-1, in.readDouble(), 1e-10D);
        assertEquals(Math.PI, in.readDouble(), 1e-10D);
        assertEquals(Double.MAX_VALUE, in.readDouble(), 1e-10D);
    }

    @Test
    public void testDoubleArray() throws IOException {
        double[] arr = new double[] {Double.MIN_VALUE, -1, Math.PI, Double.MAX_VALUE};
        out.writeDoubleArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readDoubleArray(), 1e-10D);
    }

    @Test
    public void testFloat() throws IOException {
        out.writeFloat(Float.MIN_VALUE);
        out.writeFloat(-1);
        out.writeFloat(Float.MAX_VALUE);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(Float.MIN_VALUE, in.readFloat(), 1e-10F);
        assertEquals(-1, in.readFloat(), 1e-10F);
        assertEquals(Float.MAX_VALUE, in.readFloat(), 1e-10F);
    }

    @Test
    public void testFloatArray() throws IOException {
        float[] arr = new float[] {Float.MIN_VALUE, -1, Float.MAX_VALUE};
        out.writeFloatArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readFloatArray(), 1e-10F);
    }

    @Test
    public void testLong() throws IOException {
        out.writeLong(Long.MIN_VALUE);
        out.writeLong(-1);
        out.writeLong(Long.MAX_VALUE);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(Long.MIN_VALUE, in.readLong());
        assertEquals(-1, in.readLong());
        assertEquals(Long.MAX_VALUE, in.readLong());
    }

    @Test
    public void testLongArray() throws IOException {
        long[] arr = new long[] {Long.MIN_VALUE, -1, Long.MAX_VALUE};
        out.writeLongArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readLongArray());
    }

    @Test
    public void testUTF() throws IOException {
        String s1 = "Vim is a text editor that is upwards compatible to Vi. It can be used to edit all kinds of plain text.";
        String s2 = "簡単なものから複雑なものまで、具体的な例を使って説明しています。本のように最初から順を追って読んでください。";
        out.writeString(s1);
        out.writeString(s2);

        ObjectDataInput in = getDataInputFromOutput();
        assertEquals(s1, in.readString());
        assertEquals(s2, in.readString());
    }

    @Test
    public void testUTFArray() throws IOException {
        String s1 = "Vim is a text editor that is upwards compatible to Vi. It can be used to edit all kinds of plain text.";
        String s2 = "簡単なものから複雑なものまで、具体的な例を使って説明しています。本のように最初から順を追って読んでください。";
        String[] arr = new String[] {s1, s2};
        out.writeUTFArray(arr);

        ObjectDataInput in = getDataInputFromOutput();
        assertArrayEquals(arr, in.readStringArray());
    }

    protected abstract byte[] getWrittenBytes();

    protected void assumptions() {
        // to be overridden if required
    }

    /**
     * @return a {@link ObjectDataOutput} to which test values are written
     */
    protected abstract O getDataOutput(InternalSerializationService serializationService);

    /**
     * @return a {@link ObjectDataInput} reading whatever was written to the test's output stream
     */
    protected abstract I getDataInputFromOutput();
}
