/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.serialization.GenericRecordBuilder.compact;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactNullablePrimitiveInteroperabilityTest {

    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    private SerializationService createSerializationService() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    @Test
    public void testWritePrimitiveReadNullable() {
        GenericRecordBuilder builder = compact("test");
        builder.setBoolean("boolean", true);
        builder.setByte("byte", (byte) 2);
        builder.setShort("short", (short) 4);
        builder.setInt("int", 8);
        builder.setLong("long", 4444L);
        builder.setFloat("float", 8321.321F);
        builder.setDouble("double", 41231.32);
        builder.setArrayOfBooleans("booleans", new boolean[]{true, false});
        builder.setArrayOfBytes("bytes", new byte[]{1, 2});
        builder.setArrayOfShorts("shorts", new short[]{1, 4});
        builder.setArrayOfInts("ints", new int[]{1, 8});
        builder.setArrayOfLongs("longs", new long[]{1, 4444L});
        builder.setArrayOfFloats("floats", new float[]{1, 8321.321F});
        builder.setArrayOfDoubles("doubles", new double[]{41231.32, 2});
        GenericRecord record = builder.build();

        assertTrue(record instanceof DeserializedGenericRecord);
        assertReadAsNullable(record);

        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);
        GenericRecord serializedRecord = serializationService.toObject(data);

        assertTrue(serializedRecord instanceof CompactInternalGenericRecord);
        assertReadAsNullable(serializedRecord);
    }

    void assertReadAsNullable(GenericRecord record) {
        assertEquals(true, record.getNullableBoolean("boolean"));
        assertEquals(Byte.valueOf((byte) 2), record.getNullableByte("byte"));
        assertEquals(Short.valueOf((short) 4), record.getNullableShort("short"));
        assertEquals(Integer.valueOf(8), record.getNullableInt("int"));
        assertEquals(Long.valueOf(4444L), record.getNullableLong("long"));
        assertEquals(Float.valueOf(8321.321F), record.getNullableFloat("float"));
        assertEquals(Double.valueOf(41231.32), record.getNullableDouble("double"));

        assertArrayEquals(new Boolean[]{true, false}, record.getArrayOfNullableBooleans("booleans"));
        assertArrayEquals(new Byte[]{1, 2}, record.getArrayOfNullableBytes("bytes"));
        assertArrayEquals(new Short[]{1, 4}, record.getArrayOfNullableShorts("shorts"));
        assertArrayEquals(new Integer[]{1, 8}, record.getArrayOfNullableInts("ints"));
        assertArrayEquals(new Long[]{1L, 4444L}, record.getArrayOfNullableLongs("longs"));
        assertArrayEquals(new Float[]{1F, 8321.321F}, record.getArrayOfNullableFloats("floats"));
        assertArrayEquals(new Double[]{41231.32, 2.0}, record.getArrayOfNullableDoubles("doubles"));
    }

    @Test
    public void testWriteNullableReadPrimitive() {
        GenericRecordBuilder builder = compact("test");
        builder.setNullableBoolean("boolean", true);
        builder.setNullableByte("byte", (byte) 4);
        builder.setNullableShort("short", (short) 6);
        builder.setNullableInt("int", 8);
        builder.setNullableLong("long", 4444L);
        builder.setNullableFloat("float", 8321.321F);
        builder.setNullableDouble("double", 41231.32);
        builder.setArrayOfNullableBooleans("booleans", new Boolean[]{true, false});
        builder.setArrayOfNullableBytes("bytes", new Byte[]{1, 2});
        builder.setArrayOfNullableShorts("shorts", new Short[]{1, 2});
        builder.setArrayOfNullableInts("ints", new Integer[]{1, 8});
        builder.setArrayOfNullableLongs("longs", new Long[]{1L, 4444L});
        builder.setArrayOfNullableFloats("floats", new Float[]{1F, 8321.321F});
        builder.setArrayOfNullableDoubles("doubles", new Double[]{41231.32, 2.0});
        GenericRecord record = builder.build();

        assertTrue(record instanceof DeserializedGenericRecord);
        assertReadAsPrimitive(record);

        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);
        GenericRecord serializedRecord = serializationService.toObject(data);

        assertTrue(serializedRecord instanceof CompactInternalGenericRecord);
        assertReadAsPrimitive(serializedRecord);
    }

    void assertReadAsPrimitive(GenericRecord record) {
        assertEquals(true, record.getBoolean("boolean"));
        assertEquals((byte) 4, record.getByte("byte"));
        assertEquals((short) 6, record.getShort("short"));
        assertEquals(8, record.getInt("int"));
        assertEquals(4444L, record.getLong("long"));
        assertEquals(8321.321F, record.getFloat("float"), 0);
        assertEquals(41231.32, record.getDouble("double"), 0);

        assertArrayEquals(new boolean[]{true, false}, record.getArrayOfBooleans("booleans"));
        assertArrayEquals(new byte[]{1, 2}, record.getArrayOfBytes("bytes"));
        assertArrayEquals(new short[]{1, 2}, record.getArrayOfShorts("shorts"));
        assertArrayEquals(new int[]{1, 8}, record.getArrayOfInts("ints"));
        assertArrayEquals(new long[]{1L, 4444L}, record.getArrayOfLongs("longs"));
        assertArrayEquals(new float[]{1F, 8321.321F}, record.getArrayOfFloats("floats"), 0);
        assertArrayEquals(new double[]{41231.32, 2.0}, record.getArrayOfDoubles("doubles"), 0);
    }

    @Test
    public void testWriteNullReadPrimitiveThrowsException() {
        GenericRecordBuilder builder = compact("test");
        builder.setNullableBoolean("boolean", null);
        builder.setNullableByte("byte", null);
        builder.setNullableShort("short", null);
        builder.setNullableInt("int", null);
        builder.setNullableLong("long", null);
        builder.setNullableFloat("float", null);
        builder.setNullableDouble("double", null);
        builder.setArrayOfNullableBooleans("booleans", new Boolean[]{null, false});
        builder.setArrayOfNullableBytes("bytes", new Byte[]{1, null});
        builder.setArrayOfNullableShorts("shorts", new Short[]{null, 2});
        builder.setArrayOfNullableInts("ints", new Integer[]{1, null});
        builder.setArrayOfNullableLongs("longs", new Long[]{null, 2L});
        builder.setArrayOfNullableFloats("floats", new Float[]{null, 2F});
        builder.setArrayOfNullableDoubles("doubles", new Double[]{1.0, null});
        GenericRecord record = builder.build();

        assertReadNullAsPrimitiveThrowsException(record);
        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);
        GenericRecord serializedRecord = serializationService.toObject(data);

        assertTrue(serializedRecord instanceof CompactInternalGenericRecord);
        assertReadNullAsPrimitiveThrowsException(serializedRecord);
    }

    private void assertReadNullAsPrimitiveThrowsException(GenericRecord record) {
        assertThrows(HazelcastSerializationException.class, () -> record.getBoolean("boolean"));
        assertThrows(HazelcastSerializationException.class, () -> record.getByte("byte"));
        assertThrows(HazelcastSerializationException.class, () -> record.getShort("short"));
        assertThrows(HazelcastSerializationException.class, () -> record.getInt("int"));
        assertThrows(HazelcastSerializationException.class, () -> record.getLong("long"));
        assertThrows(HazelcastSerializationException.class, () -> record.getFloat("float"));
        assertThrows(HazelcastSerializationException.class, () -> record.getDouble("double"));

        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfBooleans("booleans"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfBytes("bytes"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfShorts("shorts"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfInts("ints"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfLongs("longs"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfFloats("floats"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfDoubles("doubles"));
    }

    @Test
    public void testReflectiveSerializer() {
        SerializationService serializationService = createSerializationService();
        //We set Nullable for primitives and primitives for Nullable fields on purpose on the generic record
        GenericRecordBuilder builder = compact(PrimitiveObject.class.getName());
        builder.setNullableBoolean("boolean_", true);
        builder.setNullableByte("byte_", (byte) 2);
        builder.setNullableShort("short_", (short) 4);
        builder.setNullableInt("int_", 8);
        builder.setNullableLong("long_", 4444L);
        builder.setNullableFloat("float_", 8321.321F);
        builder.setNullableDouble("double_", 41231.32);
        builder.setArrayOfNullableBooleans("booleans", new Boolean[]{true, false});
        builder.setArrayOfNullableBytes("bytes", new Byte[]{1, 2});
        builder.setArrayOfNullableShorts("shorts", new Short[]{1, 4});
        builder.setArrayOfNullableInts("ints", new Integer[]{1, 8});
        builder.setArrayOfNullableLongs("longs", new Long[]{1L, 4444L});
        builder.setArrayOfNullableFloats("floats", new Float[]{1F, 8321.321F});
        builder.setArrayOfNullableDoubles("doubles", new Double[]{41231.32, 2.0});
        builder.setBoolean("nullableBoolean", true);
        builder.setByte("nullableByte", (byte) 4);
        builder.setShort("nullableShort", (short) 6);
        builder.setInt("nullableInt", 8);
        builder.setLong("nullableLong", 4444L);
        builder.setFloat("nullableFloat", 8321.321F);
        builder.setDouble("nullableDouble", 41231.32);
        builder.setArrayOfBooleans("nullableBooleans", new boolean[]{true, false});
        builder.setArrayOfBytes("nullableBytes", new byte[]{1, 2});
        builder.setArrayOfShorts("nullableShorts", new short[]{1, 4});
        builder.setArrayOfInts("nullableInts", new int[]{1, 8});
        builder.setArrayOfLongs("nullableLongs", new long[]{1L, 4444L});
        builder.setArrayOfFloats("nullableFloats", new float[]{1F, 8321.321F});
        builder.setArrayOfDoubles("nullableDoubles", new double[]{41231.32, 2.0});
        GenericRecord record = builder.build();

        Data data = serializationService.toData(record);
        PrimitiveObject primitiveObject = serializationService.toObject(data);

        assertEquals(true, primitiveObject.boolean_);
        assertEquals((byte) 2, primitiveObject.byte_);
        assertEquals((short) 4, primitiveObject.short_);
        assertEquals(8, primitiveObject.int_);
        assertEquals(4444L, primitiveObject.long_);
        assertEquals(8321.321F, primitiveObject.float_, 0);
        assertEquals(41231.32, primitiveObject.double_, 0);

        assertArrayEquals(new boolean[]{true, false}, primitiveObject.booleans);
        assertArrayEquals(new byte[]{1, 2}, primitiveObject.bytes);
        assertArrayEquals(new short[]{1, 4}, primitiveObject.shorts);
        assertArrayEquals(new int[]{1, 8}, primitiveObject.ints);
        assertArrayEquals(new long[]{1L, 4444L}, primitiveObject.longs);
        assertArrayEquals(new float[]{1F, 8321.321F}, primitiveObject.floats, 0);
        assertArrayEquals(new double[]{41231.32, 2.0}, primitiveObject.doubles, 0);

        assertEquals(true, primitiveObject.nullableBoolean);
        assertEquals(Byte.valueOf((byte) 4), primitiveObject.nullableByte);
        assertEquals(Short.valueOf((short) 6), primitiveObject.nullableShort);
        assertEquals(Integer.valueOf(8), primitiveObject.nullableInt);
        assertEquals(Long.valueOf(4444L), primitiveObject.nullableLong);
        assertEquals(Float.valueOf(8321.321F), primitiveObject.nullableFloat);
        assertEquals(Double.valueOf(41231.32), primitiveObject.nullableDouble);

        assertArrayEquals(new Boolean[]{true, false}, primitiveObject.nullableBooleans);
        assertArrayEquals(new Byte[]{1, 2}, primitiveObject.nullableBytes);
        assertArrayEquals(new Short[]{1, 4}, primitiveObject.nullableShorts);
        assertArrayEquals(new Integer[]{1, 8}, primitiveObject.nullableInts);
        assertArrayEquals(new Long[]{1L, 4444L}, primitiveObject.nullableLongs);
        assertArrayEquals(new Float[]{1F, 8321.321F}, primitiveObject.nullableFloats);
        assertArrayEquals(new Double[]{41231.32, 2.0}, primitiveObject.nullableDoubles);
    }

    public static class PrimitiveObject {

        boolean boolean_;
        byte byte_;
        short short_;
        int int_;
        long long_;
        float float_;
        double double_;

        boolean[] booleans;
        byte[] bytes;
        short[] shorts;
        int[] ints;
        long[] longs;
        float[] floats;
        double[] doubles;

        Boolean nullableBoolean;
        Byte nullableByte;
        Short nullableShort;
        Integer nullableInt;
        Long nullableLong;
        Float nullableFloat;
        Double nullableDouble;

        Boolean[] nullableBooleans;
        Byte[] nullableBytes;
        Short[] nullableShorts;
        Integer[] nullableInts;
        Long[] nullableLongs;
        Float[] nullableFloats;
        Double[] nullableDoubles;
    }
}
