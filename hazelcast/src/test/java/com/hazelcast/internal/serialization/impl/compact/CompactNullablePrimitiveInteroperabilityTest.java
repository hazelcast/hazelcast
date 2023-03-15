/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import javax.annotation.Nonnull;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createArrayOfFixedSizeFieldsDTO;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createArrayOfFixedSizeFieldsDTOAsNullValues;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createFixedSizeFieldsDTO;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.compact;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactNullablePrimitiveInteroperabilityTest {

    private static class A {
        public Integer[] ids;
        public Integer age;

        A(Integer age, Integer[] ids) {
            this.age = age;
            this.ids = ids;
        }
    }

    private static class ASerializer implements CompactSerializer<A> {
        @Nonnull
        @Override
        public A read(@Nonnull CompactReader reader) {
            int age = reader.readInt32("age");
            int[] ids = reader.readArrayOfInt32("ids");
            Integer[] boxedIds = new Integer[ids.length];
            for (int i = 0; i < ids.length; i++) {
                boxedIds[i] = ids[i];
            }
            return new A(age, boxedIds);
        }

        @Override
        public void write(@Nonnull CompactWriter writer, @Nonnull A object) {
            writer.writeNullableInt32("age", object.age);
            writer.writeArrayOfNullableInt32("ids", object.ids);
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "A";
        }

        @Nonnull
        @Override
        public Class<A> getCompactClass() {
            return A.class;
        }
    }

    @Test
    public void testWritePrimitiveReadNullable() {
        GenericRecordBuilder builder = compact("test");
        builder.setBoolean("boolean", true);
        builder.setInt8("byte", (byte) 2);
        builder.setInt16("short", (short) 4);
        builder.setInt32("int", 8);
        builder.setInt64("long", 4444L);
        builder.setFloat32("float", 8321.321F);
        builder.setFloat64("double", 41231.32);
        builder.setArrayOfBoolean("booleans", new boolean[]{true, false});
        builder.setArrayOfInt8("bytes", new byte[]{1, 2});
        builder.setArrayOfInt16("shorts", new short[]{1, 4});
        builder.setArrayOfInt32("ints", new int[]{1, 8});
        builder.setArrayOfInt64("longs", new long[]{1, 4444L});
        builder.setArrayOfFloat32("floats", new float[]{1, 8321.321F});
        builder.setArrayOfFloat64("doubles", new double[]{41231.32, 2});
        GenericRecord record = builder.build();

        assertTrue(record instanceof DeserializedGenericRecord);
        assertReadAsNullable(record);

        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);
        GenericRecord serializedRecord = serializationService.toObject(data);

        assertTrue(serializedRecord instanceof DeserializedGenericRecord);
        assertReadAsNullable(serializedRecord);
    }

    @Test
    public void testWritePrimitiveReadNullableCustomSerializer() {
        FixedSizeFieldsDTO fixedSizeFieldsDTO = createFixedSizeFieldsDTO();
        SerializationService serializationService = createSerializationService(FixedSizeFieldsDTOSerializerReadingNullable::new);

        Data data = serializationService.toData(fixedSizeFieldsDTO);
        FixedSizeFieldsDTO obj = serializationService.toObject(data);

        assertEquals(obj, fixedSizeFieldsDTO);
    }

    @Test
    public void testWriteArraysOfPrimitiveReadNullableCustomSerializer() {
        ArrayOfFixedSizeFieldsDTO arrayOfFixedSizeFieldsDTO = createArrayOfFixedSizeFieldsDTO();
        SerializationService serializationService = createSerializationService(ArrayOfFixedSizeFieldsDTOSerializerReadingNullable::new);

        Data data = serializationService.toData(arrayOfFixedSizeFieldsDTO);
        ArrayOfFixedSizeFieldsDTO obj = serializationService.toObject(data);

        assertEquals(obj, arrayOfFixedSizeFieldsDTO);
    }

    @Test
    public void testWriteArraysOfPrimitiveAsNullReadNullableCustomSerializer() {
        ArrayOfFixedSizeFieldsDTO arrayOfFixedSizeFieldsDTO = createArrayOfFixedSizeFieldsDTOAsNullValues();
        SerializationService serializationService = createSerializationService(ArrayOfFixedSizeFieldsDTOSerializerReadingNullable::new);

        Data data = serializationService.toData(arrayOfFixedSizeFieldsDTO);
        ArrayOfFixedSizeFieldsDTO obj = serializationService.toObject(data);

        assertEquals(obj, arrayOfFixedSizeFieldsDTO);
    }

    @Test
    public void testWriteNullableReadPrimitiveCustomSerializer() {
        FixedSizeFieldsDTO fixedSizeFieldsDTO = createFixedSizeFieldsDTO();
        SerializationService serializationService = createSerializationService(FixedSizeFieldsDTOSerializerWritingNullable::new);

        Data data = serializationService.toData(fixedSizeFieldsDTO);
        FixedSizeFieldsDTO obj = serializationService.toObject(data);

        assertEquals(obj, fixedSizeFieldsDTO);
    }

    @Test
    public void testWriteNullableReadArraysOfPrimitiveCustomSerializer() {
        ArrayOfFixedSizeFieldsDTO arrayOfFixedSizeFieldsDTO = createArrayOfFixedSizeFieldsDTO();
        SerializationService serializationService =
                createSerializationService(ArrayOfFixedSizeFieldsDTOSerializerWritingNullable::new);

        Data data = serializationService.toData(arrayOfFixedSizeFieldsDTO);
        ArrayOfFixedSizeFieldsDTO obj = serializationService.toObject(data);

        assertEquals(obj, arrayOfFixedSizeFieldsDTO);
    }

    void assertReadAsNullable(GenericRecord record) {
        assertEquals(true, record.getNullableBoolean("boolean"));
        assertEquals(Byte.valueOf((byte) 2), record.getNullableInt8("byte"));
        assertEquals(Short.valueOf((short) 4), record.getNullableInt16("short"));
        assertEquals(Integer.valueOf(8), record.getNullableInt32("int"));
        assertEquals(Long.valueOf(4444L), record.getNullableInt64("long"));
        assertEquals(Float.valueOf(8321.321F), record.getNullableFloat32("float"));
        assertEquals(Double.valueOf(41231.32), record.getNullableFloat64("double"));

        assertArrayEquals(new Boolean[]{true, false}, record.getArrayOfNullableBoolean("booleans"));
        assertArrayEquals(new Byte[]{1, 2}, record.getArrayOfNullableInt8("bytes"));
        assertArrayEquals(new Short[]{1, 4}, record.getArrayOfNullableInt16("shorts"));
        assertArrayEquals(new Integer[]{1, 8}, record.getArrayOfNullableInt32("ints"));
        assertArrayEquals(new Long[]{1L, 4444L}, record.getArrayOfNullableInt64("longs"));
        assertArrayEquals(new Float[]{1F, 8321.321F}, record.getArrayOfNullableFloat32("floats"));
        assertArrayEquals(new Double[]{41231.32, 2.0}, record.getArrayOfNullableFloat64("doubles"));
    }

    @Test
    public void testWriteNullableReadPrimitive() {
        GenericRecordBuilder builder = compact("test");
        builder.setNullableBoolean("boolean", true);
        builder.setNullableInt8("byte", (byte) 4);
        builder.setNullableInt16("short", (short) 6);
        builder.setNullableInt32("int", 8);
        builder.setNullableInt64("long", 4444L);
        builder.setNullableFloat32("float", 8321.321F);
        builder.setNullableFloat64("double", 41231.32);
        builder.setArrayOfNullableBoolean("booleans", new Boolean[]{true, false});
        builder.setArrayOfNullableInt8("bytes", new Byte[]{1, 2});
        builder.setArrayOfNullableInt16("shorts", new Short[]{1, 2});
        builder.setArrayOfNullableInt32("ints", new Integer[]{1, 8});
        builder.setArrayOfNullableInt64("longs", new Long[]{1L, 4444L});
        builder.setArrayOfNullableFloat32("floats", new Float[]{1F, 8321.321F});
        builder.setArrayOfNullableFloat64("doubles", new Double[]{41231.32, 2.0});
        GenericRecord record = builder.build();

        assertTrue(record instanceof DeserializedGenericRecord);
        assertReadAsPrimitive(record);

        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);
        GenericRecord serializedRecord = serializationService.toObject(data);

        assertTrue(serializedRecord instanceof DeserializedGenericRecord);
        assertReadAsPrimitive(serializedRecord);
    }

    void assertReadAsPrimitive(GenericRecord record) {
        assertTrue(record.getBoolean("boolean"));
        assertEquals((byte) 4, record.getInt8("byte"));
        assertEquals((short) 6, record.getInt16("short"));
        assertEquals(8, record.getInt32("int"));
        assertEquals(4444L, record.getInt64("long"));
        assertEquals(8321.321F, record.getFloat32("float"), 0);
        assertEquals(41231.32, record.getFloat64("double"), 0);

        assertArrayEquals(new boolean[]{true, false}, record.getArrayOfBoolean("booleans"));
        assertArrayEquals(new byte[]{1, 2}, record.getArrayOfInt8("bytes"));
        assertArrayEquals(new short[]{1, 2}, record.getArrayOfInt16("shorts"));
        assertArrayEquals(new int[]{1, 8}, record.getArrayOfInt32("ints"));
        assertArrayEquals(new long[]{1L, 4444L}, record.getArrayOfInt64("longs"));
        assertArrayEquals(new float[]{1F, 8321.321F}, record.getArrayOfFloat32("floats"), 0);
        assertArrayEquals(new double[]{41231.32, 2.0}, record.getArrayOfFloat64("doubles"), 0);
    }

    @Test
    public void testWriteNullReadPrimitiveThrowsException() {
        GenericRecordBuilder builder = compact("test");
        builder.setNullableBoolean("boolean", null);
        builder.setNullableInt8("byte", null);
        builder.setNullableInt16("short", null);
        builder.setNullableInt32("int", null);
        builder.setNullableInt64("long", null);
        builder.setNullableFloat32("float", null);
        builder.setNullableFloat64("double", null);
        builder.setArrayOfNullableBoolean("booleans", new Boolean[]{null, false});
        builder.setArrayOfNullableInt8("bytes", new Byte[]{1, null});
        builder.setArrayOfNullableInt16("shorts", new Short[]{null, 2});
        builder.setArrayOfNullableInt32("ints", new Integer[]{1, null});
        builder.setArrayOfNullableInt64("longs", new Long[]{null, 2L});
        builder.setArrayOfNullableFloat32("floats", new Float[]{null, 2F});
        builder.setArrayOfNullableFloat64("doubles", new Double[]{1.0, null});
        GenericRecord record = builder.build();

        assertReadNullAsPrimitiveThrowsException(record);
        SerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);
        GenericRecord serializedRecord = serializationService.toObject(data);

        assertTrue(serializedRecord instanceof DeserializedGenericRecord);
        assertReadNullAsPrimitiveThrowsException(serializedRecord);
    }

    @Test
    public void testWriteNullReadPrimitiveThrowsExceptionWithCorrectMethodPrefixCompactReader() {
        SerializationService serializationService = createSerializationService(ASerializer::new);
        // Reading null value with non-nullable reader method
        A a1 = new A(null, new Integer[]{1, 2, 3});
        Data data = serializationService.toData(a1);
        // Reading compact with serializer case
        assertThatThrownBy(() -> serializationService.toObject(data))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Use readNullable");
        // Reading array field with null value
        A a2 = new A(1, new Integer[]{1, null, 3});
        Data data2 = serializationService.toData(a2);
        // Reading compact with serializer case
        assertThatThrownBy(() -> serializationService.toObject(data2))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Use readArrayOfNullable");
    }

    @Test
    public void testWriteNullReadPrimitiveThrowsExceptionWithCorrectMethodPrefixGenericRecord() {
        SerializationService serializationService = createSerializationService();
        GenericRecordBuilder builder = compact("genericRecord");
        builder.setNullableInt32("aField", null);
        GenericRecord record = builder.build();

        Data data = serializationService.toData(record);
        GenericRecord obj = serializationService.toObject(data);
        // Read null value with non-nullable reader method
        assertThatThrownBy(() -> obj.getInt32("aField"))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Use getNullable");

        GenericRecordBuilder builder2 = compact("genericRecord2");
        builder2.setArrayOfNullableInt32("aField",  new Integer[]{1, null, 3});
        GenericRecord record2 = builder2.build();

        Data data2 = serializationService.toData(record2);
        GenericRecord obj2 = serializationService.toObject(data2);
        // Read an array with null value with non-nullable array reader method
        assertThatThrownBy(() -> obj2.getArrayOfInt32("aField"))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Use getArrayOfNullable");
    }

    private void assertReadNullAsPrimitiveThrowsException(GenericRecord record) {
        assertThrows(HazelcastSerializationException.class, () -> record.getBoolean("boolean"));
        assertThrows(HazelcastSerializationException.class, () -> record.getInt8("byte"));
        assertThrows(HazelcastSerializationException.class, () -> record.getInt16("short"));
        assertThrows(HazelcastSerializationException.class, () -> record.getInt32("int"));
        assertThrows(HazelcastSerializationException.class, () -> record.getInt64("long"));
        assertThrows(HazelcastSerializationException.class, () -> record.getFloat32("float"));
        assertThrows(HazelcastSerializationException.class, () -> record.getFloat64("double"));

        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfBoolean("booleans"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfInt8("bytes"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfInt16("shorts"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfInt32("ints"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfInt64("longs"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfFloat32("floats"));
        assertThrows(HazelcastSerializationException.class, () -> record.getArrayOfFloat64("doubles"));
    }

    @Test
    public void testReflectiveSerializer() {
        SerializationService serializationService = createSerializationService();
        //We set Nullable for primitives and primitives for Nullable fields on purpose on the generic record
        GenericRecordBuilder builder = compact(PrimitiveObject.class.getName());
        builder.setNullableBoolean("boolean_", true);
        builder.setNullableInt8("byte_", (byte) 2);
        // Reflective serializer represents char as short
        builder.setNullableInt16("char_", (short) '\u4242');
        builder.setNullableInt16("short_", (short) 4);
        builder.setNullableInt32("int_", 8);
        builder.setNullableInt64("long_", 4444L);
        builder.setNullableFloat32("float_", 8321.321F);
        builder.setNullableFloat64("double_", 41231.32);
        builder.setArrayOfNullableBoolean("booleans", new Boolean[]{true, false});
        builder.setArrayOfNullableInt8("bytes", new Byte[]{1, 2});
        builder.setArrayOfNullableInt16("chars", new Short[]{'\u4224', '\u0101'});
        builder.setArrayOfNullableInt16("shorts", new Short[]{1, 4});
        builder.setArrayOfNullableInt32("ints", new Integer[]{1, 8});
        builder.setArrayOfNullableInt64("longs", new Long[]{1L, 4444L});
        builder.setArrayOfNullableFloat32("floats", new Float[]{1F, 8321.321F});
        builder.setArrayOfNullableFloat64("doubles", new Double[]{41231.32, 2.0});
        builder.setBoolean("nullableBoolean", true);
        builder.setInt8("nullableByte", (byte) 4);
        builder.setInt16("nullableCharacter", (short) '\u1234');
        builder.setInt16("nullableShort", (short) 6);
        builder.setInt32("nullableInt", 8);
        builder.setInt64("nullableLong", 4444L);
        builder.setFloat32("nullableFloat", 8321.321F);
        builder.setFloat64("nullableDouble", 41231.32);
        builder.setArrayOfBoolean("nullableBooleans", new boolean[]{true, false});
        builder.setArrayOfInt8("nullableBytes", new byte[]{1, 2});
        builder.setArrayOfInt16("nullableCharacters", new short[]{'\u4321', 'a', '0'});
        builder.setArrayOfInt16("nullableShorts", new short[]{1, 4});
        builder.setArrayOfInt32("nullableInts", new int[]{1, 8});
        builder.setArrayOfInt64("nullableLongs", new long[]{1L, 4444L});
        builder.setArrayOfFloat32("nullableFloats", new float[]{1F, 8321.321F});
        builder.setArrayOfFloat64("nullableDoubles", new double[]{41231.32, 2.0});
        GenericRecord record = builder.build();

        Data data = serializationService.toData(record);
        PrimitiveObject primitiveObject = serializationService.toObject(data);

        assertTrue(primitiveObject.boolean_);
        assertEquals((byte) 2, primitiveObject.byte_);
        assertEquals('\u4242', primitiveObject.char_);
        assertEquals((short) 4, primitiveObject.short_);
        assertEquals(8, primitiveObject.int_);
        assertEquals(4444L, primitiveObject.long_);
        assertEquals(8321.321F, primitiveObject.float_, 0);
        assertEquals(41231.32, primitiveObject.double_, 0);

        assertArrayEquals(new boolean[]{true, false}, primitiveObject.booleans);
        assertArrayEquals(new byte[]{1, 2}, primitiveObject.bytes);
        assertArrayEquals(new char[]{'\u4224', '\u0101'}, primitiveObject.chars);
        assertArrayEquals(new short[]{1, 4}, primitiveObject.shorts);
        assertArrayEquals(new int[]{1, 8}, primitiveObject.ints);
        assertArrayEquals(new long[]{1L, 4444L}, primitiveObject.longs);
        assertArrayEquals(new float[]{1F, 8321.321F}, primitiveObject.floats, 0);
        assertArrayEquals(new double[]{41231.32, 2.0}, primitiveObject.doubles, 0);

        assertEquals(true, primitiveObject.nullableBoolean);
        assertEquals(Byte.valueOf((byte) 4), primitiveObject.nullableByte);
        assertEquals(Character.valueOf('\u1234'), primitiveObject.nullableCharacter);
        assertEquals(Short.valueOf((short) 6), primitiveObject.nullableShort);
        assertEquals(Integer.valueOf(8), primitiveObject.nullableInt);
        assertEquals(Long.valueOf(4444L), primitiveObject.nullableLong);
        assertEquals(Float.valueOf(8321.321F), primitiveObject.nullableFloat);
        assertEquals(Double.valueOf(41231.32), primitiveObject.nullableDouble);

        assertArrayEquals(new Boolean[]{true, false}, primitiveObject.nullableBooleans);
        assertArrayEquals(new Byte[]{1, 2}, primitiveObject.nullableBytes);
        assertArrayEquals(new Character[]{'\u4321', 'a', '0'}, primitiveObject.nullableCharacters);
        assertArrayEquals(new Short[]{1, 4}, primitiveObject.nullableShorts);
        assertArrayEquals(new Integer[]{1, 8}, primitiveObject.nullableInts);
        assertArrayEquals(new Long[]{1L, 4444L}, primitiveObject.nullableLongs);
        assertArrayEquals(new Float[]{1F, 8321.321F}, primitiveObject.nullableFloats);
        assertArrayEquals(new Double[]{41231.32, 2.0}, primitiveObject.nullableDoubles);
    }

    public static class PrimitiveObject {

        boolean boolean_;
        byte byte_;
        char char_;
        short short_;
        int int_;
        long long_;
        float float_;
        double double_;

        boolean[] booleans;
        byte[] bytes;
        char[] chars;
        short[] shorts;
        int[] ints;
        long[] longs;
        float[] floats;
        double[] doubles;

        Boolean nullableBoolean;
        Byte nullableByte;
        Character nullableCharacter;
        Short nullableShort;
        Integer nullableInt;
        Long nullableLong;
        Float nullableFloat;
        Double nullableDouble;

        Boolean[] nullableBooleans;
        Byte[] nullableBytes;
        Character[] nullableCharacters;
        Short[] nullableShorts;
        Integer[] nullableInts;
        Long[] nullableLongs;
        Float[] nullableFloats;
        Double[] nullableDoubles;
    }
}
