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

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.BitsDTO;
import example.serialization.EmployeeDTO;
import example.serialization.EmployeeDTOSerializer;
import example.serialization.EmployerDTO;
import example.serialization.ExternalizableEmployeeDTO;
import example.serialization.HiringStatus;
import example.serialization.InnerDTO;
import example.serialization.InnerDTOSerializer;
import example.serialization.MainDTO;
import example.serialization.MainDTOSerializer;
import example.serialization.NamedDTO;
import example.serialization.NodeDTO;
import example.serialization.CompactWithInnerFieldAsUuid;
import example.serialization.CustomUUIDSerializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.UUID;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createCompactGenericRecord;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createFixedSizeFieldsDTO;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createMainDTO;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createVarSizedFieldsDTO;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.compact;
import static example.serialization.HiringStatus.HIRING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactStreamSerializerTest {

    @Test
    public void testAllTypesWithReflectiveSerializer() {
        SerializationService serializationService = createSerializationService();
        MainDTO expected = createMainDTO();

        Data data = serializationService.toData(expected);
        MainDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testAllTypes() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setSerializers(new MainDTOSerializer(), new InnerDTOSerializer());
        SerializationService serializationService = createSerializationService(compactSerializationConfig);
        MainDTO expected = createMainDTO();

        Data data = serializationService.toData(expected);
        MainDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testNoFields() {
        SerializationService serializationService = createSerializationService(EmptyDTOSerializer::new);
        EmptyDTO expected = new EmptyDTO();

        Data data = serializationService.toData(expected);
        EmptyDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testVarSizedFields() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.addSerializer(new VarSizedFieldsDTOSerializer());
        compactSerializationConfig.addSerializer(new InnerDTOSerializer());
        SerializationService serializationService = createSerializationService(compactSerializationConfig);
        VarSizedFieldsDTO expected = createVarSizedFieldsDTO();

        Data data = serializationService.toData(expected);
        VarSizedFieldsDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testFixedSizedFields() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.addSerializer(new FixedSizeFieldsDTOSerializer());
        compactSerializationConfig.addSerializer(new InnerDTOSerializer());
        SerializationService serializationService = createSerializationService(compactSerializationConfig);
        FixedSizeFieldsDTO expected = createFixedSizeFieldsDTO();

        Data data = serializationService.toData(expected);
        FixedSizeFieldsDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testTypeMismatchThrowsException() {
        SerializationService serializationService = createSerializationService(FixedSizeFieldsDTOSerializer::new);
        GenericRecord expected = GenericRecordBuilder.compact("fixedSizeFields")
                .setInt16("b", (short) 1) // wrong type, should have been int8
                .setBoolean("bool", true)
                .setInt16("s", (short) 1)
                .setInt32("i", 1)
                .setInt64("l", 1L)
                .setFloat32("f", 1.0f)
                .setFloat64("d", 1.0)
                .build();

        Data data = serializationService.toData(expected);

        assertThatThrownBy(() -> serializationService.toObject(data))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Invalid field kind");
    }

    @Test
    public void testReaderReturnsDefaultValues_whenDataIsMissing() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.addSerializer(new MainDTOSerializer());
        compactSerializationConfig.addSerializer(new InnerDTOSerializer());
        SerializationService serializationService = createSerializationService(compactSerializationConfig);

        Data data = serializationService.toData(GenericRecordBuilder.compact("main").build());
        MainDTO actual = serializationService.toObject(data);

        assertEquals(1, actual.b);
        assertFalse(actual.bool);
        assertEquals(1, actual.s);
        assertEquals(1, actual.i);
        assertEquals(1, actual.l);
        assertEquals(1, actual.f, 0.001);
        assertEquals(1, actual.d, 0.001);
        assertEquals("NA", actual.str);
        assertEquals(BigDecimal.valueOf(1), actual.bigDecimal);
        assertEquals(LocalTime.of(1, 1, 1), actual.localTime);
        assertEquals(LocalDate.of(1, 1, 1), actual.localDate);
        assertEquals(LocalDateTime.of(1, 1, 1, 1, 1, 1), actual.localDateTime);
        assertEquals(OffsetDateTime.of(1, 1, 1, 1, 1, 1, 1, ZoneOffset.ofHours(1)), actual.offsetDateTime);
        assertEquals(Byte.valueOf((byte) 1), actual.nullableB);
        assertEquals(Boolean.FALSE, actual.nullableBool);
        assertEquals(Short.valueOf((short) 1), actual.nullableS);
        assertEquals(Integer.valueOf(1), actual.nullableI);
        assertEquals(Long.valueOf(1), actual.nullableL);
        assertEquals(1F, actual.nullableF, 0.001);
        assertEquals(1.0, actual.nullableD, 0.001);


        Data innerData = serializationService.toData(GenericRecordBuilder.compact("inner").build());
        InnerDTO innerDTO = serializationService.toObject(innerData);
        assertArrayEquals(new boolean[0], innerDTO.bools);
        assertArrayEquals(new byte[0], innerDTO.bytes);
        assertArrayEquals(new short[0], innerDTO.shorts);
        assertArrayEquals(new int[0], innerDTO.ints);
        assertArrayEquals(new long[0], innerDTO.longs);
        assertArrayEquals(new float[0], innerDTO.floats, 0.001f);
        assertArrayEquals(new double[0], innerDTO.doubles, 0.001);
        assertArrayEquals(new String[0], innerDTO.strings);
        assertArrayEquals(new NamedDTO[0], innerDTO.nn);
        assertArrayEquals(new BigDecimal[0], innerDTO.bigDecimals);
        assertArrayEquals(new LocalTime[0], innerDTO.localTimes);
        assertArrayEquals(new LocalDate[0], innerDTO.localDates);
        assertArrayEquals(new LocalDateTime[0], innerDTO.localDateTimes);
        assertArrayEquals(new OffsetDateTime[0], innerDTO.offsetDateTimes);
        assertArrayEquals(new Boolean[0], innerDTO.nullableBools);
        assertArrayEquals(new Byte[0], innerDTO.nullableBytes);
        assertArrayEquals(new Short[0], innerDTO.nullableShorts);
        assertArrayEquals(new Integer[0], innerDTO.nullableIntegers);
        assertArrayEquals(new Long[0], innerDTO.nullableLongs);
        assertArrayEquals(new Float[0], innerDTO.nullableFloats);
        assertArrayEquals(new Double[0], innerDTO.nullableDoubles);
    }

    @Test
    public void testDefaultsReflection_insideCollection() {
        SerializationService serializationService = createSerializationService();

        NodeDTO node = new NodeDTO(new NodeDTO(new NodeDTO(2), 1), 0);

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        long[] ids = new long[]{22, 44};

        EmployeeDTO[] employeeDTOS = new EmployeeDTO[5];
        for (int j = 0; j < employeeDTOS.length; j++) {
            employeeDTOS[j] = new EmployeeDTO(20 + j, j * 100);
        }
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, HIRING, ids, employeeDTO, employeeDTOS);

        ArrayList<Object> expected = new ArrayList<>();
        expected.add(node);
        expected.add(employeeDTO);
        expected.add(employerDTO);

        Data data = serializationService.toData(expected);
        ArrayList<Object> arrayList = serializationService.toObject(data);
        assertEquals(node, arrayList.get(0));
        assertEquals(employeeDTO, arrayList.get(1));
        assertEquals(employerDTO, arrayList.get(2));
    }

    @Test
    public void testDefaultsReflection_recursive() {
        SerializationService serializationService = createSerializationService();

        NodeDTO node = new NodeDTO(new NodeDTO(new NodeDTO(2), 1), 0);

        Data data = serializationService.toData(node);

        Object object = serializationService.toObject(data);
        NodeDTO o = (NodeDTO) object;

        assertEquals(node, o);
    }


    @Test
    public void testDefaultsReflection_nested() {
        SerializationService serializationService = createSerializationService();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        long[] ids = new long[2];
        ids[0] = 22;
        ids[1] = 44;

        EmployeeDTO[] employeeDTOS = new EmployeeDTO[5];
        for (int j = 0; j < employeeDTOS.length; j++) {
            employeeDTOS[j] = new EmployeeDTO(20 + j, j * 100);
        }
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, HIRING, ids, employeeDTO, employeeDTOS);

        Data data = serializationService.toData(employerDTO);

        Object object = serializationService.toObject(data);
        EmployerDTO o = (EmployerDTO) object;
        assertEquals(employerDTO, o);
    }

    @Test
    public void testBits() throws IOException {
        // Share schemaService to make schema available to ss2
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        InternalSerializationService ss1 = createSerializationService(schemaService);
        InternalSerializationService ss2 = createSerializationService(schemaService);

        BitsDTO bitsDTO = new BitsDTO();
        bitsDTO.a = true;
        bitsDTO.h = true;
        bitsDTO.id = 121;
        bitsDTO.booleans = new boolean[8];
        bitsDTO.booleans[0] = true;
        bitsDTO.booleans[4] = true;

        Data data = ss1.toData(bitsDTO);

        // hash(4) + typeid(4) + schemaId(8) + (4 byte length) + (1 bytes for 8 bits) + (4 bytes for int)
        // (4 byte length of byte array) + (1 byte for booleans array of 8 bits) + (1 byte offset bytes)
        assertEquals(31, data.toByteArray().length);

        GenericRecordQueryReader reader = new GenericRecordQueryReader(ss2.readAsInternalGenericRecord(data));
        assertEquals(121, reader.read("id"));
        assertTrue((Boolean) reader.read("a"));
        assertFalse((Boolean) reader.read("b"));
        assertFalse((Boolean) reader.read("c"));
        assertFalse((Boolean) reader.read("d"));
        assertFalse((Boolean) reader.read("e"));
        assertFalse((Boolean) reader.read("f"));
        assertFalse((Boolean) reader.read("g"));
        assertTrue((Boolean) reader.read("h"));
        assertTrue((Boolean) reader.read("booleans[0]"));
        assertFalse((Boolean) reader.read("booleans[1]"));
        assertFalse((Boolean) reader.read("booleans[2]"));
        assertFalse((Boolean) reader.read("booleans[3]"));
        assertTrue((Boolean) reader.read("booleans[4]"));
        assertFalse((Boolean) reader.read("booleans[5]"));
        assertFalse((Boolean) reader.read("booleans[6]"));
        assertFalse((Boolean) reader.read("booleans[7]"));

        Object object = ss2.toObject(data);
        BitsDTO o = (BitsDTO) object;
        assertEquals(bitsDTO, o);
    }


    @Test
    public void testWithExplicitSerializer_nested() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.addSerializer(
                new CompactSerializer<EmployeeDTO>() {
                    @Nonnull
                    @Override
                    public EmployeeDTO read(@Nonnull CompactReader reader) {
                        return new EmployeeDTO(reader.readInt32("a"), reader.readInt64("i"));
                    }

                    @Override
                    public void write(@Nonnull CompactWriter writer, @Nonnull EmployeeDTO object) {
                        writer.writeInt32("a", object.getAge());
                        writer.writeInt64("i", object.getId());
                    }

                    @Nonnull
                    @Override
                    public String getTypeName() {
                        return "employee";
                    }

                    @Nonnull
                    @Override
                    public Class<EmployeeDTO> getCompactClass() {
                        return EmployeeDTO.class;
                    }
                });
        compactSerializationConfig.addSerializer(
                new CompactSerializer<EmployerDTO>() {
                    @Nonnull
                    @Override
                    public EmployerDTO read(@Nonnull CompactReader reader) {
                        String name = reader.readString("n");
                        String status = reader.readString("hs");
                        int age = reader.readInt32("a");
                        long[] ids = reader.readArrayOfInt64("ids");
                        EmployeeDTO s = reader.readCompact("s");
                        EmployeeDTO[] ss = reader.readArrayOfCompact("ss", EmployeeDTO.class);
                        return new EmployerDTO(name, age, status == null ? null : HiringStatus.valueOf(status), ids, s, ss);
                    }

                    @Override
                    public void write(@Nonnull CompactWriter writer, @Nonnull EmployerDTO object) {
                        writer.writeString("n", object.getName());
                        writer.writeString("hs", object.getHiringStatus() == null ? null : object.getHiringStatus().name());
                        writer.writeInt32("a", object.getZcode());
                        writer.writeArrayOfInt64("ids", object.getIds());
                        writer.writeCompact("s", object.getSingleEmployee());
                        writer.writeArrayOfCompact("ss", object.getOtherEmployees());
                    }

                    @Nonnull
                    @Override
                    public String getTypeName() {
                        return "employer";
                    }

                    @Nonnull
                    @Override
                    public Class<EmployerDTO> getCompactClass() {
                        return EmployerDTO.class;
                    }
                });

        SerializationService serializationService = createSerializationService(compactSerializationConfig);

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        long[] ids = new long[2];
        ids[0] = 22;
        ids[1] = 44;

        EmployeeDTO[] employeeDTOS = new EmployeeDTO[5];
        for (int j = 0; j < employeeDTOS.length; j++) {
            employeeDTOS[j] = new EmployeeDTO(20 + j, j * 100);
        }
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, HIRING, ids, employeeDTO, employeeDTOS);

        Data data = serializationService.toData(employerDTO);

        Object object = serializationService.toObject(data);
        EmployerDTO o = (EmployerDTO) object;
        assertEquals(employerDTO, o);
    }

    @Test
    public void testDefaultsReflection() {
        SerializationService serializationService = createSerializationService();
        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toData(employeeDTO);
        EmployeeDTO object = serializationService.toObject(data);
        assertEquals(employeeDTO, object);
    }

    @Test
    public void testWithExplicitSerializer() {
        SerializationService serializationService = createSerializationService(EmployeeDTOSerializer::new);

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toData(employeeDTO);

        Object object = serializationService.toObject(data);
        EmployeeDTO actual = (EmployeeDTO) object;

        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testGenericRecordHashcode_Equals() {
        SerializationService serializationService = createSerializationService();

        MainDTO expectedDTO = createMainDTO();
        GenericRecord expectedGenericRecord = createCompactGenericRecord(expectedDTO);

        Data data = serializationService.toData(expectedGenericRecord);

        Object object = serializationService.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;

        assertEquals(expectedGenericRecord, genericRecord);
        assertEquals(genericRecord, expectedGenericRecord);
        assertEquals(expectedGenericRecord.hashCode(), genericRecord.hashCode());
    }

    @Test
    public void testOverridesJavaSerializationWhenRegisteredAsReflectivelySerializable() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.addClass(ExternalizableEmployeeDTO.class);
        SerializationService serializationService = createSerializationService(compactSerializationConfig);

        ExternalizableEmployeeDTO employeeDTO = new ExternalizableEmployeeDTO(30, "John Doe");
        Data data = serializationService.toData(employeeDTO);
        assertFalse(employeeDTO.usedExternalizableSerialization());

        Object object = serializationService.toObject(data);
        ExternalizableEmployeeDTO actual = (ExternalizableEmployeeDTO) object;
        assertFalse(employeeDTO.usedExternalizableSerialization());

        assertEquals(employeeDTO, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompactInnerFieldCanNotOverrideDefaultSerializer() {
        testUsageOfCompactClassInnerFieldAsCustomSerializer(false);
    }

    @Test
    public void testCompactInnerFieldCanOverrideDefaultSerializer() {
        testUsageOfCompactClassInnerFieldAsCustomSerializer(true);
    }

    private void testUsageOfCompactClassInnerFieldAsCustomSerializer(boolean allowOverrideDefaultSerializers) {
        final SerializationConfig config = new SerializationConfig().setAllowOverrideDefaultSerializers(allowOverrideDefaultSerializers);

        config.getCompactSerializationConfig().addSerializer(new CustomUUIDSerializer());

        final SerializationService ss = createSerializationService(config);

        UUID innerField = UUID.randomUUID();
        CompactWithInnerFieldAsUuid compactWithUuidField = new CompactWithInnerFieldAsUuid(innerField);
        final Data d = ss.toData(compactWithUuidField);
        final CompactWithInnerFieldAsUuid deserializedAnswer = ss.toObject(d);

        assertEquals(compactWithUuidField, deserializedAnswer);
    }

    @Test
    public void testDeserializedToGenericRecordWhenClassNotFoundOnClassPath() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService = createSerializationService(EmployeeDTOSerializer::new, schemaService);

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toData(employeeDTO);

        SerializationService readerService = createSerializationService(schemaService);
        GenericRecord genericRecord = readerService.toObject(data);

        assertEquals(employeeDTO.getAge(), genericRecord.getInt32("age"));
        assertEquals(employeeDTO.getId(), genericRecord.getInt64("id"));
    }

    @Test
    public void testFieldOrder() {
        Schema schema = CompactTestUtil.getSchemasFor(EmployerDTO.class).iterator().next();

        assertEquals(0, schema.getField("zcode").getOffset());
        assertEquals(-1, schema.getField("zcode").getIndex());

        assertEquals(-1, schema.getField("hiringStatus").getOffset());
        assertEquals(0, schema.getField("hiringStatus").getIndex());

        assertEquals(-1, schema.getField("ids").getOffset());
        assertEquals(1, schema.getField("ids").getIndex());

        assertEquals(-1, schema.getField("name").getOffset());
        assertEquals(2, schema.getField("name").getIndex());

        assertEquals(-1, schema.getField("otherEmployees").getOffset());
        assertEquals(3, schema.getField("otherEmployees").getIndex());

        assertEquals(-1, schema.getField("singleEmployee").getOffset());
        assertEquals(4, schema.getField("singleEmployee").getIndex());
    }

    @Test
    public void testFieldOrderFixedSize() {
        Schema schema = CompactTestUtil.getSchemasFor(EmployeeDTO.class).iterator().next();

        assertEquals(0, schema.getField("id").getOffset());
        assertEquals(-1, schema.getField("id").getIndex());

        assertEquals(8, schema.getField("age").getOffset());
        assertEquals(-1, schema.getField("age").getIndex());

        assertEquals(12, schema.getField("rank").getOffset());
        assertEquals(-1, schema.getField("rank").getIndex());

        assertEquals(16, schema.getField("isFired").getOffset());
        assertEquals(0, schema.getField("isFired").getBitOffset());
        assertEquals(-1, schema.getField("isFired").getIndex());

        assertEquals(16, schema.getField("isHired").getOffset());
        assertEquals(1, schema.getField("isHired").getBitOffset());
        assertEquals(-1, schema.getField("isHired").getIndex());
    }

    @Test
    public void testSchemaEvolution_GenericRecord() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationService oldSerializationService = createSerializationService(schemaService);

        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        builder.setInt64("bar", 1231L);
        GenericRecord oldGenericRecord = builder.build();

        Data oldData = oldSerializationService.toData(oldGenericRecord);

        SerializationService newSerializationService = createSerializationService(schemaService);

        GenericRecordBuilder builder2 = compact("fooBarTypeName");
        builder2.setInt32("foo", 1);
        builder2.setInt64("bar", 1231L);
        builder2.setString("foobar", "new field");
        GenericRecord newGenericRecord = builder2.build();
        Data newData = newSerializationService.toData(newGenericRecord);

        // Newer client can read old data
        GenericRecord genericRecord = newSerializationService.toObject(oldData);

        assertEquals(FieldKind.NOT_AVAILABLE, genericRecord.getFieldKind("foobar"));
        assertEquals(1, genericRecord.getInt32("foo"));
        assertEquals(1231L, genericRecord.getInt64("bar"));

        // Older client can read newer data
        GenericRecord genericRecord2 = oldSerializationService.toObject(newData);

        assertEquals("new field", genericRecord2.getString("foobar"));
        assertEquals(1, genericRecord2.getInt32("foo"));
        assertEquals(1231L, genericRecord2.getInt64("bar"));
    }

    @Test
    public void testSchemaEvolution_variableSizeFieldAdded() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        //Using this type of serializer to mimic schema evolution. This is usage is not advised.
        CompactSerializer<EmployeeDTO> serializer = new CompactSerializer<EmployeeDTO>() {
            @Nonnull
            @Override
            public EmployeeDTO read(@Nonnull CompactReader reader) {

                int age = reader.readInt32("age");
                long id = reader.readInt64("id");
                String surname = "N/A";
                if (reader.getFieldKind("surname") == FieldKind.STRING) {
                    surname = reader.readString("surname");
                }
                return new EmployeeDTO(age, id);
            }

            @Override
            public void write(@Nonnull CompactWriter writer, @Nonnull EmployeeDTO object) {
                writer.writeInt32("age", object.getAge());
                writer.writeInt64("id", object.getId());
                writer.writeString("surname", "sir");
            }

            @Nonnull
            @Override
            public String getTypeName() {
                return "employee";
            }

            @Nonnull
            @Override
            public Class<EmployeeDTO> getCompactClass() {
                return EmployeeDTO.class;
            }
        };

        SerializationService newSerializationService = createSerializationService(() -> serializer, schemaService);

        EmployeeDTO expected = new EmployeeDTO(20, 102310312);
        Data data = newSerializationService.toData(expected);

        // Assert that older client can read newer data
        SerializationService oldSerializationService = createSerializationService(EmployeeDTOSerializer::new, schemaService);
        EmployeeDTO employee = oldSerializationService.toObject(data);

        assertEquals(expected.getAge(), employee.getAge());
        assertEquals(expected.getId(), employee.getId());

        // Assert that newer client can read older data
        Data data2 = oldSerializationService.toData(expected);
        EmployeeDTO employee2 = newSerializationService.toObject(data2);

        assertEquals(expected.getAge(), employee2.getAge());
        assertEquals(expected.getId(), employee2.getId());
    }

    @Test
    public void testSchemaEvolution_fixedSizeFieldAdded() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        //Using this type of serializer to mimic schema evolution. This is usage is not advised.
        CompactSerializer<EmployeeDTO> serializer = new CompactSerializer<EmployeeDTO>() {
            @Nonnull
            @Override
            public EmployeeDTO read(@Nonnull CompactReader in) {
                int age = in.readInt32("age");
                long id = in.readInt64("id");
                byte height;
                if (in.getFieldKind("height") == FieldKind.INT8) {
                    height = in.readInt8("height");
                }
                return new EmployeeDTO(age, id);
            }

            @Override
            public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) {
                out.writeInt32("age", object.getAge());
                out.writeInt64("id", object.getId());
                out.writeInt8("height", (byte) 187);
            }

            @Nonnull
            @Override
            public String getTypeName() {
                return "employee";
            }

            @Nonnull
            @Override
            public Class<EmployeeDTO> getCompactClass() {
                return EmployeeDTO.class;
            }
        };
        SerializationService newSerializationService = createSerializationService(() -> serializer, schemaService);

        EmployeeDTO expected = new EmployeeDTO(20, 102310312);
        Data data = newSerializationService.toData(expected);

        // Assert that older client can read newer data
        SerializationService oldSerializationService = createSerializationService(EmployeeDTOSerializer::new, schemaService);
        EmployeeDTO actual = oldSerializationService.toObject(data);

        assertEquals(expected.getAge(), actual.getAge());
        assertEquals(expected.getId(), actual.getId());

        // Assert that newer client can read older data
        Data data2 = oldSerializationService.toData(expected);
        EmployeeDTO employee2 = newSerializationService.toObject(data2);

        assertEquals(expected.getAge(), employee2.getAge());
        assertEquals(expected.getId(), employee2.getId());
    }

    @Test
    public void testSchemaEvolution_fixedSizeFieldRemoved() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        //Using this type of serializer to mimic schema evolution. This is usage is not advised.
        CompactSerializer<EmployeeDTO> newSerializer = new CompactSerializer<EmployeeDTO>() {
            @Nonnull
            @Override
            public EmployeeDTO read(@Nonnull CompactReader reader) {
                int age = reader.readInt32("age");
                return new EmployeeDTO(age, 0);
            }

            @Override
            public void write(@Nonnull CompactWriter writer, @Nonnull EmployeeDTO object) {
                writer.writeInt32("age", object.getAge());
            }

            @Nonnull
            @Override
            public String getTypeName() {
                return EmployeeDTO.class.getName();
            }

            @Nonnull
            @Override
            public Class<EmployeeDTO> getCompactClass() {
                return EmployeeDTO.class;
            }
        };

        CompactSerializer<EmployeeDTO> oldSerializer = new CompactSerializer<EmployeeDTO>() {
            @Nonnull
            @Override
            public EmployeeDTO read(@Nonnull CompactReader reader) {
                int age = reader.readInt32("age");
                long id = 0;
                if (reader.getFieldKind("id") == FieldKind.INT64) {
                    id = reader.readInt64("id");
                }
                return new EmployeeDTO(age, id);
            }

            @Override
            public void write(@Nonnull CompactWriter writer, @Nonnull EmployeeDTO object) {
                writer.writeInt32("age", object.getAge());
                writer.writeInt64("id", object.getId());
            }

            @Nonnull
            @Override
            public String getTypeName() {
                return EmployeeDTO.class.getName();
            }

            @Nonnull
            @Override
            public Class<EmployeeDTO> getCompactClass() {
                return EmployeeDTO.class;
            }
        };
        SerializationService newSerializationService = createSerializationService(() -> newSerializer, schemaService);
        SerializationService oldSerializationService = createSerializationService(() -> oldSerializer, schemaService);

        EmployeeDTO expected = new EmployeeDTO(20, 102310312);

        // Assert that older client can read newer data
        Data data = newSerializationService.toData(expected);
        EmployeeDTO actual = oldSerializationService.toObject(data);

        assertEquals(expected.getAge(), actual.getAge());
        assertEquals(0, actual.getId());

        // Assert that newer client can read older data
        Data data2 = oldSerializationService.toData(expected);
        EmployeeDTO employee2 = newSerializationService.toObject(data2);

        assertEquals(expected.getAge(), employee2.getAge());
        assertEquals(0, employee2.getId());
    }

    @Test
    public void testSchemaEvolution_variableSizeFieldRemoved() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        //Using this type of serializer to mimic schema evolution. This is usage is not advised.
        CompactSerializer<NodeDTO> newSerializer = new CompactSerializer<NodeDTO>() {
            @Nonnull
            @Override
            public NodeDTO read(@Nonnull CompactReader in) {
                int id = in.readInt32("id");
                return new NodeDTO(id);
            }

            @Override
            public void write(@Nonnull CompactWriter out, @Nonnull NodeDTO object) {
                out.writeInt32("id", object.getId());
            }

            @Nonnull
            @Override
            public String getTypeName() {
                return NodeDTO.class.getName();
            }

            @Nonnull
            @Override
            public Class<NodeDTO> getCompactClass() {
                return NodeDTO.class;
            }
        };

        CompactSerializer<NodeDTO> oldSerializer = new CompactSerializer<NodeDTO>() {
            @Nonnull
            @Override
            public NodeDTO read(@Nonnull CompactReader in) {
                int id = in.readInt32("id");
                NodeDTO child = null;
                if (in.getFieldKind("child") == FieldKind.COMPACT) {
                    child = in.readCompact("child");
                }
                return new NodeDTO(child, id);
            }

            @Override
            public void write(@Nonnull CompactWriter out, @Nonnull NodeDTO object) {
                out.writeInt32("id", object.getId());
                out.writeCompact("child", object.getChild());
            }

            @Nonnull
            @Override
            public String getTypeName() {
                return NodeDTO.class.getName();
            }

            @Nonnull
            @Override
            public Class<NodeDTO> getCompactClass() {
                return NodeDTO.class;
            }
        };

        SerializationService newSerializationService = createSerializationService(() -> newSerializer, schemaService);
        SerializationService oldSerializationService = createSerializationService(() -> oldSerializer, schemaService);

        NodeDTO expected = new NodeDTO(new NodeDTO(null, 1), 102310312);

        // Older client can read newer data
        Data newData = newSerializationService.toData(expected);
        NodeDTO actual = oldSerializationService.toObject(newData);

        assertEquals(expected.getId(), actual.getId());
        assertNull(actual.getChild());

        // Newer client can read older data
        Data oldData = oldSerializationService.toData(expected);
        NodeDTO actual2 = newSerializationService.toObject(oldData);

        assertEquals(expected.getId(), actual2.getId());
        assertNull(actual2.getChild());
    }

    private static class EmptyDTO {
        EmptyDTO() {
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        @Override
        public String toString() {
            return "EmptyDTO{}";
        }
    }

    private static class EmptyDTOSerializer implements CompactSerializer<EmptyDTO> {
        @Nonnull
        @Override
        public EmptyDTO read(@Nonnull CompactReader in) {
            return new EmptyDTO();
        }

        @Override
        public void write(@Nonnull CompactWriter out, @Nonnull EmptyDTO object) {
            // no-op
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "empty";
        }

        @Nonnull
        @Override
        public Class<EmptyDTO> getCompactClass() {
            return EmptyDTO.class;
        }
    }
}
