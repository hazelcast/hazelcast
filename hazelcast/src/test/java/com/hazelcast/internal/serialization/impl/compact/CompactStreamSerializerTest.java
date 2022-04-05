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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
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

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createCompactGenericRecord;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createMainDTO;
import static com.hazelcast.nio.serialization.GenericRecordBuilder.compact;
import static example.serialization.HiringStatus.HIRING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactStreamSerializerTest {

    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    @Test
    public void testAllTypesWithReflectiveSerializer() {
        SerializationService serializationService = createSerializationService();
        MainDTO expected = createMainDTO();

        Data data = serializationService.toData(expected);
        MainDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testAllTypesWithCustomSerializer() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.register(MainDTO.class, "main", new MainDTOSerializer());
        compactSerializationConfig.register(InnerDTO.class, "inner", new InnerDTOSerializer());
        compactSerializationConfig.setEnabled(true);
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
        MainDTO expected = createMainDTO();

        Data data = serializationService.toData(expected);
        MainDTO actual = serializationService.toObject(data);

        assertEquals(expected, actual);
    }

    @Test
    public void testReaderReturnsDefaultValues_whenDataIsMissing() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.register(MainDTO.class, "main", new MainDTOSerializer());
        compactSerializationConfig.register(InnerDTO.class, "inner", new InnerDTOSerializer());
        compactSerializationConfig.setEnabled(true);
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();

        Data data = serializationService.toData(GenericRecordBuilder.compact("main").build());
        MainDTO actual = serializationService.toObject(data);

        assertEquals(1, actual.b);
        assertEquals(false, actual.bool);
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

    private InternalSerializationService createSerializationService() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
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
        InternalSerializationService ss1 = createSerializationService();
        InternalSerializationService ss2 = createSerializationService();

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
        SerializationConfig serializationConfig = new SerializationConfig();
        CompactSerializationConfig compactSerializationConfig = serializationConfig.getCompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        compactSerializationConfig.register(EmployeeDTO.class, "employee",
                new CompactSerializer<EmployeeDTO>() {
                    @Nonnull
                    @Override
                    public EmployeeDTO read(@Nonnull CompactReader in) {
                        return new EmployeeDTO(in.readInt32("a"), in.readInt64("i"));
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) {
                        out.writeInt32("a", object.getAge());
                        out.writeInt64("i", object.getId());
                    }
                });
        compactSerializationConfig.register(EmployerDTO.class, "employer",
                new CompactSerializer<EmployerDTO>() {
                    @Nonnull
                    @Override
                    public EmployerDTO read(@Nonnull CompactReader in) {
                        String name = in.readString("n");
                        String status = in.readString("hs");
                        int age = in.readInt32("a");
                        long[] ids = in.readArrayOfInt64("ids");
                        EmployeeDTO s = in.readCompact("s");
                        EmployeeDTO[] ss = in.readArrayOfCompact("ss", EmployeeDTO.class);
                        return new EmployerDTO(name, age, status == null ? null : HiringStatus.valueOf(status), ids, s, ss);
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployerDTO object) {
                        out.writeString("n", object.getName());
                        out.writeString("hs", object.getHiringStatus() == null ? null : object.getHiringStatus().name());
                        out.writeInt32("a", object.getZcode());
                        out.writeArrayOfInt64("ids", object.getIds());
                        out.writeCompact("s", object.getSingleEmployee());
                        out.writeArrayOfCompact("ss", object.getOtherEmployees());
                    }
                });

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setConfig(serializationConfig)
                .setSchemaService(schemaService)
                .build();

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
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(EmployeeDTO.class, "employee", new EmployeeDTOSerializer());

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService).setConfig(serializationConfig).build();

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

        assertTrue(expectedGenericRecord.equals(genericRecord));
        assertTrue(genericRecord.equals(expectedGenericRecord));
        assertEquals(expectedGenericRecord.hashCode(), genericRecord.hashCode());
    }

    @Test
    public void testOverridesJavaSerializationWhenRegisteredAsReflectivelySerializable() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(ExternalizableEmployeeDTO.class);

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService).setConfig(serializationConfig).build();

        ExternalizableEmployeeDTO employeeDTO = new ExternalizableEmployeeDTO(30, "John Doe");
        Data data = serializationService.toData(employeeDTO);
        assertFalse(employeeDTO.usedExternalizableSerialization());

        Object object = serializationService.toObject(data);
        ExternalizableEmployeeDTO actual = (ExternalizableEmployeeDTO) object;
        assertFalse(employeeDTO.usedExternalizableSerialization());

        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testDeserializedToGenericRecordWhenClassNotFoundOnClassPath() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(EmployeeDTO.class, "employee", new EmployeeDTOSerializer());

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(serializationConfig)
                .build();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toData(employeeDTO);

        SerializationConfig serializationConfig2 = new SerializationConfig();
        serializationConfig2.getCompactSerializationConfig().setEnabled(true);
        SerializationService readerService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(serializationConfig2)
                .build();
        GenericRecord genericRecord = readerService.toObject(data);

        assertEquals(employeeDTO.getAge(), genericRecord.getInt32("age"));
        assertEquals(employeeDTO.getId(), genericRecord.getInt64("id"));
    }

    @Test
    public void testFieldOrder() throws IOException {
        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        long[] ids = new long[2];
        ids[0] = 22;
        ids[1] = 44;

        EmployeeDTO[] employeeDTOS = new EmployeeDTO[5];
        for (int j = 0; j < employeeDTOS.length; j++) {
            employeeDTOS[j] = new EmployeeDTO(20 + j, j * 100);
        }

        SchemaWriter writer = new SchemaWriter("typeName");

        ReflectiveCompactSerializer reflectiveCompactSerializer = new ReflectiveCompactSerializer();
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, HIRING, ids, employeeDTO, employeeDTOS);
        reflectiveCompactSerializer.write(writer, employerDTO);

        Schema schema = writer.build();

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
    public void testFieldOrderFixedSize() throws IOException {
        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);

        SchemaWriter writer = new SchemaWriter("typeName");

        ReflectiveCompactSerializer reflectiveCompactSerializer = new ReflectiveCompactSerializer();
        reflectiveCompactSerializer.write(writer, employeeDTO);

        Schema schema = writer.build();

        assertEquals(schema.getField("id").getOffset(), 0);
        assertEquals(schema.getField("id").getIndex(), -1);

        assertEquals(schema.getField("age").getOffset(), 8);
        assertEquals(schema.getField("age").getIndex(), -1);

        assertEquals(schema.getField("rank").getOffset(), 12);
        assertEquals(schema.getField("rank").getIndex(), -1);

        assertEquals(schema.getField("isFired").getOffset(), 16);
        assertEquals(schema.getField("isFired").getBitOffset(), 0);
        assertEquals(schema.getField("isFired").getIndex(), -1);

        assertEquals(schema.getField("isHired").getOffset(), 16);
        assertEquals(schema.getField("isHired").getBitOffset(), 1);
        assertEquals(schema.getField("isHired").getIndex(), -1);
    }

    @Test
    public void testSchemaEvolution_GenericRecord() {
        SerializationService serializationService = createSerializationService();

        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        builder.setInt64("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        SerializationService serializationService2 = createSerializationService();

        GenericRecordBuilder builder2 = compact("fooBarTypeName");
        builder2.setInt32("foo", 1);
        builder2.setInt64("bar", 1231L);
        builder2.setString("foobar", "new field");
        serializationService2.toData(builder2.build());

        Object object = serializationService2.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;

        assertFalse(genericRecord.hasField("foobar"));

        assertEquals(1, genericRecord.getInt32("foo"));
        assertEquals(1231L, genericRecord.getInt64("bar"));
    }

    @Test
    public void testSchemaEvolution_fieldAdded() {
        SerializationConfig serializationConfig = new SerializationConfig();
        //Using this registration to mimic schema evolution. This is usage is not advised.
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(EmployeeDTO.class, EmployeeDTO.class.getName(), new CompactSerializer<EmployeeDTO>() {
                    @Nonnull
                    @Override
                    public EmployeeDTO read(@Nonnull CompactReader in) {
                        throw new UnsupportedOperationException("We will not read from here on this test");
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) {
                        out.writeInt32("age", object.getAge());
                        out.writeInt64("id", object.getId());
                        out.writeString("surname", "sir");
                    }
                });

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setConfig(serializationConfig)
                .setSchemaService(schemaService)
                .build();

        EmployeeDTO expected = new EmployeeDTO(20, 102310312);
        Data data = serializationService.toData(expected);

        SerializationConfig serializationConfig2 = new SerializationConfig();
        serializationConfig2.getCompactSerializationConfig().setEnabled(true);
        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(serializationConfig2)
                .build();

        EmployeeDTO actual = serializationService2.toObject(data);

        assertEquals(expected.getAge(), actual.getAge());
        assertEquals(expected.getId(), actual.getId());
    }

    @Test
    public void testSchemaEvolution_fieldRemoved() {
        SerializationConfig serializationConfig = new SerializationConfig();
        //Using this registration to mimic schema evolution. This is usage is not advised.
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(EmployeeDTO.class, EmployeeDTO.class.getName(), new CompactSerializer<EmployeeDTO>() {
                    @Nonnull
                    @Override
                    public EmployeeDTO read(@Nonnull CompactReader in) {
                        throw new UnsupportedOperationException("We will not read from here on this test");
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) {
                        out.writeInt32("age", object.getAge());
                    }
                });

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setConfig(serializationConfig)
                .setSchemaService(schemaService)
                .build();


        EmployeeDTO expected = new EmployeeDTO(20, 102310312);
        Data data = serializationService.toData(expected);

        SerializationService serializationService2 = createSerializationService();

        EmployeeDTO actual = serializationService2.toObject(data);

        assertEquals(expected.getAge(), actual.getAge());
        assertEquals(0, actual.getId());
    }

}
