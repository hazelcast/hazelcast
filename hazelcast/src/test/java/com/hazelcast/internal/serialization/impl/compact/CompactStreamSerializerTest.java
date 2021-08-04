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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.nio.serialization.AbstractGenericRecord;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import example.serialization.BitsDTO;
import example.serialization.EmployeeDTO;
import example.serialization.EmployeeWithSerializerDTO;
import example.serialization.EmployerDTO;
import example.serialization.NodeDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import static com.hazelcast.nio.serialization.GenericRecordBuilder.compact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class CompactStreamSerializerTest {

    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

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
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, ids, employeeDTO, employeeDTOS);

        ArrayList<Object> expected = new ArrayList<>();
        expected.add(node);
        expected.add(employerDTO);

        Data data = serializationService.toData(expected);
        ArrayList<Object> arrayList = serializationService.toObject(data);
        assertEquals(node, arrayList.get(0));
        assertEquals(employerDTO, arrayList.get(1));
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
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, ids, employeeDTO, employeeDTOS);

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
                        return new EmployeeDTO(in.readInt("a"), in.readLong("i"));
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) {
                        out.writeInt("a", object.getAge());
                        out.writeLong("i", object.getId());
                    }
                });
        compactSerializationConfig.register(EmployerDTO.class, "employer",
                new CompactSerializer<EmployerDTO>() {
                    @Nonnull
                    @Override
                    public EmployerDTO read(@Nonnull CompactReader in) {
                        String name = in.readString("n");
                        int age = in.readInt("a");
                        long[] ids = in.readLongArray("ids");
                        EmployeeDTO s = in.readObject("s");
                        EmployeeDTO[] ss = in.readObjectArray("ss", EmployeeDTO.class);
                        return new EmployerDTO(name, age, ids, s, ss);
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployerDTO object) {
                        out.writeString("n", object.getName());
                        out.writeInt("a", object.getZcode());
                        out.writeLongArray("ids", object.getIds());
                        out.writeObject("s", object.getSingleEmployee());
                        out.writeObjectArray("ss", object.getOtherEmployees());
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
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, ids, employeeDTO, employeeDTOS);

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
                .register(EmployeeDTO.class, "employee",
                        new CompactSerializer<EmployeeDTO>() {
                            @Nonnull
                            @Override
                            public EmployeeDTO read(@Nonnull CompactReader in) {
                                return new EmployeeDTO(in.readInt("a"), in.readLong("i"));
                            }

                            @Override
                            public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) {
                                out.writeInt("a", object.getAge());
                                out.writeLong("i", object.getId());
                            }
                        });

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService).setConfig(serializationConfig).build();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toData(employeeDTO);

        Object object = serializationService.toObject(data);
        EmployeeDTO actual = (EmployeeDTO) object;

        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testWithExplicitSerializerViaCompactable() {
        SerializationService serializationService = createSerializationService();

        EmployeeWithSerializerDTO employeeDTO = new EmployeeWithSerializerDTO(30, 102310312);
        Data data = serializationService.toData(employeeDTO);

        Object object = serializationService.toObject(data);
        EmployeeWithSerializerDTO actual = (EmployeeWithSerializerDTO) object;

        assertEquals(employeeDTO, actual);

        //create a second service and make sure that class can not be loaded.
        //We are simulating a separate jvm where `EmployeeWithSerializerDTO` does not exist in the classpath.
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader().getParent();
        try {
            Thread.currentThread().setContextClassLoader(parentClassLoader);
            SerializationConfig serializationConfig = new SerializationConfig();
            serializationConfig.setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));
            SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                    .setSchemaService(schemaService)
                    .setClassLoader(parentClassLoader)
                    .setConfig(serializationConfig)
                    .build();
            GenericRecord genericRecord = serializationService2.toObject(data);
            //testing the field names introduced by the Serializer in EmployeeWithSerializerDTO, not reflection
            assertEquals(30, genericRecord.getInt("a"));
            assertEquals(102310312, genericRecord.getLong("i"));
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
    }

    @Test
    public void testGenericRecordHashcode_Equals() {
        SerializationService serializationService = createSerializationService();

        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        builder.setLong("bar", 1231L);
        builder.setLongArray("barArray", new long[]{1L, 2L});
        builder.setDecimal("dec", new BigDecimal(12131321));
        builder.setGenericRecord("nestedField",
                compact("nested").setInt("a", 2).build());
        builder.setGenericRecordArray("nestedFieldArray", new GenericRecord[]{
                compact("nested").setInt("a", 2).build(),
                compact("nested").setInt("a", 3).build(),
        });
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        Object object = serializationService.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;
        AbstractGenericRecord abstractGenericRecord = (AbstractGenericRecord) object;
        abstractGenericRecord.readAny("bar");

        assertTrue(expectedGenericRecord.equals(genericRecord));
        assertTrue(genericRecord.equals(expectedGenericRecord));
        assertEquals(expectedGenericRecord.hashCode(), genericRecord.hashCode());
    }

    @Test
    public void testOverridenClassNameWithAlias() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(EmployeeDTO.class, "employee");

        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService).setConfig(serializationConfig).build();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toData(employeeDTO);

        Object object = serializationService.toObject(data);
        EmployeeDTO actual = (EmployeeDTO) object;

        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testDeserializedToGenericRecordWhenClassNotFoundOnClassPath() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(EmployeeDTO.class, "employee");

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

        assertEquals(employeeDTO.getAge(), genericRecord.getInt("age"));
        assertEquals(employeeDTO.getId(), genericRecord.getLong("id"));
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

        SchemaWriter writer = new SchemaWriter("className");

        ReflectiveCompactSerializer reflectiveCompactSerializer = new ReflectiveCompactSerializer();
        EmployerDTO employerDTO = new EmployerDTO("nbss", 40, ids, employeeDTO, employeeDTOS);
        reflectiveCompactSerializer.write(writer, employerDTO);

        Schema schema = writer.build();

        assertEquals(schema.getField("zcode").getOffset(), 0);
        assertEquals(schema.getField("zcode").getIndex(), -1);

        assertEquals(schema.getField("ids").getOffset(), -1);
        assertEquals(schema.getField("ids").getIndex(), 0);

        assertEquals(schema.getField("name").getOffset(), -1);
        assertEquals(schema.getField("name").getIndex(), 1);

        assertEquals(schema.getField("otherEmployees").getOffset(), -1);
        assertEquals(schema.getField("otherEmployees").getIndex(), 2);

        assertEquals(schema.getField("singleEmployee").getOffset(), -1);
        assertEquals(schema.getField("singleEmployee").getIndex(), 3);
    }

    @Test
    public void testFieldOrderFixedSize() throws IOException {
        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);

        SchemaWriter writer = new SchemaWriter("className");

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

        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        builder.setLong("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        SerializationService serializationService2 = createSerializationService();

        GenericRecordBuilder builder2 = compact("fooBarClassName");
        builder2.setInt("foo", 1);
        builder2.setLong("bar", 1231L);
        builder2.setString("foobar", "new field");
        serializationService2.toData(builder2.build());

        Object object = serializationService2.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;

        assertFalse(genericRecord.hasField("foobar"));

        assertEquals(1, genericRecord.getInt("foo"));
        assertEquals(1231L, genericRecord.getLong("bar"));
    }

    @Test
    public void testSchemaEvolution_fieldAdded() {
        SerializationConfig serializationConfig = new SerializationConfig();
        //Using this registration to mimic schema evolution. This is usage is not advised.
        serializationConfig.getCompactSerializationConfig().setEnabled(true)
                .register(EmployeeDTO.class, new CompactSerializer<EmployeeDTO>() {
                    @Nonnull
                    @Override
                    public EmployeeDTO read(@Nonnull CompactReader in) throws IOException {
                        throw new UnsupportedOperationException("We will not read from here on this test");
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) throws IOException {
                        out.writeInt("age", object.getAge());
                        out.writeLong("id", object.getId());
                        out.writeString("surname", "sir");
                    }
                });

        SerializationService serializationService = createSerializationService();

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
                .register(EmployeeDTO.class, new CompactSerializer<EmployeeDTO>() {
                    @Nonnull
                    @Override
                    public EmployeeDTO read(@Nonnull CompactReader in) throws IOException {
                        throw new UnsupportedOperationException("We will not read from here on this test");
                    }

                    @Override
                    public void write(@Nonnull CompactWriter out, @Nonnull EmployeeDTO object) throws IOException {
                        out.writeInt("age", object.getAge());
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
