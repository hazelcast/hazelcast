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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.NodeDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.nio.serialization.GenericRecordBuilder.compact;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactWithSchemaStreamSerializerTest {

    @Test
    public void testReadAsGenericRecord() throws IOException {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .build();

        GenericRecord expected = compact("fooBarClassName")
                .setInt("foo", 1)
                .setLong("bar", 1231L)
                .setGenericRecord("nested",
                        compact("nested").setBoolean("bool", true).build())
                .build();

        Data data = serializationService.toDataWithSchema(expected);

        // Create a second schema service so that schemas are not shared accross these two
        // This is to make sure that toObject call will use the schema in the data
        SchemaService schemaService2 = CompactTestUtil.createInMemorySchemaService();
        InternalSerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService2)
                .build();

        GenericRecord actual = serializationService2.readAsInternalGenericRecord(data);
        assertEquals(expected, actual);
    }

    @Test
    public void testFromGenericRecord() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .build();

        GenericRecord expected = compact("fooBarClassName")
                .setInt("foo", 1)
                .setLong("bar", 1231L)
                .setGenericRecord("nested",
                        compact("nested").setBoolean("bool", true).build())
                .build();

        Data data = serializationService.toDataWithSchema(expected);

        // Create a second schema service so that schemas are not shared across these two
        // This is to make sure that toObject call will use the schema in the data
        SchemaService schemaService2 = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService2)
                .build();

        GenericRecord actual = serializationService2.toObject(data);
        assertEquals(expected, actual);
    }

    @Test
    public void testFromObject() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationConfig serializationConfig = new SerializationConfig();
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService).setConfig(serializationConfig).build();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toDataWithSchema(employeeDTO);

        // Create a second schema service so that schemas are not shared accross these two
        // This is to make sure that toObject call will use the schema in the data
        SchemaService schemaService2 = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService2).build();
        EmployeeDTO actual = serializationService2.toObject(data);
        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testFromData() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationConfig serializationConfig = new SerializationConfig();
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService).setConfig(serializationConfig).build();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data employeeData = serializationService.toDataWithSchema(employeeDTO);

        Data data = serializationService.toDataWithSchema(employeeData);

        // Create a second schema service so that schemas are not shared accross these two
        // This is to make sure that toObject call will use the schema in the data
        SchemaService schemaService2 = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService2).build();
        EmployeeDTO actual = serializationService2.toObject(data);
        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testRecursive() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService).build();

        NodeDTO expected = new NodeDTO(new NodeDTO(new NodeDTO(2), 1), 0);

        Data data = serializationService.toDataWithSchema(expected);

        // Create a second schema service so that schemas are not shared accross these two
        // This is to make sure that toObject call will use the schema in the data
        SchemaService schemaService2 = CompactTestUtil.createInMemorySchemaService();
        SerializationService serializationService2 = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService2).build();
        NodeDTO actual = serializationService2.toObject(data);
        assertEquals(expected, actual);
    }
}
