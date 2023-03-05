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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.NodeDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteOrder;

import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.compact;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactWithSchemaStreamSerializerTest {

    @Parameterized.Parameter
    public ByteOrder byteOrder;

    @Parameterized.Parameters(name = "byteOrder:{0}")
    public static Object[] parameters() {
        return new Object[]{ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN};
    }

    @Test
    public void testReadAsGenericRecord() throws IOException {
        SerializationService serializationService = createSerializationService();

        GenericRecord expected = compact("fooBarTypeName")
                .setInt32("foo", 1)
                .setInt64("bar", 1231L)
                .setGenericRecord("nested",
                        compact("nested").setBoolean("bool", true).build())
                .build();

        Data data = serializationService.toDataWithSchema(expected);

        // Create a second schema service so that schemas are not shared across these two
        // This is to make sure that toObject call will use the schema in the data
        SerializationService serializationService2 = createSerializationService();

        GenericRecord actual = serializationService2.toObject(data);
        assertEquals(expected, actual);
    }

    @Test
    public void testFromGenericRecord() {
        SerializationService serializationService = createSerializationService();

        GenericRecord expected = compact("fooBarTypeName")
                .setInt32("foo", 1)
                .setInt64("bar", 1231L)
                .setGenericRecord("nested",
                        compact("nested").setBoolean("bool", true).build())
                .build();

        Data data = serializationService.toDataWithSchema(expected);

        // Create a second schema service so that schemas are not shared across these two
        // This is to make sure that toObject call will use the schema in the data
        SerializationService serializationService2 = createSerializationService();

        GenericRecord actual = serializationService2.toObject(data);
        assertEquals(expected, actual);
    }

    @Test
    public void testFromObject() {
        SerializationService serializationService = createSerializationService();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data data = serializationService.toDataWithSchema(employeeDTO);

        // Create a second schema service so that schemas are not shared accross these two
        // This is to make sure that toObject call will use the schema in the data
        SerializationService serializationService2 = createSerializationService();
        EmployeeDTO actual = serializationService2.toObject(data);
        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testFromData() {
        SerializationService serializationService = createSerializationService();

        EmployeeDTO employeeDTO = new EmployeeDTO(30, 102310312);
        Data employeeData = serializationService.toDataWithSchema(employeeDTO);

        Data data = serializationService.toDataWithSchema(employeeData);

        // Create a second schema service so that schemas are not shared accross these two
        // This is to make sure that toObject call will use the schema in the data
        SerializationService serializationService2 = createSerializationService();
        EmployeeDTO actual = serializationService2.toObject(data);
        assertEquals(employeeDTO, actual);
    }

    @Test
    public void testRecursive() {
        SerializationService serializationService = createSerializationService();

        NodeDTO expected = new NodeDTO(new NodeDTO(new NodeDTO(2), 1), 0);

        Data data = serializationService.toDataWithSchema(expected);

        // Create a second schema service so that schemas are not shared accross these two
        // This is to make sure that toObject call will use the schema in the data
        SerializationService serializationService2 = createSerializationService();
        NodeDTO actual = serializationService2.toObject(data);
        assertEquals(expected, actual);
    }

    private SerializationService createSerializationService() {
        SerializationConfig config = new SerializationConfig();
        config.setByteOrder(byteOrder);
        return CompactTestUtil.createSerializationService(config);
    }
}
