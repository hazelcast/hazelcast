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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.serialization.GenericRecordBuilder.compact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class GenericRecordTest {

    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    @Test
    public void testCloneObjectConvertedFromData() {
        SerializationService serializationService = createSerializationService();

        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        Object object = serializationService.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;

        GenericRecordBuilder cloneBuilder = genericRecord.cloneWithBuilder();
        cloneBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, cloneBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, cloneBuilder).startsWith("Invalid field name"));

        GenericRecord clone = cloneBuilder.build();

        assertEquals(2, clone.getInt("foo"));
        assertEquals(1231L, clone.getLong("bar"));
    }

    private SerializationService createSerializationService() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    @Test
    public void testCloneObjectCreatedViaAPI() {
        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord genericRecord = builder.build();

        GenericRecordBuilder cloneBuilder = genericRecord.cloneWithBuilder();
        cloneBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, cloneBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, cloneBuilder).startsWith("Invalid field name"));

        GenericRecord clone = cloneBuilder.build();

        assertEquals(2, clone.getInt("foo"));
        assertEquals(1231L, clone.getLong("bar"));
    }

    @Test
    public void testBuildFromObjectConvertedFromData() {
        SerializationService serializationService = createSerializationService();

        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        Object object = serializationService.toObject(data);
        GenericRecord genericRecord = (GenericRecord) object;

        GenericRecordBuilder recordBuilder = genericRecord.newBuilder();
        recordBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, recordBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, recordBuilder).startsWith("Invalid field name"));
        assertTrue(tryBuildAndGetMessage(recordBuilder).startsWith("Found an unset field"));

        recordBuilder.setLong("bar", 100);
        GenericRecord newRecord = recordBuilder.build();

        assertEquals(2, newRecord.getInt("foo"));
        assertEquals(100, newRecord.getLong("bar"));
    }

    @Test
    public void testBuildFromObjectCreatedViaAPI() {
        GenericRecordBuilder builder = compact("fooBarClassName");
        builder.setInt("foo", 1);
        assertTrue(trySetAndGetMessage("foo", 5, builder).startsWith("Field can only be written once"));
        builder.setLong("bar", 1231L);
        GenericRecord genericRecord = builder.build();

        GenericRecordBuilder recordBuilder = genericRecord.newBuilder();
        recordBuilder.setInt("foo", 2);
        assertTrue(trySetAndGetMessage("foo", 5, recordBuilder).startsWith("Field can only be written once"));

        assertTrue(trySetAndGetMessage("notExisting", 3, recordBuilder).startsWith("Invalid field name"));
        assertTrue(tryBuildAndGetMessage(recordBuilder).startsWith("Found an unset field"));

        recordBuilder.setLong("bar", 100);
        GenericRecord newRecord = recordBuilder.build();

        assertEquals(2, newRecord.getInt("foo"));
        assertEquals(100, newRecord.getLong("bar"));
    }

    private String trySetAndGetMessage(String fieldName, int value, GenericRecordBuilder cloneBuilder) {
        try {
            cloneBuilder.setInt(fieldName, value);
        } catch (HazelcastSerializationException e) {
            return e.getMessage();
        }
        return null;
    }

    private String tryBuildAndGetMessage(GenericRecordBuilder builder) {
        try {
            builder.build();
        } catch (HazelcastSerializationException e) {
            return e.getMessage();
        }
        return null;
    }
}
