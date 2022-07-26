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
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.MainDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createCompactGenericRecord;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createMainDTO;
import static com.hazelcast.nio.serialization.GenericRecordBuilder.compact;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordTest {

    SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    @Test
    public void testGenericRecordToStringValidJson() throws IOException {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();

        MainDTO expectedDTO = createMainDTO();
        expectedDTO.nullableBool = null;
        expectedDTO.p.localDateTimes[0] = null;
        Data data = serializationService.toData(expectedDTO);
        assertTrue(data.isCompact());

        //internal generic record created on the servers on query
        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);
        String string = internalGenericRecord.toString();
        Json.parse(string);

        //generic record read from a remote instance without classes on the classpath
        List<String> excludes = Collections.singletonList("example.serialization");
        FilteringClassLoader classLoader = new FilteringClassLoader(excludes, null);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            InternalSerializationService ss2 = new DefaultSerializationServiceBuilder()
                    .setSchemaService(schemaService)
                    .setClassLoader(classLoader)
                    .setConfig(new SerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig()))
                    .build();
            GenericRecord genericRecord = ss2.toObject(data);
            Json.parse(genericRecord.toString());

            //generic record build by API
            GenericRecord apiGenericRecord = createCompactGenericRecord(expectedDTO);
            Json.parse(apiGenericRecord.toString());
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Test
    public void testCloneDeserializedGenericRecord() {
        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        DeserializedGenericRecord genericRecord = (DeserializedGenericRecord) builder.build();

        verifyNewBuilderWithClone(genericRecord);
    }

    @Test
    public void testCloneCompactInternalGenericRecord() throws IOException {
        InternalSerializationService serializationService = (InternalSerializationService) createSerializationService();

        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);
        CompactInternalGenericRecord genericRecord = (CompactInternalGenericRecord)
                serializationService.readAsInternalGenericRecord(data);

        verifyNewBuilderWithClone(genericRecord);
    }

    private void verifyNewBuilderWithClone(GenericRecord genericRecord) {
        GenericRecordBuilder cloneBuilder = genericRecord.newBuilderWithClone();
        cloneBuilder.setInt32("foo", 2);

        assertSetterThrows(cloneBuilder, "foo", 5, "Field can only be written once");
        assertSetterThrows(cloneBuilder, "notExisting", 3, "Invalid field name");

        GenericRecord clone = cloneBuilder.build();

        assertEquals(2, clone.getInt32("foo"));
        assertEquals(1231L, clone.getInt64("bar"));
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
    public void testBuildFromCompactInternalGenericRecord() throws IOException {
        InternalSerializationService serializationService = (InternalSerializationService) createSerializationService();

        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        CompactInternalGenericRecord genericRecord = (CompactInternalGenericRecord)
                serializationService.readAsInternalGenericRecord(data);

        verifyNewBuilder(genericRecord);
    }

    private void verifyNewBuilder(GenericRecord genericRecord) {
        GenericRecordBuilder recordBuilder = genericRecord.newBuilder();
        recordBuilder.setInt32("foo", 2);

        assertSetterThrows(recordBuilder, "foo", 5, "Field can only be written once");
        assertSetterThrows(recordBuilder, "notExisting", 3, "Invalid field name");

        assertThatThrownBy(recordBuilder::build)
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageStartingWith("Found an unset field");

        recordBuilder.setInt64("bar", 100);
        GenericRecord newRecord = recordBuilder.build();

        assertEquals(2, newRecord.getInt32("foo"));
        assertEquals(100, newRecord.getInt64("bar"));
    }

    @Test
    public void testBuildFromDeserializedGenericRecord() {
        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        GenericRecord genericRecord = builder.build();

        verifyNewBuilder(genericRecord);
    }

    @Test
    public void testReadWriteChar() {
        assertThatThrownBy(() -> compact("writeChar").setChar("c", 'a'))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> {
            GenericRecord record = compact("readChar").build();
            record.getChar("c");
        }).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testReadWriteCharArray() {
        assertThatThrownBy(() -> compact("writeCharArray").setArrayOfChar("ca", new char[]{'c'}))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> {
            GenericRecord record = compact("readCharArray").build();
            record.getArrayOfChar("ca");
        }).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testGetFieldKindThrowsExceptionWhenFieldDoesNotExist() throws IOException {
        GenericRecord record = compact("test").build();
        assertThatThrownBy(() -> record.getFieldKind("doesNotExist"))
                .isInstanceOf(IllegalArgumentException.class);

        InternalSerializationService serializationService = (InternalSerializationService) createSerializationService();
        Data data = serializationService.toData(record);

        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);
        assertThatThrownBy(() -> internalGenericRecord.getFieldKind("doesNotExist"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void assertSetterThrows(GenericRecordBuilder builder, String fieldName, int value, String errorMessage) {
        assertThatThrownBy(() -> builder.setInt32(fieldName, value))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageStartingWith(errorMessage);
    }
}
