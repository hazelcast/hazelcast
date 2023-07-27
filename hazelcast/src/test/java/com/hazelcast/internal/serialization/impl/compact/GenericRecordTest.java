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
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createCompactGenericRecord;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createInMemorySchemaService;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createMainDTO;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.compact;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordTest {

    @Test
    public void testGenericRecordToStringValidJson() throws IOException {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        InternalSerializationService serializationService = (InternalSerializationService)
                createSerializationService(schemaService);

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
    public void testChangingClonedRecordDoesNotChangeOriginal() {
        GenericRecordBuilder builder = compact("foo");
        GenericRecord record = builder.setString("str" , "hello").build();

        GenericRecordBuilder builder2 = record.newBuilderWithClone();
        builder2.setString("str", "aa");

        assertEquals("hello", record.getString("str"));
    }

    @Test
    public void testCloneCompactInternalGenericRecord() throws IOException {
        InternalSerializationService serializationService = createSerializationService();

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
    public void testGetFieldKind() throws IOException {
        GenericRecord record = compact("test")
                .setString("s", "s")
                .build();

        assertEquals(FieldKind.STRING, record.getFieldKind("s"));
        assertEquals(FieldKind.NOT_AVAILABLE, record.getFieldKind("ss"));

        SerializationService service = createSerializationService();
        Data data = service.toData(record);
        InternalGenericRecord internalGenericRecord
                = ((InternalSerializationService) service).readAsInternalGenericRecord(data);

        assertEquals(FieldKind.STRING, internalGenericRecord.getFieldKind("s"));
        assertEquals(FieldKind.NOT_AVAILABLE, internalGenericRecord.getFieldKind("ss"));
    }

    private void assertSetterThrows(GenericRecordBuilder builder, String fieldName, int value, String errorMessage) {
        assertThatThrownBy(() -> builder.setInt32(fieldName, value))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageStartingWith(errorMessage);
    }

    @Test
    public void testGetFieldThrowsExceptionWhenFieldDoesNotExist() throws IOException {
        GenericRecord record = compact("test").build();
        assertThatThrownBy(() -> {
            record.getInt32("doesNotExist");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field name");

        InternalSerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);

        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);

        assertThatThrownBy(() -> {
            internalGenericRecord.getInt32("doesNotExist");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field name");
    }

    @Test
    public void testGetFieldThrowsExceptionWhenFieldTypeDoesNotMatch() throws IOException {
        GenericRecord record = compact("test").setInt32("foo", 123).build();
        assertThatThrownBy(() -> {
            record.getInt64("foo");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field kind");

        InternalSerializationService serializationService = createSerializationService();
        Data data = serializationService.toData(record);

        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);

        assertThatThrownBy(() -> {
            internalGenericRecord.getInt64("foo");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field kind");
    }

    @Test
    public void testReadingGenericRecord_classLoaderShouldBeInvokedOncePerTypeName() {
        SchemaService schemaService = createInMemorySchemaService();
        InternalSerializationService ss1 = createSerializationService(schemaService);
        GenericRecord record = compact("test").setInt32("foo", 123).build();
        Data recordData = ss1.toData(record);

        CountingClassLoader classLoader = new CountingClassLoader("test");
        InternalSerializationService ss2 = createSerializationService(classLoader, schemaService);
        assertEquals(0, classLoader.getCount());
        assertEquals(record, ss2.toObject(recordData));
        assertEquals(record, ss2.toObject(recordData));
        assertEquals(record, ss2.toObject(recordData));
        assertEquals(1, classLoader.getCount());
    }

    private static final class CountingClassLoader extends ClassLoader {
        private final String classNameToCount;
        private final AtomicInteger count;

        CountingClassLoader(String classNameToCount) {
            this.classNameToCount = classNameToCount;
            this.count = new AtomicInteger();
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (classNameToCount.equals(name)) {
                count.incrementAndGet();
            }
            return super.loadClass(name);
        }

        public int getCount() {
            return count.get();
        }
    }
}
