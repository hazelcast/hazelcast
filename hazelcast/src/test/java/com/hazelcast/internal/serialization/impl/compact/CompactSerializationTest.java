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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.EmployeeDTOSerializer;
import example.serialization.ExternalizableEmployeeDTO;
import example.serialization.InnerDTOSerializer;
import example.serialization.MainDTO;
import example.serialization.MainDTOSerializer;
import example.serialization.SameClassEmployeeDTOSerializer;
import example.serialization.SameTypeNameEmployeeDTOSerializer;
import example.serialization.SerializableEmployeeDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.OptionalDouble;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSerializationTest {

    private final SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();

    @Test
    public void testOverridingDefaultSerializers() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .addSerializer(new IntegerSerializer());

        assertThatThrownBy(() -> createSerializationService(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowOverrideDefaultSerializers");
    }

    @Test
    public void testOverridingDefaultSerializers_withAllowOverrideDefaultSerializers() {
        SerializationConfig config = new SerializationConfig();
        config.setAllowOverrideDefaultSerializers(true);
        config.getCompactSerializationConfig()
                .addSerializer(new IntegerSerializer());

        SerializationService service = createSerializationService(config);
        Data data = service.toData(42);

        assertTrue(data.isCompact());

        int i = service.toObject(data);

        assertEquals(42, i);
    }

    @Test
    public void testSerializer_withDuplicateFieldNames() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .addSerializer(new DuplicateWritingSerializer());

        SerializationService service = createSerializationService(config);

        Foo foo = new Foo(42);

        assertThatThrownBy(() -> service.toData(foo))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Failed to serialize")
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasRootCauseMessage("Field with the name 'bar' already exists");
    }

    @Test
    public void testSerializerRegistration_withDuplicateClasses() {
        SerializationConfig config = new SerializationConfig();
        assertThatThrownBy(() -> {
            config.getCompactSerializationConfig()
                    .addSerializer(new EmployeeDTOSerializer())
                    .addSerializer(new SameClassEmployeeDTOSerializer());
        }).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("class");
    }

    @Test
    public void testSerializerRegistration_withDuplicateTypeNames() {
        SerializationConfig config = new SerializationConfig();

        assertThatThrownBy(() -> {
            config.getCompactSerializationConfig()
                    .addSerializer(new EmployeeDTOSerializer())
                    .addSerializer(new SameTypeNameEmployeeDTOSerializer());
        }).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("type name");
    }

    @Test
    public void testSetSerializers() {
        SerializationConfig config = new SerializationConfig();

        InnerDTOSerializer innerDTOSerializer = spy(new InnerDTOSerializer());
        MainDTOSerializer mainDTOSerializer = spy(new MainDTOSerializer());

        config.getCompactSerializationConfig()
                .setSerializers(innerDTOSerializer, mainDTOSerializer);

        SerializationService service = createSerializationService(config);

        MainDTO mainDTO = CompactTestUtil.createMainDTO();
        MainDTO deserialized = service.toObject(service.toData(mainDTO));

        assertEquals(mainDTO, deserialized);
        verify(innerDTOSerializer, times(1)).read(any());
        // one to build schema, one to write object
        verify(innerDTOSerializer, times(2)).write(any(), any());
        verify(mainDTOSerializer, times(1)).read(any());
        // one to build schema, one to write object
        verify(mainDTOSerializer, times(2)).write(any(), any());
    }

    @Test
    public void testSetSerializers_withDuplicateClasses() {
        SerializationConfig config = new SerializationConfig();

        assertThatThrownBy(() -> {
            config.getCompactSerializationConfig()
                    .setSerializers(new EmployeeDTOSerializer(), new SameClassEmployeeDTOSerializer());
        }).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("class");
    }

    @Test
    public void testSetSerializers_withDuplicateTypeNames() {
        SerializationConfig config = new SerializationConfig();

        assertThatThrownBy(() -> {
            config.getCompactSerializationConfig()
                    .setSerializers(new EmployeeDTOSerializer(), new SameTypeNameEmployeeDTOSerializer());
        }).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("type name");
    }

    @Test
    public void testSetSerializers_clearPreviousRegistrations() {
        SerializationConfig config = new SerializationConfig();

        EmployeeDTOSerializer serializer = spy(new EmployeeDTOSerializer());
        config.getCompactSerializationConfig()
                .addSerializer(serializer)
                .setSerializers(new InnerDTOSerializer(), new MainDTOSerializer());

        SerializationService service = createSerializationService(config);
        EmployeeDTO employeeDTO = new EmployeeDTO();
        service.toObject(service.toData(employeeDTO));

        // The previously added serializer should be cleared and not used
        verify(serializer, never()).read(any());
        verify(serializer, never()).write(any(), any());
    }

    @Test
    public void testSetClasses() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .setClasses(ExternalizableEmployeeDTO.class, SerializableEmployeeDTO.class);

        SerializationService service = createSerializationService(config);

        ExternalizableEmployeeDTO externalizableEmployeeDTO = new ExternalizableEmployeeDTO();
        service.toObject(service.toData(externalizableEmployeeDTO));

        assertFalse(externalizableEmployeeDTO.usedExternalizableSerialization());

        Data data = service.toData(new SerializableEmployeeDTO("John Doe", 42));
        assertTrue(data.isCompact());
    }

    @Test
    public void testSetClasses_withDuplicateClasses() {
        SerializationConfig config = new SerializationConfig();

        assertThatThrownBy(() -> {
            config.getCompactSerializationConfig()
                    .setClasses(ExternalizableEmployeeDTO.class, ExternalizableEmployeeDTO.class);
        }).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("type name");
    }

    @Test
    public void testSetSerializers_whenThereAreClassRegistrations() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig()
                .setClasses(SerializableEmployeeDTO.class)
                .setSerializers(new EmployeeDTOSerializer());

        SerializationService service = createSerializationService(config);
        Data data = service.toData(new SerializableEmployeeDTO("John Doe", 42));
        assertTrue(data.isCompact());
    }

    @Test
    public void testSetClasses_whenThereAreSerializerRegistrations() {
        SerializationConfig config = new SerializationConfig();
        EmployeeDTOSerializer serializer = spy(new EmployeeDTOSerializer());
        config.getCompactSerializationConfig()
                .setSerializers(serializer)
                .setClasses(SerializableEmployeeDTO.class);

        SerializationService service = createSerializationService(config);
        service.toObject(service.toData(new EmployeeDTO()));

        verify(serializer, times(1)).read(any());
        verify(serializer, times(2)).write(any(), any());
    }

    @Test
    public void testSerializingClassReflectively_whenTheClassIsNotSupported() {
        SerializationService service = createSerializationService(new SerializationConfig());

        // OptionalDouble is a class that does not implement the Serializable interface
        // from the java.util package, which is not allowed to be serialized
        // by zero-config serializer.
        assertThatThrownBy(() -> {
            service.toData(OptionalDouble.of(1));
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("If you want to serialize this class");
    }

    @Test
    public void testSerializingClassReflectively_withUnsupportedFieldType() {
        SerializationService service = createSerializationService(new SerializationConfig());
        assertThatThrownBy(() -> {
            service.toData(new ClassWithUnsupportedField());
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("which uses this class in its fields");
    }

    @Test
    public void testSerializingClassReflectively_withUnsupportedArrayItemType() {
        SerializationService service = createSerializationService(new SerializationConfig());
        assertThatThrownBy(() -> {
            service.toData(new ClassWithUnsupportedArrayField());
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("which uses this class in its fields");
    }

    @Test
    public void testWritingArrayOfCompactGenericRecordField_withDifferentSchemas() {
        SerializationService service = createSerializationService(new SerializationConfig());

        GenericRecord itemType1 = GenericRecordBuilder.compact("item-type-1").build();
        GenericRecord itemType2 = GenericRecordBuilder.compact("item-type-2").build();

        GenericRecord record = GenericRecordBuilder.compact("foo")
                .setArrayOfGenericRecord("bar", new GenericRecord[]{itemType1, itemType2})
                .build();

        assertThatThrownBy(() -> {
            service.toData(record);
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("array of Compact serializable GenericRecord objects containing different schemas");
    }

    @Test
    public void testWritingArrayOfCompactGenericField_withDifferentItemTypes() {
        SerializationService service = createSerializationService(new SerializationConfig());

        SomeCompactObject[] objects = new SomeCompactObject[2];
        objects[0] = new SomeCompactObjectImpl();
        objects[1] = new SomeOtherCompactObjectImpl();

        WithCompactArrayField object = new WithCompactArrayField(objects);

        assertThatThrownBy(() -> {
            service.toData(object);
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("array of Compact serializable objects containing different item types");
    }

    private SerializationService createSerializationService(SerializationConfig config) {
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(config)
                .build();
    }

    private static class IntegerSerializer implements CompactSerializer<Integer> {
        @Nonnull
        @Override
        public Integer read(@Nonnull CompactReader reader) {
            return reader.readInt32("field");
        }

        @Override
        public void write(@Nonnull CompactWriter writer, @Nonnull Integer object) {
            writer.writeInt32("field", object);
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "int";
        }

        @Nonnull
        @Override
        public Class<Integer> getCompactClass() {
            return Integer.class;
        }
    }

    private static class ClassWithUnsupportedField {
        private ArrayList<String> list;
    }

    private static class ClassWithUnsupportedArrayField {
        private ArrayList<String>[] lists;
    }

    private static class Foo {
        private final int bar;

        private Foo(int bar) {
            this.bar = bar;
        }
    }

    private static class DuplicateWritingSerializer implements CompactSerializer<Foo> {
        @Nonnull
        @Override
        public Foo read(@Nonnull CompactReader reader) {
            return new Foo(reader.readInt32("bar"));
        }

        @Override
        public void write(@Nonnull CompactWriter writer, @Nonnull Foo object) {
            writer.writeInt32("bar", object.bar);
            writer.writeInt32("bar", object.bar);
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "foo";
        }

        @Nonnull
        @Override
        public Class<Foo> getCompactClass() {
            return Foo.class;
        }
    }

    private static class WithCompactArrayField {
        private SomeCompactObject[] compactObjects;

        private WithCompactArrayField(SomeCompactObject[] compactObjects) {
            this.compactObjects = compactObjects;
        }
    }

    private interface SomeCompactObject {
    }

    private static class SomeCompactObjectImpl implements SomeCompactObject {
    }

    private static class SomeOtherCompactObjectImpl implements SomeCompactObject {
    }
}
