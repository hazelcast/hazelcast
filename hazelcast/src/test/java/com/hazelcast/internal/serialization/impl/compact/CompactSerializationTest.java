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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSerializationTest {

    @Test
    public void testOverridingDefaultSerializers() {
        assertThatThrownBy(() -> createSerializationService(IntegerSerializer::new))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overrides the default serializer");
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
    public void testReadWhenFieldDoesNotExist() {
        SerializationService service = createSerializationService(NonExistingFieldReadingSerializer::new);

        Foo foo = new Foo(42);
        Data data = service.toData(foo);

        assertThatThrownBy(() -> service.toObject(data))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("Invalid field name");
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
    public void testSerializingClassReflectively_withUnsupportedField_whenTheFieldHasExplicitSerializer() {
        SerializationService service = createSerializationService(InstantSerializer::new);
        ClassWithInstantField object = new ClassWithInstantField(Instant.ofEpochSecond(123, 456));
        Data data = service.toData(object);
        ClassWithInstantField deserialized = service.toObject(data);
        assertEquals(object, deserialized);
    }

    @Test
    public void testSerializingClassReflectively_withArrayOfUnsupportedField_whenTheComponentTypeHasExplicitSerializer() {
        SerializationService service = createSerializationService(InstantSerializer::new);
        ClassWithInstantArrayField object = new ClassWithInstantArrayField(new Instant[]{
                Instant.ofEpochSecond(123, 456), Instant.ofEpochSecond(789123, 2112356)
        });
        Data data = service.toData(object);
        ClassWithInstantArrayField deserialized = service.toObject(data);
        assertEquals(object, deserialized);
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
    public void testSerializingClassReflectively_withArrayOfArrayField() {
        SerializationService service = createSerializationService(new SerializationConfig());
        assertThatThrownBy(() -> {
                service.toData(new ClassWithArrayOfArrayField());
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("which uses this class in its fields");
    }

    @Test
    public void testSerializingClassReflectively_withVoidField() {
        SerializationService service = createSerializationService(new SerializationConfig());
        assertThatThrownBy(() -> {
            service.toData(new ClassWithVoidField());
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

    @Test
    public void testSerializingClassReflectively_withCollectionTypes_whenCollectionsHasUnsupportedGenericTypes() {
        SerializationConfig config = new SerializationConfig();
        SerializationService service = createSerializationService(config);

        assertThatThrownBy(() -> {
            service.toData(new ClassWithUnsupportedArrayListField());
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("UUID")
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("which uses this class in its fields");

        assertThatThrownBy(() -> {
            service.toData(new ClassWithUnsupportedHashSetField());
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("UUID")
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("which uses this class in its fields");

        assertThatThrownBy(() -> {
            service.toData(new ClassWithUnsupportedHashMapField());
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("UUID")
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("which uses this class in its fields");
    }

    @Test
    public void testSerializingClassReflectively_withCollectionTypes() {
        SerializationConfig config = new SerializationConfig();
        SerializationService service = createSerializationService(config);
        ClassWithCollectionFields object = new ClassWithCollectionFields(
                new ArrayList<>(Arrays.asList("a", "b", "c", null)),
                new ArrayList<>(Arrays.asList(1, 2, 3, null, -42)),
                new HashSet<>(Arrays.asList(null, true, false)),
                new HashSet<>(Arrays.asList(BigDecimal.ONE, BigDecimal.TEN, null)),
                new HashMap<Long, Float>() {{
                    put(1L, 42F);
                }},
                new HashMap<Byte, Double>() {{
                    put((byte) 0, 42D);
                }}
        );

        Data data = service.toData(object);
        ClassWithCollectionFields deserialized = service.toObject(data);
        assertEquals(object, deserialized);
    }

    @Test
    public void testSerializingClassReflectively_withCollectionTypes_whenTheyAreNull() {
        SerializationConfig config = new SerializationConfig();
        SerializationService service = createSerializationService(config);
        ClassWithCollectionFields object = new ClassWithCollectionFields(
                null,
                null,
                null,
                null,
                null,
                null
        );

        Data data = service.toData(object);
        ClassWithCollectionFields deserialized = service.toObject(data);
        assertEquals(object, deserialized);
    }

    @Test
    public void testReflectiveSerializer_withFieldsWithOtherSerializationMechanisms() {
        SerializationConfig config = new SerializationConfig();
        SerializationService service = createSerializationService(config);

        assertThatThrownBy(() -> {
            service.toData(new UsesSerializableClassAsField(new SerializableEmployeeDTO("John Doe", 42)));
        }).isInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("cannot be serialized with zero configuration Compact serialization")
                .hasStackTraceContaining("can be serialized with another serialization mechanism.");
    }

    @Test
    public void testReflectiveSerializer_withFieldsWithOtherSerializationMechanisms_whenCompactOverridesIt() {
        SerializationConfig config = new SerializationConfig();
        config.getCompactSerializationConfig().addClass(SerializableEmployeeDTO.class);
        SerializationService service = createSerializationService(config);

        Data data = service.toData(new UsesSerializableClassAsField(new SerializableEmployeeDTO("John Doe", 42)));
        assertTrue(data.isCompact());
    }

    @Test
    public void testCompactWriterThrowsExceptionWhileWritingWhenSchemaDoesNotHaveThatField() {
        FooBarSerializer serializer = spy(new FooBarSerializer());
        FooBar fooBar = new FooBar("foo", 42L);

        SerializationService serializationService = createSerializationService(() -> serializer);
        // Add correct schema to classToSchemaMap
        serializationService.toData(fooBar);
        // Change serializer's write method
        doAnswer(invocation -> {
            CompactWriter writer = invocation.getArgument(0);
            FooBar value = invocation.getArgument(1);
            writer.writeString("foo", value.getFoo());
            writer.writeInt64("bar", value.getBar());
            writer.writeString("baz", "a");
            return null;
        }).when(serializer).write(any(), any());
        // This should throw because schema does not have a baz field.
        assertThatThrownBy(() -> {
            serializationService.toData(fooBar);
        }).isInstanceOf(HazelcastSerializationException.class)
          .hasCauseInstanceOf(HazelcastSerializationException.class)
          .hasStackTraceContaining("Invalid field name");
    }

    @Test
    public void testCompactWriterThrowsExceptionWhileWritingWhenFieldTypeDoesNotMatch() {
        FooBar fooBar = new FooBar("foo", 42L);
        FooBarSerializer serializer = spy(new FooBarSerializer());

        SerializationService serializationService = createSerializationService(() -> serializer);
        // Add correct schema to classToSchemaMap
        serializationService.toData(fooBar);
        // Change serializer's write method
        doAnswer(invocation -> {
            CompactWriter writer = invocation.getArgument(0);
            FooBar value = invocation.getArgument(1);
            writer.writeInt32("foo", 123);
            writer.writeInt64("bar", value.getBar());
            return null;
        }).when(serializer).write(any(), any());
        // This should throw because in the schema type of foo is not int32.
        assertThatThrownBy(() -> {
                serializationService.toData(fooBar);
        }).isInstanceOf(HazelcastSerializationException.class)
          .hasCauseInstanceOf(HazelcastSerializationException.class)
          .hasStackTraceContaining("Invalid field type");
    }

    private static class FooBar {
        private final String foo;
        private final long bar;

        FooBar(String foo, long bar) {
            this.foo = foo;
            this.bar = bar;
        }

        public String getFoo() {
            return foo;
        }

        public long getBar() {
            return bar;
        }
    }

    private static class FooBarSerializer implements CompactSerializer<FooBar> {
        @Nonnull
        @Override
        public FooBar read(@Nonnull CompactReader reader) {
            String foo = reader.readString("foo");
            long bar = reader.readInt64("bar");
            return new FooBar(foo, bar);
        }

        @Override
        public void write(@Nonnull CompactWriter writer, @Nonnull FooBar object) {
            writer.writeString("foo", object.foo);
            writer.writeInt64("bar", object.bar);
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "foobar";
        }

        @Nonnull
        @Override
        public Class getCompactClass() {
            return FooBar.class;
        }
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
        private LinkedList<String> list;
    }

    private static class ClassWithInstantField {
        private final Instant instant;
        ClassWithInstantField(Instant instant) {
            this.instant = instant;
        }

        public Instant getInstant() {
            return instant;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassWithInstantField that = (ClassWithInstantField) o;
            return Objects.equals(instant, that.instant);
        }

        @Override
        public int hashCode() {
            return Objects.hash(instant);
        }
    }

    private static class ClassWithInstantArrayField {
        private final Instant[] instants;
        ClassWithInstantArrayField(Instant[] instants) {
            this.instants = instants;
        }

        public Instant[] getInstants() {
            return instants;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassWithInstantArrayField that = (ClassWithInstantArrayField) o;
            return Arrays.equals(instants, that.getInstants());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(instants);
        }
    }

    private static class ClassWithUnsupportedArrayField {
        private LinkedList<String>[] lists;
    }

    private static class ClassWithArrayOfArrayField {
        private int[][] arrays;
    }

    private static class ClassWithVoidField {
        private Void aVoid;
    }

    private static class ClassWithCollectionFields {
        private List<String> stringList;
        private ArrayList<Integer> integerArrayList;
        private Set<Boolean> booleanSet;
        private HashSet<BigDecimal> decimalHashSet;
        private Map<Long, Float> longFloatMap;
        private HashMap<Byte, Double> byteDoubleHashMap;

        private ClassWithCollectionFields(List<String> stringList,
                                         ArrayList<Integer> integerArrayList,
                                         Set<Boolean> booleanSet,
                                         HashSet<BigDecimal> decimalHashSet,
                                         Map<Long, Float> longFloatMap,
                                         HashMap<Byte, Double> byteDoubleHashMap) {
            this.stringList = stringList;
            this.integerArrayList = integerArrayList;
            this.booleanSet = booleanSet;
            this.decimalHashSet = decimalHashSet;
            this.longFloatMap = longFloatMap;
            this.byteDoubleHashMap = byteDoubleHashMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassWithCollectionFields that = (ClassWithCollectionFields) o;
            return Objects.equals(stringList, that.stringList)
                    && Objects.equals(integerArrayList, that.integerArrayList)
                    && Objects.equals(booleanSet, that.booleanSet)
                    && Objects.equals(decimalHashSet, that.decimalHashSet)
                    && Objects.equals(longFloatMap, that.longFloatMap)
                    && Objects.equals(byteDoubleHashMap, that.byteDoubleHashMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stringList, integerArrayList, booleanSet,
                    decimalHashSet, longFloatMap, byteDoubleHashMap);
        }
    }

    private static class ClassWithUnsupportedArrayListField {
        private ArrayList<UUID> list;
    }

    private static class ClassWithUnsupportedHashSetField {
        private HashSet<UUID> set;
    }

    private static class ClassWithUnsupportedHashMapField {
        private HashMap<UUID, Long> map;
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

    private static class UsesSerializableClassAsField {
        private SerializableEmployeeDTO serializableClass;

        private UsesSerializableClassAsField(SerializableEmployeeDTO serializableClass) {
            this.serializableClass = serializableClass;
        }
    }

    private static class NonExistingFieldReadingSerializer implements CompactSerializer<Foo> {
        @Nonnull
        @Override
        public Foo read(@Nonnull CompactReader in) {
            return new Foo(in.readInt32("nonExistingField"));
        }

        @Override
        public void write(@Nonnull CompactWriter out, @Nonnull Foo object) {
            out.writeInt32("bar", object.bar);
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

    private static class InstantSerializer implements CompactSerializer<Instant> {
        @Nonnull
        @Override
        public Instant read(@Nonnull CompactReader reader) {
            long epoch = reader.readInt64("epoch");
            int nano = reader.readInt32("nano");
            return Instant.ofEpochSecond(epoch, nano);
        }

        @Override
        public void write(@Nonnull CompactWriter writer, @Nonnull Instant object) {
            writer.writeInt64("epoch", object.getEpochSecond());
            writer.writeInt32("nano", object.getNano());
        }

        @Nonnull
        @Override
        public String getTypeName() {
            return "instant";
        }

        @Nonnull
        @Override
        public Class<Instant> getCompactClass() {
            return Instant.class;
        }
    }
}
