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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import example.serialization.EmployeeDTO;
import example.serialization.ExternalizableEmployeeDTO;
import example.serialization.HiringStatus;
import example.serialization.InnerDTO;
import example.serialization.MainDTO;
import example.serialization.NamedDTO;
import example.serialization.AllFieldsDTO;
import example.serialization.SerializableEmployeeDTO;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.phonehome.TestUtil.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class CompactTestUtil {

    private CompactTestUtil() {
    }

    @Nonnull
    static GenericRecord createCompactGenericRecord(MainDTO mainDTO) {
        InnerDTO inner = mainDTO.p;
        GenericRecord[] namedRecords = new GenericRecord[inner.nn.length];
        int i = 0;
        for (NamedDTO named : inner.nn) {
            GenericRecord namedRecord = GenericRecordBuilder.compact("named")
                    .setString("name", named.name)
                    .setInt32("myint", named.myint).build();
            namedRecords[i++] = namedRecord;
        }

        GenericRecord innerRecord = GenericRecordBuilder.compact("inner")
                .setArrayOfInt8("b", inner.bytes)
                .setArrayOfInt16("s", inner.shorts)
                .setArrayOfInt32("i", inner.ints)
                .setArrayOfInt64("l", inner.longs)
                .setArrayOfFloat32("f", inner.floats)
                .setArrayOfFloat64("d", inner.doubles)
                .setArrayOfString("strings", inner.strings)
                .setArrayOfGenericRecord("nn", namedRecords)
                .setArrayOfDecimal("bigDecimals", inner.bigDecimals)
                .setArrayOfTime("localTimes", inner.localTimes)
                .setArrayOfDate("localDates", inner.localDates)
                .setArrayOfTimestamp("localDateTimes", inner.localDateTimes)
                .setArrayOfTimestampWithTimezone("offsetDateTimes", inner.offsetDateTimes)
                .build();

        return GenericRecordBuilder.compact("main")
                .setInt8("b", mainDTO.b)
                .setBoolean("bool", mainDTO.bool)
                .setInt16("s", mainDTO.s)
                .setInt32("i", mainDTO.i)
                .setInt64("l", mainDTO.l)
                .setFloat32("f", mainDTO.f)
                .setFloat64("d", mainDTO.d)
                .setString("str", mainDTO.str)
                .setDecimal("bigDecimal", mainDTO.bigDecimal)
                .setGenericRecord("p", innerRecord)
                .setTime("localTime", mainDTO.localTime)
                .setDate("localDate", mainDTO.localDate)
                .setTimestamp("localDateTime", mainDTO.localDateTime)
                .setTimestampWithTimezone("offsetDateTime", mainDTO.offsetDateTime)
                .setNullableInt8("nullable_b", mainDTO.b)
                .setNullableBoolean("nullable_bool", mainDTO.bool)
                .setNullableInt16("nullable_s", mainDTO.s)
                .setNullableInt32("nullable_i", mainDTO.i)
                .setNullableInt64("nullable_l", mainDTO.l)
                .setNullableFloat32("nullable_f", mainDTO.f)
                .setNullableFloat64("nullable_d", mainDTO.d)
                .build();
    }

    @Nonnull
    static InnerDTO createInnerDTO() {
        NamedDTO[] nn = new NamedDTO[2];
        nn[0] = new NamedDTO("name", 123);
        nn[1] = new NamedDTO("name", 123);
        return new InnerDTO(new boolean[]{true, false}, new byte[]{0, 1, 2}, new char[]{'0', 'a', 'b'},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321},
                new String[]{"test", null}, nn,
                new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.of(22, 13, 15, 123123), null, LocalTime.of(1, 2, 3, 999_999_999)},
                new LocalDate[]{LocalDate.of(2022, 12, 23), null, LocalDate.of(999_999_999, 1, 2)},
                new LocalDateTime[]{LocalDateTime.of(2022, 12, 23, 22, 13, 15, 123123), null},
                new OffsetDateTime[]{OffsetDateTime.of(2022, 12, 23, 22, 13, 15, 123123, ZoneOffset.MAX)},
                new Boolean[]{true, false, null},
                new Byte[]{0, 1, 2, null}, new Character[]{'i', null, '9'},
                new Short[]{3, 4, 5, null}, new Integer[]{9, 8, 7, 6, null}, new Long[]{0L, 1L, 5L, 7L, 9L, 11L},
                new Float[]{0.6543f, -3.56f, 45.67f}, new Double[]{456.456, 789.789, 321.321});
    }

    @Nonnull
    public static MainDTO createMainDTO() {
        InnerDTO inner = createInnerDTO();
        return new MainDTO((byte) 113, true, '\u1256', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main object created for testing!", inner,
                new BigDecimal("12312313"), LocalTime.now(), LocalDate.now(), LocalDateTime.now(), OffsetDateTime.now(),
                (byte) 113, true, '\u4567', (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d);
    }

    @Nonnull
    public static AllFieldsDTO createAllFieldsDTO() {
        List<Integer> listOfIntegers = Arrays.asList(1, 3, 5, 6);
        Map<Integer, Integer> mapOfIntegers = new HashMap<>();
        mapOfIntegers.put(1, 2);
        mapOfIntegers.put(3, 4);
        mapOfIntegers.put(5, 6);
        Set<Integer> setOfIntegers = new HashSet<>();
        setOfIntegers.add(1);
        setOfIntegers.add(2);
        setOfIntegers.add(3);

        return new AllFieldsDTO(true, (byte) 113, (short) -500, 56789, -50992225L, 900.5678f,
                -897543.3678909d, "this is main object created for testing!",
                new BigDecimal("12312313"), LocalTime.of(1, 2, 3, 999999999), LocalDate.of(-999999999, 3, 31),
                LocalDateTime.of(-999999999, 3, 31, 1, 2, 3, 999999999),
                OffsetDateTime.of(-999999999, 3, 31, 1, 2, 3, 999999999, ZoneOffset.UTC), true,
                (byte) 113, (short) -500, 56789, -50992225L, 900.5678f, -897543.3678909d,
                new boolean[]{true, false}, new byte[]{0, 1, 2}, new short[]{3, 4, 5},
                 new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321},
                new String[]{"test", null}, new BigDecimal[]{new BigDecimal("12345"), new BigDecimal("123456")},
                new LocalTime[]{LocalTime.of(22, 13, 15, 123123), null, LocalTime.of(1, 2, 3, 999_999_999)},
                new LocalDate[]{LocalDate.of(2022, 12, 23), null, LocalDate.of(999_999_999, 1, 2)},
                new LocalDateTime[]{LocalDateTime.of(2022, 12, 23, 22, 13, 15, 123123), null},
                new OffsetDateTime[]{OffsetDateTime.of(2022, 12, 23, 22, 13, 15, 123123, ZoneOffset.MAX)},
                new Boolean[]{true, false, null}, new Byte[]{0, 1, 2, null}, new Short[]{3, 4, 5, null},
                new Integer[]{9, 8, 7, 6, null}, new Long[]{0L, 1L, 5L, 7L, 9L, 11L}, new Float[]{0.6543f, -3.56f, 45.67f},
                new Double[]{456.456, 789.789, 321.321}, new EmployeeDTO(123, 123L),
                new EmployeeDTO[]{new EmployeeDTO(123, 123L), null}, '\u1256', new char[]{'0', 'a', 'b'}, '\u4567',
                new Character[]{'i', null, '9'}, HiringStatus.HIRING, new HiringStatus[]{HiringStatus.HIRING,
                HiringStatus.NOT_HIRING}, listOfIntegers, mapOfIntegers, setOfIntegers);
    }

    @Nonnull
    static VarSizedFieldsDTO createVarSizedFieldsDTO() {
        InnerDTO inner = createInnerDTO();
        return new VarSizedFieldsDTO(new boolean[]{true, false}, new byte[]{0, 1, 2},
                new short[]{3, 4, 5}, new int[]{9, 8, 7, 6}, new long[]{0, 1, 5, 7, 9, 11},
                new float[]{0.6543f, -3.56f, 45.67f}, new double[]{456.456, 789.789, 321.321}, "test",
                new String[]{"test", null}, new BigDecimal("12345"),
                new BigDecimal[]{new BigDecimal("12345.123123123"), null},
                null, new LocalTime[]{LocalTime.now(), null, LocalTime.now()},
                LocalDate.now(),  new LocalDate[]{LocalDate.now(), null, LocalDate.now()},
                LocalDateTime.now(), new LocalDateTime[]{LocalDateTime.now(), null},
                OffsetDateTime.now(), null, inner, new InnerDTO[]{createInnerDTO(), null},
                null, new Boolean[]{true, false, null}, (byte) 123, new Byte[]{0, 1, 2, null},
                (short) 1232, new Short[]{3, 4, 5, null}, null, new Integer[]{9, 8, 7, 6, null},
                12323121331L, new Long[]{0L, 1L, 5L, 7L, 9L, 12323121331L}, 0.6543f,
                new Float[]{0.6543f, -3.56f, 45.67f}, 456.4123156,
                new Double[]{456.4123156, 789.789, 321.321});
    }

    @Nonnull
    static FixedSizeFieldsDTO createFixedSizeFieldsDTO() {
        return new FixedSizeFieldsDTO((byte) 1, true, (short) 1231, 123123123, 123123123123L, 12312.123f,
                1111.1111111123123);
    }

    @Nonnull
    static ArrayOfFixedSizeFieldsDTO createArrayOfFixedSizeFieldsDTO() {
        return new ArrayOfFixedSizeFieldsDTO(new byte[]{1, 2, 3}, new boolean[]{true, false},
                new short[]{1231, 1232}, new int[]{123123123, 123123123}, new long[]{123123123123L, 123123123123L},
                new float[]{12312.123f, 12312.123f}, new double[]{1111.1111111123123, 1111.1111111123123});
    }

    @Nonnull
    static ArrayOfFixedSizeFieldsDTO createArrayOfFixedSizeFieldsDTOAsNullValues() {
        return new ArrayOfFixedSizeFieldsDTO(null, null, null, null, null, null, null);
    }

    public static InternalSerializationService createSerializationService() {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig())
                .build();
    }

    public static InternalSerializationService createSerializationService(SchemaService schemaService) {
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig())
                .build();
    }

    public static InternalSerializationService createSerializationService(SerializationConfig config) {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(config)
                .build();
    }

    public static InternalSerializationService createSerializationService(CompactSerializationConfig compactSerializationConfig) {
        SerializationConfig config = new SerializationConfig();
        config.setCompactSerializationConfig(compactSerializationConfig);
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(config)
                .build();
    }

    public static <T> InternalSerializationService createSerializationService(Supplier<CompactSerializer<T>> serializerSupplier) {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.addSerializer(serializerSupplier.get());
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    public static <T> InternalSerializationService createSerializationService(
            Supplier<CompactSerializer<T>> serializerSupplier, SchemaService schemaService
    ) {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.addSerializer(serializerSupplier.get());
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    public static InternalSerializationService createSerializationService(ClassLoader classLoader, SchemaService schemaService) {
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setClassLoader(classLoader)
                .build();
    }

    public static SchemaService createInMemorySchemaService() {
        return new SchemaService() {
            private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();

            @Override
            public Schema get(long schemaId) {
                return schemas.get(schemaId);
            }

            @Override
            public void put(Schema schema) {
                long schemaId = schema.getSchemaId();
                Schema existingSchema = schemas.putIfAbsent(schemaId, schema);
                if (existingSchema != null && !schema.equals(existingSchema)) {
                    throw new IllegalStateException("Schema with schemaId " + schemaId + " already exists. "
                            + "existing schema " + existingSchema
                            + "new schema " + schema);
                }
            }

            @Override
            public void putLocal(Schema schema) {
                put(schema);
            }
        };
    }

    public static void verifyReflectiveSerializerIsUsed(SerializationConfig serializationConfig) {
        SerializationService serializationService = createSerializationService(serializationConfig);

        ExternalizableEmployeeDTO object = new ExternalizableEmployeeDTO();
        Data data = serializationService.toData(object);
        assertFalse(object.usedExternalizableSerialization());

        ExternalizableEmployeeDTO deserializedObject = serializationService.toObject(data);
        assertFalse(deserializedObject.usedExternalizableSerialization());
    }

    public static void verifyExplicitSerializerIsUsed(SerializationConfig serializationConfig) {
        SerializationService serializationService = createSerializationService(serializationConfig);

        SerializableEmployeeDTO object = new SerializableEmployeeDTO("John Doe", 1);
        Data data = serializationService.toData(object);

        assertTrue(data.isCompact());

        SerializableEmployeeDTO deserializedObject = serializationService.toObject(data);
        assertEquals(object, deserializedObject);
    }

    public static void verifySerializationServiceBuilds(SerializationConfig serializationConfig) {
        createSerializationService(serializationConfig);
    }

    public static void assertSchemasAvailable(Collection<HazelcastInstance> instances, Class<?>... classes) {
        Collection<Schema> expectedSchemas = getSchemasFor(classes);
        for (HazelcastInstance instance : instances) {
            Collection<Schema> schemas = getNode(instance).getSchemaService().getAllSchemas();
            assertThat(schemas).containsExactlyInAnyOrderElementsOf(expectedSchemas);
        }
    }

    /**
     * Can only return the schemas for classes that are serialized with
     * reflective serializer.
     */
    public static Collection<Schema> getSchemasFor(Class<?>... classes) {
        CompactStreamSerializer compactStreamSerializer = mock(CompactStreamSerializer.class);
        when(compactStreamSerializer.canBeSerializedAsCompact(any())).thenReturn(true);
        ReflectiveCompactSerializer serializer = new ReflectiveCompactSerializer(compactStreamSerializer);
        ArrayList<Schema> schemas = new ArrayList<>(classes.length);
        for (Class<?> clazz : classes) {
            SchemaWriter writer = new SchemaWriter(clazz.getName());
            try {
                serializer.write(writer, clazz.getDeclaredConstructor().newInstance());
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            schemas.add(writer.build());
        }
        return schemas;
    }
}
