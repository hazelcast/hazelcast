/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.time.ZoneOffset.UTC;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class PortableUpsertTargetTest extends UpsertTargetTestSupport {

    @Test
    public void test_set() throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition innerClassDefinition = new ClassDefinitionBuilder(4, 5, 6).build();
        ss.getPortableContext().registerClassDefinition(innerClassDefinition);

        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addPortableField("null", innerClassDefinition)
                        .addPortableField("object", innerClassDefinition)
                        .addStringField("string")
                        .addCharField("character")
                        .addBooleanField("boolean")
                        .addByteField("byte")
                        .addShortField("short")
                        .addIntField("int")
                        .addLongField("long")
                        .addFloatField("float")
                        .addDoubleField("double")
                        .addDecimalField("decimal")
                        .addTimeField("time")
                        .addDateField("date")
                        .addTimestampField("timestamp")
                        .addTimestampWithTimezoneField("timestampTz")
                        .build();

        UpsertTarget target = new PortableUpsertTarget(classDefinition, ss);
        UpsertConverter converter = target.createConverter(List.of(
                field("null", QueryDataType.OBJECT),
                field("object", QueryDataType.OBJECT),
                field("string", QueryDataType.VARCHAR),
                field("character", QueryDataType.VARCHAR_CHARACTER),
                field("boolean", QueryDataType.BOOLEAN),
                field("byte", QueryDataType.TINYINT),
                field("short", QueryDataType.SMALLINT),
                field("int", QueryDataType.INT),
                field("long", QueryDataType.BIGINT),
                field("float", QueryDataType.REAL),
                field("double", QueryDataType.DOUBLE),
                field("decimal", QueryDataType.DECIMAL),
                field("time", QueryDataType.TIME),
                field("date", QueryDataType.DATE),
                field("timestamp", QueryDataType.TIMESTAMP),
                field("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME)
        ));

        Object portable = converter.applyRow(
                null,
                new PortableGenericRecordBuilder(innerClassDefinition).build(),
                "1",
                '2',
                true,
                (byte) 3,
                (short) 4,
                5,
                6L,
                7.1F,
                7.2D,
                new BigDecimal("8.1"),
                LocalTime.of(12, 23, 34),
                LocalDate.of(2021, 2, 9),
                LocalDateTime.of(2021, 2, 9, 12, 23, 34, 1_000_000),
                OffsetDateTime.of(2021, 2, 9, 12, 23, 34, 200_000_000, UTC)
        );

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(record.getGenericRecord("null")).isNull();
        assertThat(record.getGenericRecord("object"))
                .isEqualTo(new PortableGenericRecordBuilder(innerClassDefinition).build());
        assertThat(record.getString("string")).isEqualTo("1");
        assertThat(record.getChar("character")).isEqualTo('2');
        assertThat(record.getBoolean("boolean")).isEqualTo(true);
        assertThat(record.getInt8("byte")).isEqualTo((byte) 3);
        assertThat(record.getInt16("short")).isEqualTo((short) 4);
        assertThat(record.getInt32("int")).isEqualTo(5);
        assertThat(record.getInt64("long")).isEqualTo(6L);
        assertThat(record.getFloat32("float")).isEqualTo(7.1F);
        assertThat(record.getFloat64("double")).isEqualTo(7.2D);
        assertThat(record.getDecimal("decimal")).isEqualTo(new BigDecimal("8.1"));
        assertThat(record.getTime("time")).isEqualTo(LocalTime.of(12, 23, 34));
        assertThat(record.getDate("date")).isEqualTo(LocalDate.of(2021, 2, 9));
        assertThat(record.getTimestamp("timestamp")).isEqualTo(LocalDateTime.of(2021, 2, 9, 12, 23, 34, 1_000_000));
        assertThat(record.getTimestampWithTimezone("timestampTz"))
                .isEqualTo(OffsetDateTime.of(2021, 2, 9, 12, 23, 34, 200_000_000, UTC));
    }

    @SuppressWarnings("unused")
    private Object[] primitiveClassDefinitions() {
        return new Object[]{
                testCase(schema -> schema.addBooleanField("field"), QueryDataType.BOOLEAN, false,
                        record -> record.getBoolean("field")),
                testCase(schema -> schema.addByteField("field"), QueryDataType.TINYINT, (byte) 0,
                        record -> record.getInt8("field")),
                testCase(schema -> schema.addShortField("field"), QueryDataType.SMALLINT, (short) 0,
                        record -> record.getInt16("field")),
                testCase(schema -> schema.addIntField("field"), QueryDataType.INT, 0,
                        record -> record.getInt32("field")),
                testCase(schema -> schema.addLongField("field"), QueryDataType.BIGINT, 0L,
                        record -> record.getInt64("field")),
                testCase(schema -> schema.addFloatField("field"), QueryDataType.REAL, 0F,
                        record -> record.getFloat32("field")),
                testCase(schema -> schema.addDoubleField("field"), QueryDataType.DOUBLE, 0D,
                        record -> record.getFloat64("field"))
        };
    }

    private static Object[] testCase(UnaryOperator<ClassDefinitionBuilder> classDefinition,
                                     QueryDataType type,
                                     Object defaultValue,
                                     Function<InternalGenericRecord, Object> valueExtractor) {
        return new Object[]{
                classDefinition.apply(new ClassDefinitionBuilder(1, 2, 3)).build(),
                type, defaultValue, valueExtractor
        };
    }

    @Test
    @Parameters(method = "primitiveClassDefinitions")
    @SuppressWarnings("unused")
    public void when_injectNullIntoPrimitive_then_throws(
            ClassDefinition classDefinition,
            QueryDataType type,
            Object defaultValue,
            Function<InternalGenericRecord, Object> valueExtractor
    ) {
        UpsertTarget target = new PortableUpsertTarget(classDefinition, null);
        UpsertConverter converter = target.createConverter(List.of(
                field("field", type)
        ));

        assertThatThrownBy(() -> converter.applyRow((Object) null))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot set NULL to a primitive field");
    }

    @Test
    @Parameters(method = "primitiveClassDefinitions")
    @SuppressWarnings("unused")
    public void when_doesNotInjectIntoPrimitive_then_insertsDefaultValue(
            ClassDefinition classDefinition,
            QueryDataType type,
            Object defaultValue,
            Function<InternalGenericRecord, Object> valueExtractor
    ) throws IOException {
        UpsertTarget target = new PortableUpsertTarget(classDefinition, null);

        UpsertConverter converter = target.createConverter(emptyList());
        Object portable = converter.applyRow();

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(valueExtractor.apply(record)).isEqualTo(defaultValue);
    }

    @SuppressWarnings("unused")
    private Object[] objectClassDefinitions() {
        ClassDefinition innerClassDefinition = new ClassDefinitionBuilder(4, 5, 6).build();

        return new Object[]{
                testCase(schema -> schema.addPortableField("object", innerClassDefinition), null,
                        record -> record.getGenericRecord("object")),
                testCase(schema -> schema.addPortableField("object", innerClassDefinition),
                        new PortableGenericRecordBuilder(innerClassDefinition).build(),
                        record -> record.getGenericRecord("object")),
                testCase(schema -> schema.addPortableArrayField("object", innerClassDefinition), null,
                        record -> record.getArrayOfGenericRecord("object")),
                testCase(schema -> schema.addPortableArrayField("object", innerClassDefinition),
                        new GenericRecord[0], record -> record.getArrayOfGenericRecord("object")),
                testCase(schema -> schema.addBooleanArrayField("object"), null,
                        record -> record.getArrayOfBoolean("object")),
                testCase(schema -> schema.addBooleanArrayField("object"), new boolean[0],
                        record -> record.getArrayOfBoolean("object")),
                testCase(schema -> schema.addByteArrayField("object"), null,
                        record -> record.getArrayOfInt8("object")),
                testCase(schema -> schema.addByteArrayField("object"), new byte[0],
                        record -> record.getArrayOfInt8("object")),
                testCase(schema -> schema.addShortArrayField("object"), null,
                        record -> record.getArrayOfInt16("object")),
                testCase(schema -> schema.addShortArrayField("object"), new short[0],
                        record -> record.getArrayOfInt16("object")),
                testCase(schema -> schema.addCharArrayField("object"), null,
                        record -> record.getArrayOfChar("object")),
                testCase(schema -> schema.addCharArrayField("object"), new char[0],
                        record -> record.getArrayOfChar("object")),
                testCase(schema -> schema.addIntArrayField("object"), null,
                        record -> record.getArrayOfInt32("object")),
                testCase(schema -> schema.addIntArrayField("object"), new int[0],
                        record -> record.getArrayOfInt32("object")),
                testCase(schema -> schema.addLongArrayField("object"), null,
                        record -> record.getArrayOfInt64("object")),
                testCase(schema -> schema.addLongArrayField("object"), new long[0],
                        record -> record.getArrayOfInt64("object")),
                testCase(schema -> schema.addFloatArrayField("object"), null,
                        record -> record.getArrayOfFloat32("object")),
                testCase(schema -> schema.addFloatArrayField("object"), new float[0],
                        record -> record.getArrayOfFloat32("object")),
                testCase(schema -> schema.addDoubleArrayField("object"), null,
                        record -> record.getArrayOfFloat64("object")),
                testCase(schema -> schema.addDoubleArrayField("object"), new double[0],
                        record -> record.getArrayOfFloat64("object")),
                testCase(schema -> schema.addDecimalArrayField("object"), null,
                        record -> record.getArrayOfDecimal("object")),
                testCase(schema -> schema.addDecimalArrayField("object"), new BigDecimal[0],
                        record -> record.getArrayOfDecimal("object")),
                testCase(schema -> schema.addStringArrayField("object"), null,
                        record -> record.getArrayOfString("object")),
                testCase(schema -> schema.addStringArrayField("object"), new String[0],
                        record -> record.getArrayOfString("object")),
                testCase(schema -> schema.addTimeArrayField("object"), null,
                        record -> record.getArrayOfTime("object")),
                testCase(schema -> schema.addTimeArrayField("object"), new LocalTime[0],
                        record -> record.getArrayOfTime("object")),
                testCase(schema -> schema.addDateArrayField("object"), null,
                        record -> record.getArrayOfDate("object")),
                testCase(schema -> schema.addDateArrayField("object"), new LocalDate[0],
                        record -> record.getArrayOfDate("object")),
                testCase(schema -> schema.addTimestampArrayField("object"), null,
                        record -> record.getArrayOfTimestamp("object")),
                testCase(schema -> schema.addTimestampArrayField("object"), new LocalDateTime[0],
                        record -> record.getArrayOfTimestamp("object")),
                testCase(schema -> schema.addTimestampWithTimezoneArrayField("object"), null,
                        record -> record.getArrayOfTimestampWithTimezone("object")),
                testCase(schema -> schema.addTimestampWithTimezoneArrayField("object"),
                        new OffsetDateTime[0], record -> record.getArrayOfTimestampWithTimezone("object"))
        };
    }

    private static Object[] testCase(UnaryOperator<ClassDefinitionBuilder> classDefinition,
                                     Object value,
                                     Function<InternalGenericRecord, Object> valueExtractor) {
        return new Object[]{
                classDefinition.apply(new ClassDefinitionBuilder(1, 2, 3)).build(),
                value, valueExtractor
        };
    }

    @Test
    @Parameters(method = "objectClassDefinitions")
    public void when_typeIsObject_then_allValuesAreAllowed(
            ClassDefinition classDefinition,
            Object value,
            Function<InternalGenericRecord, Object> valueExtractor
    ) throws IOException {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        if (value instanceof GenericRecord) {
            ClassDefinition innerClassDefinition = new ClassDefinitionBuilder(4, 5, 6).build();
            ss.getPortableContext().registerClassDefinition(innerClassDefinition);
        }
        UpsertTarget target = new PortableUpsertTarget(classDefinition, ss);
        UpsertConverter converter = target.createConverter(List.of(
                field("object", QueryDataType.OBJECT)
        ));

        Object portable = converter.applyRow(value);

        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(valueExtractor.apply(record)).isEqualTo(value);
    }

    @Test
    @Parameters(method = "objectClassDefinitions")
    @SuppressWarnings("unused")
    public void when_doesNotInjectIntoObject_then_insertsNull(
            ClassDefinition classDefinition,
            Object value,
            Function<InternalGenericRecord, Object> valueExtractor
    ) throws IOException {
        UpsertTarget target = new PortableUpsertTarget(classDefinition, null);

        UpsertConverter converter = target.createConverter(emptyList());
        Object portable = converter.applyRow();

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(valueExtractor.apply(record)).isNull();
    }

    @Test
    public void when_injectNonExistingPropertyValue_then_throws() {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(1, 2, 3).build();

        UpsertTarget target = new PortableUpsertTarget(classDefinition, null);
        UpsertConverter converter = target.createConverter(List.of(
                field("field", QueryDataType.INT)
        ));

        assertThatThrownBy(() -> converter.applyRow("1"))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Field \"field\" doesn't exist in Portable class definition");
    }

    @Test
    public void when_injectNonExistingPropertyNullValue_then_succeeds() throws IOException {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(1, 2, 3).build();

        UpsertTarget target = new PortableUpsertTarget(classDefinition, null);
        UpsertConverter converter = target.createConverter(List.of(
                field("field", QueryDataType.INT)
        ));

        Object portable = converter.applyRow((Integer) null);

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        InternalGenericRecord record = ss.readAsInternalGenericRecord(ss.toData(portable));
        assertThat(record).isNotNull();
    }
}
