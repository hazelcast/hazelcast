/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.map.sample;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapSchemaTestSupport;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for sample resolution for serialized portables.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("unused")
public class MapSampleMetadataResolverTest extends MapSchemaTestSupport {

    private static final String PORTABLE_BOOLEAN = "_boolean";
    private static final String PORTABLE_BYTE = "_byte";
    private static final String PORTABLE_CHAR = "_char";
    private static final String PORTABLE_SHORT = "_short";
    private static final String PORTABLE_INT = "_int";
    private static final String PORTABLE_LONG = "_long";
    private static final String PORTABLE_FLOAT = "_float";
    private static final String PORTABLE_DOUBLE = "_double";
    private static final String PORTABLE_STRING = "_string";
    private static final String PORTABLE_OBJECT = "_object";

    @Test
    public void testPrimitive() {
        checkPrimitive("value", QueryDataType.VARCHAR);
        checkPrimitive('v', QueryDataType.VARCHAR_CHARACTER);

        checkPrimitive(true, QueryDataType.BOOLEAN);

        checkPrimitive((byte) 1, QueryDataType.TINYINT);
        checkPrimitive((short) 1, QueryDataType.SMALLINT);
        checkPrimitive(1, QueryDataType.INT);
        checkPrimitive(1L, QueryDataType.BIGINT);
        checkPrimitive(1f, QueryDataType.REAL);
        checkPrimitive(1d, QueryDataType.DOUBLE);

        checkPrimitive(BigInteger.ONE, QueryDataType.DECIMAL_BIG_INTEGER);
        checkPrimitive(BigDecimal.ONE, QueryDataType.DECIMAL);

        checkPrimitive(LocalTime.now(), QueryDataType.TIME);
        checkPrimitive(LocalDate.now(), QueryDataType.DATE);
        checkPrimitive(LocalDateTime.now(), QueryDataType.TIMESTAMP);

        checkPrimitive(new Date(), QueryDataType.TIMESTAMP_WITH_TZ_DATE);
        checkPrimitive(Calendar.getInstance(), QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR);
        checkPrimitive(Instant.now(), QueryDataType.TIMESTAMP_WITH_TZ_INSTANT);
        checkPrimitive(OffsetDateTime.now(), QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);
        checkPrimitive(ZonedDateTime.now(), QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME);

        checkPrimitive(new JavaWithoutFields(), QueryDataType.OBJECT);
        checkPrimitive(new PortableWithoutFields(), QueryDataType.OBJECT);
    }

    @Test
    public void testPortableObject() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
            .addPortableFactory(1, classId -> {
                if (classId == 2) {
                    return new PortableParent();
                } else if (classId == 3) {
                    return new PortableChild();
                }

                throw new IllegalArgumentException("Invalid class ID: " + classId);
            })
            .build();

        // Test key.
        MapSampleMetadata metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(new PortableParent()), false, true);

        checkFields(
            metadata,
            field("fBoolean", QueryDataType.BOOLEAN, true),
            field("fByte", QueryDataType.TINYINT, true),
            field("fChar", QueryDataType.VARCHAR_CHARACTER, true),
            field("fShort", QueryDataType.SMALLINT, true),
            field("fInt", QueryDataType.INT, true),
            field("fLong", QueryDataType.BIGINT, true),
            field("fFloat", QueryDataType.REAL, true),
            field("fDouble", QueryDataType.DOUBLE, true),
            field("fString", QueryDataType.VARCHAR, true),
            field("fObject", QueryDataType.OBJECT, true),
            hiddenField(KEY, QueryDataType.OBJECT, true)
        );

        // Test value.
        metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(new PortableParent()), false, false);

        checkFields(
            metadata,
            field("fBoolean", QueryDataType.BOOLEAN, false),
            field("fByte", QueryDataType.TINYINT, false),
            field("fChar", QueryDataType.VARCHAR_CHARACTER, false),
            field("fShort", QueryDataType.SMALLINT, false),
            field("fInt", QueryDataType.INT, false),
            field("fLong", QueryDataType.BIGINT, false),
            field("fFloat", QueryDataType.REAL, false),
            field("fDouble", QueryDataType.DOUBLE, false),
            field("fString", QueryDataType.VARCHAR, false),
            field("fObject", QueryDataType.OBJECT, false),
            hiddenField(VALUE, QueryDataType.OBJECT, false)
        );
    }

    @Test
    public void testPortableBinary() {
        InternalSerializationService ss = getSerializationService();

        // Test key.
        MapSampleMetadata metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(new PortableParent()), true, true);

        checkFields(
            metadata,
            field(PORTABLE_BOOLEAN, QueryDataType.BOOLEAN, true),
            field(PORTABLE_BYTE, QueryDataType.TINYINT, true),
            field(PORTABLE_CHAR, QueryDataType.VARCHAR_CHARACTER, true),
            field(PORTABLE_SHORT, QueryDataType.SMALLINT, true),
            field(PORTABLE_INT, QueryDataType.INT, true),
            field(PORTABLE_LONG, QueryDataType.BIGINT, true),
            field(PORTABLE_FLOAT, QueryDataType.REAL, true),
            field(PORTABLE_DOUBLE, QueryDataType.DOUBLE, true),
            field(PORTABLE_STRING, QueryDataType.VARCHAR, true),
            field(PORTABLE_OBJECT, QueryDataType.OBJECT, true),
            hiddenField(KEY, QueryDataType.OBJECT, true)
        );

        // Test value.
        metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(new PortableParent()), true, false);

        checkFields(
            metadata,
            field(PORTABLE_BOOLEAN, QueryDataType.BOOLEAN, false),
            field(PORTABLE_BYTE, QueryDataType.TINYINT, false),
            field(PORTABLE_CHAR, QueryDataType.VARCHAR_CHARACTER, false),
            field(PORTABLE_SHORT, QueryDataType.SMALLINT, false),
            field(PORTABLE_INT, QueryDataType.INT, false),
            field(PORTABLE_LONG, QueryDataType.BIGINT, false),
            field(PORTABLE_FLOAT, QueryDataType.REAL, false),
            field(PORTABLE_DOUBLE, QueryDataType.DOUBLE, false),
            field(PORTABLE_STRING, QueryDataType.VARCHAR, false),
            field(PORTABLE_OBJECT, QueryDataType.OBJECT, false),
            hiddenField(VALUE, QueryDataType.OBJECT, false)
        );
    }

    /**
     * Test Java fields.
     */
    @Test
    public void testJavaFields() {
        InternalSerializationService ss = getSerializationService();

        JavaFields object = new JavaFields();

        MapSampleMetadata metadata = MapSampleMetadataResolver.resolve(ss, object, true, true);

        checkFields(
            metadata,
            field("publicField", QueryDataType.INT, true),
            hiddenField(KEY, QueryDataType.OBJECT, true)
        );

        metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, true);

        checkFields(
            metadata,
            field("publicField", QueryDataType.INT, true),
            hiddenField(KEY, QueryDataType.OBJECT, true)
        );
    }

    /**
     * Test Java getters.
     */
    @Test
    public void testJavaGetters() {
        InternalSerializationService ss = getSerializationService();

        JavaGetters object = new JavaGetters();

        MapSampleMetadata metadata = MapSampleMetadataResolver.resolve(ss, object, true, true);

        checkFields(
            metadata,
            field("publicGetter", QueryDataType.INT, true),
            field("booleanGetGetter", QueryDataType.BOOLEAN, true),
            field("booleanIsGetter", QueryDataType.BOOLEAN, true),
            hiddenField(KEY, QueryDataType.OBJECT, true)
        );

        metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, true);

        checkFields(
            metadata,
            field("publicGetter", QueryDataType.INT, true),
            field("booleanGetGetter", QueryDataType.BOOLEAN, true),
            field("booleanIsGetter", QueryDataType.BOOLEAN, true),
            hiddenField(KEY, QueryDataType.OBJECT, true)
        );
    }

    /**
     * Test clash between parent and child class fields: child should win.
     */
    @Test
    public void testJavaFieldClash() {
        InternalSerializationService ss = getSerializationService();

        JavaFieldClashChild object = new JavaFieldClashChild();

        TableField field = MapSampleMetadataResolver.resolve(ss, object, true, true).getFields().get("field");
        assertEquals(field("field", QueryDataType.BIGINT, true), field);

        field = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, true).getFields().get("field");
        assertEquals(field("field", QueryDataType.BIGINT, true), field);
    }

    /**
     * Ensure that getter wins in case of getter/field clash.
     */
    @Test
    public void testJavaGetterFieldClash() {
        InternalSerializationService ss = getSerializationService();

        JavaFieldGetterClash object = new JavaFieldGetterClash();

        TableField field = MapSampleMetadataResolver.resolve(ss, object, true, true).getFields().get("field");
        assertEquals(field("field", QueryDataType.BIGINT, true), field);

        field = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, true).getFields().get("field");
        assertEquals(field("field", QueryDataType.BIGINT, true), field);
    }

    /**
     * Test clash between object fields and top-level names.
     */
    @Test
    public void testJavaTopFieldClash() {
        InternalSerializationService ss = getSerializationService();

        JavaTopFieldClash object = new JavaTopFieldClash();

        // Key
        TableField field = MapSampleMetadataResolver.resolve(ss, object, true, true).getFields().get(KEY);
        assertEquals(hiddenField(KEY, QueryDataType.OBJECT, true), field);

        field = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, true).getFields().get(KEY);
        assertEquals(hiddenField(KEY, QueryDataType.OBJECT, true), field);

        // Value
        field = MapSampleMetadataResolver.resolve(ss, object, true, false).getFields().get(VALUE);
        assertEquals(hiddenField(VALUE, QueryDataType.OBJECT, false), field);

        field = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, false).getFields().get(VALUE);
        assertEquals(hiddenField(VALUE, QueryDataType.OBJECT, false), field);
    }

    /**
     * Test clash between object getters and top-level names.
     */
    @Test
    public void testJavaTopGetterClash() {
        InternalSerializationService ss = getSerializationService();

        JavaTopGetterClash object = new JavaTopGetterClash();

        // Key
        TableField field = MapSampleMetadataResolver.resolve(ss, object, true, true).getFields().get(KEY);
        assertEquals(hiddenField(KEY, QueryDataType.OBJECT, true), field);

        field = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, true).getFields().get(KEY);
        assertEquals(hiddenField(KEY, QueryDataType.OBJECT, true), field);

        // Value
        field = MapSampleMetadataResolver.resolve(ss, object, true, false).getFields().get(VALUE);
        assertEquals(hiddenField(VALUE, QueryDataType.OBJECT, false), field);

        field = MapSampleMetadataResolver.resolve(ss, ss.toData(object), true, false).getFields().get(VALUE);
        assertEquals(hiddenField(VALUE, QueryDataType.OBJECT, false), field);
    }

    @Test
    public void testJavaFieldTypes() {
        checkJavaTypes(new JavaFieldTypes());
    }

    @Test
    public void testJavaGetterTypes() {
        checkJavaTypes(new JavaGetterTypes());
    }

    @Test
    public void testPortableClash() {
        InternalSerializationService ss = getSerializationService();

        // Key clash
        MapSampleMetadata metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(new PortableClash()), true, true);

        checkFields(
            metadata,
            hiddenField(KEY, QueryDataType.OBJECT, true),
            field(VALUE, QueryDataType.INT, true)
        );

        // Value clash
        metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(new PortableClash()), true, false);

        checkFields(
            metadata,
            field(KEY, QueryDataType.INT, false),
            hiddenField(VALUE, QueryDataType.OBJECT, false)
        );
    }

    @Test
    public void testJson() {
        InternalSerializationService ss = getSerializationService();

        try {
            MapSampleMetadataResolver.resolve(ss, ss.toData(new HazelcastJsonValue("{ \"test\": 10 }")), true, true);
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.GENERIC, e.getCode());
            assertTrue(e.getMessage().contains("JSON objects are not supported"));
        }
    }

    private void checkPrimitive(Object value, QueryDataType expectedType) {
        InternalSerializationService ss = getSerializationService();

        // Key
        MapSampleMetadata metadata = MapSampleMetadataResolver.resolve(ss, value, true, true);
        checkFields(metadata, field(KEY, expectedType, true));

        // Serialized key
        metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(value), true, true);
        checkFields(metadata, field(KEY, expectedType, true));

        // Value
        metadata = MapSampleMetadataResolver.resolve(ss, value, true, false);
        checkFields(metadata, field(VALUE, expectedType, false));

        // Serialized value
        metadata = MapSampleMetadataResolver.resolve(ss, ss.toData(value), true, false);
        checkFields(metadata, field(VALUE, expectedType, false));
    }

    private static void checkFields(MapSampleMetadata metadata, MapTableField... expectedFields) {
        TreeMap<String, TableField> expectedFieldMap = new TreeMap<>();

        for (MapTableField field : expectedFields) {
            expectedFieldMap.put(field.getName(), field);
        }

        LinkedHashMap<String, TableField> expectedFieldMap0 = new LinkedHashMap<>(expectedFieldMap);

        List<String> expectedFieldNames = new ArrayList<>(expectedFieldMap0.keySet());

        assertEquals(expectedFieldNames, new ArrayList<>(metadata.getFields().keySet()));

        for (String expectedFieldName : expectedFieldNames) {
            TableField expectedField = expectedFieldMap0.get(expectedFieldName);
            TableField field = metadata.getFields().get(expectedFieldName);

            assertEquals(expectedField, field);
        }
    }

    private void checkJavaTypes(Object object) {
        checkJavaTypes(object, true);
        checkJavaTypes(object, false);
        checkJavaTypes(getSerializationService().toData(object), true);
        checkJavaTypes(getSerializationService().toData(object), false);
    }

    private void checkJavaTypes(Object object, boolean key) {
        MapSampleMetadata metadata = MapSampleMetadataResolver.resolve(getSerializationService(), object, true, key);

        assertEquals(GenericQueryTargetDescriptor.INSTANCE, metadata.getDescriptor());

        checkFields(
            metadata,

            field("fBoolean", QueryDataType.BOOLEAN, key),
            field("fBooleanBoxed", QueryDataType.BOOLEAN, key),
            field("fByte", QueryDataType.TINYINT, key),
            field("fByteBoxed", QueryDataType.TINYINT, key),
            field("fShort", QueryDataType.SMALLINT, key),
            field("fShortBoxed", QueryDataType.SMALLINT, key),
            field("fInt", QueryDataType.INT, key),
            field("fIntBoxed", QueryDataType.INT, key),
            field("fLong", QueryDataType.BIGINT, key),
            field("fLongBoxed", QueryDataType.BIGINT, key),
            field("fFloat", QueryDataType.REAL, key),
            field("fFloatBoxed", QueryDataType.REAL, key),
            field("fDouble", QueryDataType.DOUBLE, key),
            field("fDoubleBoxed", QueryDataType.DOUBLE, key),

            field("fChar", QueryDataType.VARCHAR_CHARACTER, key),
            field("fCharBoxed", QueryDataType.VARCHAR_CHARACTER, key),
            field("fString", QueryDataType.VARCHAR, key),

            field("fBigInteger", QueryDataType.DECIMAL_BIG_INTEGER, key),
            field("fBigDecimal", QueryDataType.DECIMAL, key),

            field("fLocalTime", QueryDataType.TIME, key),
            field("fLocalDate", QueryDataType.DATE, key),
            field("fLocalDateTime", QueryDataType.TIMESTAMP, key),

            field("fDate", QueryDataType.TIMESTAMP_WITH_TZ_DATE, key),
            field("fCalendar", QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, key),
            field("fInstant", QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, key),
            field("fOffsetDateTime", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, key),
            field("fZonedDateTime", QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, key),

            hiddenField(key ? KEY : VALUE, QueryDataType.OBJECT, key)
        );
    }


    private static class JavaFields implements Serializable {
        public int publicField;
        int defaultField;
        protected int protectedField;
        private int privateField;
    }

    private static class JavaGetters implements Serializable {
        public int getPublicGetter() {
            return 0;
        }

        int getDefaultGetter() {
            return 0;
        }

        protected int getProtectedGetter() {
            return 0;
        }

        private int getPrivateGetter() {
            return 0;
        }

        public void getVoidPrimitive() {
            // No-op
        }

        public Void getVoid() {
            return null;
        }

        public boolean isBooleanIsGetter() {
            return true;
        }

        public boolean getBooleanGetGetter() {
            return true;
        }

        public Boolean isBooleanNonPrimitiveGetter() {
            return true;
        }

        public void isVoidIntegerPrimitive() {
            // No-op
        }

        public Void isVoidInteger() {
            return null;
        }

        public void isVoidPrimitive() {
            // No-op
        }

        public Void isVoid() {
            return null;
        }

        public int getIntWithParameter(int parameter) {
            return 0;
        }
    }

    private static class JavaFieldClashParent implements Serializable {
        public int field;
    }

    private static class JavaFieldClashChild extends JavaFieldClashParent {
        public long field;
    }

    private static class JavaFieldGetterClash implements Serializable {
        public int field;

        public long getField() {
            return 0L;
        }
    }

    private static class JavaTopFieldClash implements Serializable {
        public int __key;
    }

    private static class JavaTopGetterClash implements Serializable {
        public int get__key() {
            return 0;
        }

        public int getThis() {
            return 0;
        }
    }

    private static class JavaFieldTypes implements Serializable {
        public boolean fBoolean;
        public Boolean fBooleanBoxed;
        public byte fByte;
        public Byte fByteBoxed;
        public short fShort;
        public Short fShortBoxed;
        public int fInt;
        public Integer fIntBoxed;
        public long fLong;
        public Long fLongBoxed;
        public float fFloat;
        public Float fFloatBoxed;
        public double fDouble;
        public Double fDoubleBoxed;

        public char fChar;
        public Character fCharBoxed;
        public String fString;

        public BigInteger fBigInteger;
        public BigDecimal fBigDecimal;

        public LocalTime fLocalTime;
        public LocalDate fLocalDate;
        public LocalDateTime fLocalDateTime;

        public Date fDate;
        public Calendar fCalendar;
        public Instant fInstant;
        public OffsetDateTime fOffsetDateTime;
        public ZonedDateTime fZonedDateTime;
    }

    private static class JavaGetterTypes implements Serializable {
        public boolean getFBoolean() {
            return false;
        }

        public Boolean getFBooleanBoxed() {
            return false;
        }

        public byte getFByte() {
            return (byte) 0;
        }

        public Byte getFByteBoxed() {
            return null;
        }

        public short getFShort() {
            return (short) 0;
        }

        public Short getFShortBoxed() {
            return null;
        }

        public int getFInt() {
            return 0;
        }

        public Integer getFIntBoxed() {
            return null;
        }

        public long getFLong() {
            return 0L;
        }

        public Long getFLongBoxed() {
            return null;
        }

        public float getFFloat() {
            return 0f;
        }

        public Float getFFloatBoxed() {
            return null;
        }

        public double getFDouble() {
            return 0d;
        }

        public Double getFDoubleBoxed() {
            return null;
        }

        public char getFChar() {
            return (char) 0;
        }

        public Character getFCharBoxed() {
            return null;
        }

        public String getFString() {
            return null;
        }

        public BigInteger getFBigInteger() {
            return null;
        }

        public BigDecimal getFBigDecimal() {
            return null;
        }

        public LocalTime getFLocalTime() {
            return null;
        }

        public LocalDate getFLocalDate() {
            return null;
        }

        public LocalDateTime getFLocalDateTime() {
            return null;
        }

        public Date getFDate() {
            return null;
        }

        public Calendar getFCalendar() {
            return null;
        }

        public Instant getFInstant() {
            return null;
        }

        public OffsetDateTime getFOffsetDateTime() {
            return null;
        }

        public ZonedDateTime getFZonedDateTime() {
            return null;
        }
    }

    private static class JavaWithoutFields implements Serializable {
        // No-op.
    }

    private static class PortableParent implements Portable {

        public boolean fBoolean;
        public byte fByte;
        public char fChar;
        public short fShort;
        public int fInt;
        public long fLong;
        public float fFloat;
        public double fDouble;
        public String fString;
        public PortableChild fObject = new PortableChild();

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeBoolean(PORTABLE_BOOLEAN, fBoolean);
            writer.writeByte(PORTABLE_BYTE, fByte);
            writer.writeChar(PORTABLE_CHAR, fChar);
            writer.writeShort(PORTABLE_SHORT, fShort);
            writer.writeInt(PORTABLE_INT, fInt);
            writer.writeLong(PORTABLE_LONG, fLong);
            writer.writeFloat(PORTABLE_FLOAT, fFloat);
            writer.writeDouble(PORTABLE_DOUBLE, fDouble);
            writer.writeUTF(PORTABLE_STRING, fString);
            writer.writePortable(PORTABLE_OBJECT, fObject);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            fBoolean = reader.readBoolean(PORTABLE_BOOLEAN);
            fByte = reader.readByte(PORTABLE_BYTE);
            fChar = reader.readChar(PORTABLE_CHAR);
            fShort = reader.readShort(PORTABLE_SHORT);
            fInt = reader.readInt(PORTABLE_INT);
            fLong = reader.readLong(PORTABLE_LONG);
            fFloat = reader.readFloat(PORTABLE_FLOAT);
            fDouble = reader.readDouble(PORTABLE_DOUBLE);
            fString = reader.readUTF(PORTABLE_STRING);
            fObject = reader.readPortable(PORTABLE_OBJECT);
        }
    }

    private static class PortableChild implements Portable {
        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 3;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            // No-op
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            // No-op
        }
    }

    private static class PortableClash implements Portable {

        private int k;
        private int v;

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 4;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt(KEY, k);
            writer.writeInt(VALUE, v);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            k = reader.readInt(KEY);
            v = reader.readInt(VALUE);
        }
    }

    private static class PortableWithoutFields implements Portable {
        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 5;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            // No-op.
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            // No-op.
        }
    }
}
