/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SqlPortableTest extends SqlTestSupport {

    private static SqlService sqlService;

    private static final int PERSON_ID_FACTORY_ID = 1;
    private static final int PERSON_ID_CLASS_ID = 2;
    private static final int PERSON_ID_CLASS_VERSION = 3;

    private static final int PERSON_FACTORY_ID = 4;
    private static final int PERSON_CLASS_ID = 5;
    private static final int PERSON_CLASS_VERSION = 6;

    private static final int ALL_TYPES_FACTORY_ID = 7;
    private static final int ALL_TYPES_CLASS_ID = 8;
    private static final int ALL_TYPES_CLASS_VERSION = 9;

    private static final int EMPTY_TYPES_FACTORY_ID = 10;
    private static final int EMPTY_TYPES_CLASS_ID = 11;
    private static final int EMPTY_TYPES_CLASS_VERSION = 12;

    private static InternalSerializationService serializationService;
    private static ClassDefinition personIdClassDefinition;
    private static ClassDefinition personClassDefinition;
    private static ClassDefinition emptyClassDefinition;

    @BeforeClass
    // reusing ClassDefinitions as schema does not change
    public static void beforeClass() {
        initialize(1, null);
        sqlService = instance().getSql();

        serializationService = Util.getSerializationService(instance());

        personIdClassDefinition =
                new ClassDefinitionBuilder(PERSON_ID_FACTORY_ID, PERSON_ID_CLASS_ID, PERSON_ID_CLASS_VERSION)
                        .addIntField("id")
                        .build();
        serializationService.getPortableContext().registerClassDefinition(personIdClassDefinition);

        personClassDefinition =
                new ClassDefinitionBuilder(PERSON_FACTORY_ID, PERSON_CLASS_ID, PERSON_CLASS_VERSION)
                        .addIntField("id")
                        .addStringField("name")
                        .build();
        serializationService.getPortableContext().registerClassDefinition(personClassDefinition);

        ClassDefinition evolvedPersonClassDefinition =
                new ClassDefinitionBuilder(PERSON_FACTORY_ID, PERSON_CLASS_ID, PERSON_CLASS_VERSION + 1)
                        .addIntField("id")
                        .addStringField("name")
                        .addLongField("ssn")
                        .build();
        serializationService.getPortableContext().registerClassDefinition(evolvedPersonClassDefinition);

        ClassDefinition allTypesValueClassDefinition =
                new ClassDefinitionBuilder(ALL_TYPES_FACTORY_ID, ALL_TYPES_CLASS_ID, ALL_TYPES_CLASS_VERSION)
                        .addCharField("character")
                        .addStringField("string")
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
                        .addPortableField("object", personClassDefinition)
                        .build();
        serializationService.getPortableContext().registerClassDefinition(allTypesValueClassDefinition);

        emptyClassDefinition =
                new ClassDefinitionBuilder(EMPTY_TYPES_FACTORY_ID, EMPTY_TYPES_CLASS_ID, EMPTY_TYPES_CLASS_VERSION)
                        .build();
        serializationService.getPortableContext().registerClassDefinition(emptyClassDefinition);
    }

    @Test
    public void test_nulls() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " VALUES (1, null)");

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord keyRecord = serializationService.readAsInternalGenericRecord(entry.getKey());
        assertThat(keyRecord.getInt32("id")).isEqualTo(1);

        InternalGenericRecord valueRecord = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueRecord.getInt32("id")).isEqualTo(0); // default portable value
        assertThat(valueRecord.getString("name")).isNull();

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, null))
        );
    }

    @Test
    public void when_nullIntoPrimitive_then_fails() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        assertThatThrownBy(() -> sqlService.execute("SINK INTO " + name + " VALUES (null, 'Alice')"))
                .hasMessageContaining("Cannot set NULL to a primitive field");
    }

    @Test
    public void test_fieldsShadowing() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " (id, name) VALUES (1, 'Alice')");

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord keyRecord = serializationService.readAsInternalGenericRecord(entry.getKey());
        assertThat(keyRecord.getInt32("id")).isEqualTo(1);

        InternalGenericRecord valueRecord = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueRecord.getInt32("id")).isEqualTo(0);
        assertThat(valueRecord.getString("name")).isEqualTo("Alice");

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " (value_id, key_id) VALUES (2, 1)");

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord keyRecord = serializationService.readAsInternalGenericRecord(entry.getKey());
        assertThat(keyRecord.getInt32("id")).isEqualTo(1);

        InternalGenericRecord valueRecord = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueRecord.getInt32("id")).isEqualTo(2);

        assertRowsAnyOrder(
                "SELECT key_id, value_id FROM " + name,
                singletonList(new Row(1, 2))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + (PERSON_CLASS_VERSION + 1) + '\''
                + ")"
        );

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES (2, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(1, "Alice", null),
                        new Row(2, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void test_fieldsExtensions() {
        String name = randomName();
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + (PERSON_CLASS_VERSION + 1) + '\''
                + ")"
        );

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice', 123456789)");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " ("
                + "name VARCHAR"
                + ", ssn BIGINT"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + '\''
                + ")"
        );

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES ('Bob', null)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row("Alice", 123456789L),
                        new Row("Bob", null)
                )
        );
    }

    @Test
    public void test_allTypes() throws IOException {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = randomName();
        sqlService.execute("CREATE MAPPING " + to + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + ALL_TYPES_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + ALL_TYPES_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + ALL_TYPES_CLASS_VERSION + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + to + " " +
                "SELECT 13, 'a', string, \"boolean\", byte, short, \"int\", long, \"float\", \"double\", \"decimal\", " +
                "\"time\", \"date\", \"timestamp\", timestampTz, \"object\" " +
                "FROM " + from + " f");

        InternalGenericRecord valueRecord = serializationService
                .readAsInternalGenericRecord(randomEntryFrom(to).getValue());
        assertThat(valueRecord.getString("string")).isEqualTo("string");
        assertThat(valueRecord.getChar("character")).isEqualTo('a');
        assertThat(valueRecord.getBoolean("boolean")).isTrue();
        assertThat(valueRecord.getInt8("byte")).isEqualTo((byte) 127);
        assertThat(valueRecord.getInt16("short")).isEqualTo((short) 32767);
        assertThat(valueRecord.getInt32("int")).isEqualTo(2147483647);
        assertThat(valueRecord.getInt64("long")).isEqualTo(9223372036854775807L);
        assertThat(valueRecord.getFloat32("float")).isEqualTo(1234567890.1F);
        assertThat(valueRecord.getFloat64("double")).isEqualTo(123451234567890.1D);
        assertThat(valueRecord.getDecimal("decimal")).isEqualTo(new BigDecimal("9223372036854775.123"));
        assertThat(valueRecord.getTime("time")).isEqualTo(LocalTime.of(12, 23, 34));
        assertThat(valueRecord.getDate("date")).isEqualTo(LocalDate.of(2020, 4, 15));
        assertThat(valueRecord.getTimestamp("timestamp"))
                .isEqualTo(LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000));
        assertThat(valueRecord.getTimestampWithTimezone("timestampTz"))
                .isEqualTo(OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC));
        assertThat(valueRecord.getGenericRecord("object")).isNull();

        assertRowsAnyOrder(
                "SELECT * FROM " + to,
                singletonList(new Row(
                        13,
                        "a",
                        "string",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1F,
                        123451234567890.1D,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        null
                ))
        );
    }

    @Test
    public void test_writingToTopLevelWhileNestedFieldMapped_explicit() {
        test_writingToTopLevelWhileNestedFieldMapped(true);
    }

    @Test
    public void test_writingToTopLevelWhileNestedFieldMapped_implicit() {
        test_writingToTopLevelWhileNestedFieldMapped(false);
    }

    public void test_writingToTopLevelWhileNestedFieldMapped(boolean explicit) {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + "("
                + "__key INT"
                + (explicit ? ", this OBJECT" : "")
                + ", name VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + "'\n"
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + "'\n"
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + "'\n"
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + "'\n"
                + ")"
        );

        if (explicit) {
            assertThatThrownBy(() ->
                    sqlService.execute("SINK INTO " + mapName + " VALUES(1, null, 'foo')"))
                    .isInstanceOf(HazelcastSqlException.class)
                    .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");
        }

        assertThatThrownBy(() ->
                sqlService.execute("SINK INTO " + mapName + "(__key, this) VALUES(1, null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        sqlService.execute("SINK INTO " + mapName + (explicit ? "(__key, name)" : "") + " VALUES (1, 'foo')");

        Iterator<SqlRow> resultIter = sqlService.execute("SELECT __key, this, name FROM " + mapName).iterator();
        SqlRow row = resultIter.next();
        assertEquals(1, (int) row.getObject(0));
        assertInstanceOf(GenericRecord.class, row.getObject(1));
        assertEquals("foo", row.getObject(2));
        assertFalse(resultIter.hasNext());
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + PERSON_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + PERSON_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + PERSON_CLASS_VERSION + '\''
                + ")"
        );
        sqlService.execute("SINK INTO " + name + " (id, name) VALUES (1, 'Alice')");

        Iterator<SqlRow> rowIterator = sqlService.execute("SELECT __key, this FROM " + name).iterator();
        SqlRow row = rowIterator.next();
        assertFalse(rowIterator.hasNext());

        assertEquals(new PortableGenericRecordBuilder(personIdClassDefinition)
                .setInt32("id", 1)
                .build(), row.getObject(0));
        assertEquals(new PortableGenericRecordBuilder(personClassDefinition)
                .setInt32("id", 0)
                .setString("name", "Alice")
                .build(), row.getObject(1));
    }

    @Test
    public void test_derivesClassDefinitionFromSchema() throws IOException {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = randomName();
        sqlService.execute("CREATE MAPPING " + to + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", string VARCHAR"
                + ", \"boolean\" BOOLEAN"
                + ", byte TINYINT"
                + ", short SMALLINT"
                + ", \"int\" INT"
                + ", long BIGINT"
                + ", \"float\" REAL"
                + ", \"double\" DOUBLE"
                + ", \"decimal\" DECIMAL"
                + ", \"time\" TIME"
                + ", \"date\" DATE"
                + ", \"timestamp\" TIMESTAMP"
                + ", timestampTz TIMESTAMP WITH TIME ZONE"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + PERSON_ID_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + PERSON_ID_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + PERSON_ID_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + 997 + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + 998 + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + 999 + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + to + " SELECT "
                + "13"
                + ", string "
                + ", \"boolean\" "
                + ", byte "
                + ", short "
                + ", \"int\" "
                + ", long "
                + ", \"float\" "
                + ", \"double\" "
                + ", \"decimal\" "
                + ", \"time\" "
                + ", \"date\" "
                + ", \"timestamp\" "
                + ", timestampTz "
                + "FROM " + from
        );

        InternalGenericRecord valueRecord = serializationService
                .readAsInternalGenericRecord(randomEntryFrom(to).getValue());
        assertThat(valueRecord.getString("string")).isEqualTo("string");
        assertThat(valueRecord.getBoolean("boolean")).isTrue();
        assertThat(valueRecord.getInt8("byte")).isEqualTo((byte) 127);
        assertThat(valueRecord.getInt16("short")).isEqualTo((short) 32767);
        assertThat(valueRecord.getInt32("int")).isEqualTo(2147483647);
        assertThat(valueRecord.getInt64("long")).isEqualTo(9223372036854775807L);
        assertThat(valueRecord.getFloat32("float")).isEqualTo(1234567890.1F);
        assertThat(valueRecord.getFloat64("double")).isEqualTo(123451234567890.1D);
        assertThat(valueRecord.getDecimal("decimal")).isEqualTo(new BigDecimal("9223372036854775.123"));
        assertThat(valueRecord.getTime("time")).isEqualTo(LocalTime.of(12, 23, 34));
        assertThat(valueRecord.getDate("date")).isEqualTo(LocalDate.of(2020, 4, 15));
        assertThat(valueRecord.getTimestamp("timestamp"))
                .isEqualTo(LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000));
        assertThat(valueRecord.getTimestampWithTimezone("timestampTz"))
                .isEqualTo(OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC));

        assertRowsAnyOrder(
                "SELECT * FROM " + to,
                singletonList(new Row(
                        13,
                        "string",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC)
                ))
        );
    }

    @Test
    public void when_noFieldsResolved_then_wholeValueMapped() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='" + EMPTY_TYPES_FACTORY_ID + '\''
                + ", '" + OPTION_KEY_CLASS_ID + "'='" + EMPTY_TYPES_CLASS_ID + '\''
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='" + EMPTY_TYPES_CLASS_VERSION + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='" + EMPTY_TYPES_FACTORY_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_ID + "'='" + EMPTY_TYPES_CLASS_ID + '\''
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='" + EMPTY_TYPES_CLASS_VERSION + '\''
                + ")"
        );

        GenericRecord record = new PortableGenericRecordBuilder(emptyClassDefinition).build();
        instance().getMap(name).put(record, record);

        assertRowsAnyOrder("SELECT __key, this FROM " + name, singletonList(new Row(record, record)));
    }

    @Test
    public void when_unknownClassDef_then_wholeValueMapped() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_KEY_FACTORY_ID + "'='9999'"
                + ", '" + OPTION_KEY_CLASS_ID + "'='9999'"
                + ", '" + OPTION_KEY_CLASS_VERSION + "'='9999'"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + PORTABLE_FORMAT + '\''
                + ", '" + OPTION_VALUE_FACTORY_ID + "'='9998'"
                + ", '" + OPTION_VALUE_CLASS_ID + "'='9998'"
                + ", '" + OPTION_VALUE_CLASS_VERSION + "'='9998'"
                + ")"
        );

        assertRowsAnyOrder("SELECT __key, this FROM " + name, emptyList());
    }

    @SuppressWarnings({"OptionalGetWithoutIsPresent", "unchecked", "rawtypes"})
    private static Entry<Data, Data> randomEntryFrom(String mapName) {
        NodeEngine engine = getNodeEngine(instance());
        MapService service = engine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();

        return Arrays.stream(context.getPartitionContainers())
                .map(partitionContainer -> partitionContainer.getExistingRecordStore(mapName))
                .filter(Objects::nonNull)
                .flatMap(store -> {
                    Iterator<Entry<Data, Record>> iterator = store.iterator();
                    return stream(spliteratorUnknownSize(iterator, ORDERED), false);
                })
                .map(entry -> entry(entry.getKey(), (Data) entry.getValue().getValue()))
                .findFirst()
                .get();
    }
}
