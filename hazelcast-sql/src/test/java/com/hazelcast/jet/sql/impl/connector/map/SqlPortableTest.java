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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.spi.impl.NodeEngine;
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
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder.withDefaults;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.portable;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.emptyList;
import static java.util.Map.entry;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

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
    private static ClassDefinition allTypesValueClassDefinition;
    private static ClassDefinition emptyClassDefinition;

    private static SqlMapping portableMapping(String name) {
        return new SqlMapping(name, IMapSqlConnector.class)
                .options(OPTION_KEY_FORMAT, PORTABLE_FORMAT,
                         OPTION_KEY_FACTORY_ID, PERSON_ID_FACTORY_ID,
                         OPTION_KEY_CLASS_ID, PERSON_ID_CLASS_ID,
                         OPTION_KEY_CLASS_VERSION, PERSON_ID_CLASS_VERSION,
                         OPTION_VALUE_FORMAT, PORTABLE_FORMAT,
                         OPTION_VALUE_FACTORY_ID, PERSON_FACTORY_ID,
                         OPTION_VALUE_CLASS_ID, PERSON_CLASS_ID,
                         OPTION_VALUE_CLASS_VERSION, PERSON_CLASS_VERSION);
    }

    @BeforeClass
    // reusing ClassDefinitions as schema does not change
    public static void beforeClass() {
        initialize(1, smallInstanceConfig().setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true"));
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

        allTypesValueClassDefinition =
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
        portableMapping(name).create();

        sqlService.execute("SINK INTO " + name + " VALUES (1, null)");

        Entry<Data, Data> entry = randomEntryFrom(name);
        assertEquals(
                portable(personIdClassDefinition).setInt32("id", 1).build(),
                serializationService.readAsInternalGenericRecord(entry.getKey()));
        assertEquals(
                portable(personClassDefinition)
                        .setInt32("id", 0)
                        .setString("name", null)
                        .build(),
                serializationService.readAsInternalGenericRecord(entry.getValue()));

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, null))
        );
    }

    @Test
    public void when_nullIntoPrimitive_then_fails() {
        String name = randomName();
        portableMapping(name).create();

        assertThatThrownBy(() -> sqlService.execute("SINK INTO " + name + " VALUES (null, 'Alice')"))
                .hasMessageContaining("Cannot set NULL to a primitive field");
    }

    @Test
    public void test_fieldsShadowing() throws IOException {
        String name = randomName();
        portableMapping(name).create();

        sqlService.execute("SINK INTO " + name + " (id, name) VALUES (1, 'Alice')");

        Entry<Data, Data> entry = randomEntryFrom(name);
        assertEquals(
                portable(personIdClassDefinition).setInt32("id", 1).build(),
                serializationService.readAsInternalGenericRecord(entry.getKey()));
        assertEquals(
                portable(personClassDefinition)
                        .setInt32("id", 0)
                        .setString("name", "Alice")
                        .build(),
                serializationService.readAsInternalGenericRecord(entry.getValue()));

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() throws IOException {
        String name = randomName();
        portableMapping(name)
                .fields("key_id INT EXTERNAL NAME \"__key.id\"",
                        "value_id INT EXTERNAL NAME \"this.id\"")
                .create();

        sqlService.execute("SINK INTO " + name + " (value_id, key_id) VALUES (2, 1)");

        Entry<Data, Data> entry = randomEntryFrom(name);
        assertEquals(
                portable(personIdClassDefinition).setInt32("id", 1).build(),
                serializationService.readAsInternalGenericRecord(entry.getKey()));
        assertEquals(
                withDefaults(personClassDefinition).setInt32("id", 2).build(),
                serializationService.readAsInternalGenericRecord(entry.getValue()));

        assertRowsAnyOrder(
                "SELECT key_id, value_id FROM " + name,
                List.of(new Row(1, 2))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = randomName();
        portableMapping(name).create();

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        // alter schema
        portableMapping(name)
                .options(OPTION_VALUE_CLASS_VERSION, PERSON_CLASS_VERSION + 1)
                .createOrReplace();

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES (2, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(1, "Alice", null),
                        new Row(2, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void test_fieldsExtensions() {
        String name = randomName();
        portableMapping(name)
                .options(OPTION_VALUE_CLASS_VERSION, PERSON_CLASS_VERSION + 1)
                .create();

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice', 123456789)");

        // alter schema
        portableMapping(name)
                .fields("name VARCHAR",
                        "ssn BIGINT")
                .createOrReplace();

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES ('Bob', null)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(
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
        portableMapping(to)
                .options(OPTION_VALUE_FACTORY_ID, ALL_TYPES_FACTORY_ID,
                         OPTION_VALUE_CLASS_ID, ALL_TYPES_CLASS_ID,
                         OPTION_VALUE_CLASS_VERSION, ALL_TYPES_CLASS_VERSION)
                .create();

        sqlService.execute("SINK INTO " + to + " " +
                "SELECT 13, 'a', string, \"boolean\", byte, short, \"int\", long, \"float\", \"double\", " +
                        "\"decimal\", \"time\", \"date\", \"timestamp\", timestampTz, \"object\" " +
                "FROM " + from + " f");

        GenericRecord expected = portable(allTypesValueClassDefinition)
                .setString("string", "string")
                .setChar("character", 'a')
                .setBoolean("boolean", true)
                .setInt8("byte", (byte) 127)
                .setInt16("short", (short) 32767)
                .setInt32("int", 2147483647)
                .setInt64("long", 9223372036854775807L)
                .setFloat32("float", 1234567890.1F)
                .setFloat64("double", 123451234567890.1D)
                .setDecimal("decimal", new BigDecimal("9223372036854775.123"))
                .setTime("time", LocalTime.of(12, 23, 34))
                .setDate("date", LocalDate.of(2020, 4, 15))
                .setTimestamp("timestamp", LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000))
                .setTimestampWithTimezone("timestampTz",
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC))
                .setGenericRecord("object", null)
                .build();
        GenericRecord actual = serializationService.readAsInternalGenericRecord(randomEntryFrom(to).getValue());
        assertEquals(expected, actual);

        assertRowsAnyOrder(
                "SELECT * FROM " + to,
                List.of(new Row(
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
    public void test_topLevelPathExtraction() {
        String name = randomName();
        portableMapping(name).create();

        sqlService.execute("INSERT INTO " + name + " (id, name) VALUES (1, 'Alice')");

        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(
                        portable(personIdClassDefinition).setInt32("id", 1).build(),
                        portable(personClassDefinition)
                                .setInt32("id", 0)
                                .setString("name", "Alice")
                                .build()
                ))
        );
    }

    @Test
    public void when_topLevelPathWithoutCustomType_then_fail() {
        assertThatThrownBy(() ->
                portableMapping("test")
                        .fields("__key INT",
                                "name VARCHAR EXTERNAL NAME \"this.name\"")
                        .create())
                .hasMessage("'__key' field must be used with a user-defined type");

        assertThatThrownBy(() ->
                portableMapping("test")
                        .fields("id INT EXTERNAL NAME \"this.id\"",
                                "this VARCHAR")
                        .create())
                .hasMessage("'this' field must be used with a user-defined type");
    }

    @Test
    public void test_explicitTopLevelPath() {
        new SqlType("PersonId").fields("id INT").create();
        new SqlType("Person").fields("name VARCHAR").create();

        String name = randomName();
        portableMapping(name)
                .fields("__key PersonId",
                        "this Person")
                .create();

        sqlService.execute("INSERT INTO " + name + " VALUES (?, ?)",
                portable(personIdClassDefinition).setInt32("id", 1).build(),
                withDefaults(personClassDefinition).setString("name", "Alice").build());

        assertRowsAnyOrder(
                "SELECT (__key).id, (this).name FROM " + name,
                List.of(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_writingToImplicitTopLevelPath() {
        String name = randomName();
        portableMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        sqlService.execute("INSERT INTO " + name + " (__key, name) VALUES (?, ?)",
                portable(personIdClassDefinition).setInt32("id", 1).build(),
                "Alice");
        sqlService.execute("INSERT INTO " + name + " (id, this) VALUES (?, ?)",
                2,
                withDefaults(personClassDefinition).setString("name", "Bob").build());

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
    }

    @Test
    public void test_derivesClassDefinitionFromSchema() throws IOException {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = randomName();
        portableMapping(to)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "string VARCHAR",
                        "\"boolean\" BOOLEAN",
                        "byte TINYINT",
                        "short SMALLINT",
                        "\"int\" INT",
                        "long BIGINT",
                        "\"float\" REAL",
                        "\"double\" DOUBLE",
                        "\"decimal\" DECIMAL",
                        "\"time\" TIME",
                        "\"date\" DATE",
                        "\"timestamp\" TIMESTAMP",
                        "timestampTz TIMESTAMP WITH TIME ZONE")
                .options(OPTION_VALUE_FACTORY_ID, 997,
                         OPTION_VALUE_CLASS_ID, 998,
                         OPTION_VALUE_CLASS_VERSION, 999)
                .create();

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

        ClassDefinition derivedClassDefinition = serializationService.getPortableContext()
                .lookupClassDefinition(997, 998, 999);
        GenericRecord expected = portable(derivedClassDefinition)
                .setString("string", "string")
                .setBoolean("boolean", true)
                .setInt8("byte", (byte) 127)
                .setInt16("short", (short) 32767)
                .setInt32("int", 2147483647)
                .setInt64("long", 9223372036854775807L)
                .setFloat32("float", 1234567890.1F)
                .setFloat64("double", 123451234567890.1D)
                .setDecimal("decimal", new BigDecimal("9223372036854775.123"))
                .setTime("time", LocalTime.of(12, 23, 34))
                .setDate("date", LocalDate.of(2020, 4, 15))
                .setTimestamp("timestamp", LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000))
                .setTimestampWithTimezone("timestampTz",
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC))
                .build();
        GenericRecord actual = serializationService.readAsInternalGenericRecord(randomEntryFrom(to).getValue());
        assertEquals(expected, actual);

        assertRowsAnyOrder(
                "SELECT * FROM " + to,
                List.of(new Row(
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
        portableMapping(name)
                .options(OPTION_KEY_FACTORY_ID, EMPTY_TYPES_FACTORY_ID,
                         OPTION_KEY_CLASS_ID, EMPTY_TYPES_CLASS_ID,
                         OPTION_KEY_CLASS_VERSION, EMPTY_TYPES_CLASS_VERSION,
                         OPTION_VALUE_FACTORY_ID, EMPTY_TYPES_FACTORY_ID,
                         OPTION_VALUE_CLASS_ID, EMPTY_TYPES_CLASS_ID,
                         OPTION_VALUE_CLASS_VERSION, EMPTY_TYPES_CLASS_VERSION)
                .create();

        GenericRecord record = portable(emptyClassDefinition).build();
        instance().getMap(name).put(record, record);

        assertRowsAnyOrder("SELECT __key, this FROM " + name, List.of(new Row(record, record)));
    }

    @Test
    public void when_unknownClassDef_then_wholeValueMapped() {
        String name = randomName();
        portableMapping(name)
                .options(OPTION_KEY_FACTORY_ID, 9999,
                        OPTION_KEY_CLASS_ID, 9999,
                        OPTION_KEY_CLASS_VERSION, 9999,
                        OPTION_VALUE_FACTORY_ID, 9998,
                        OPTION_VALUE_CLASS_ID, 9998,
                        OPTION_VALUE_CLASS_VERSION, 9998)
                .create();

        assertRowsAnyOrder("SELECT __key, this FROM " + name, emptyList());
    }

    @Test
    public void test_classDefMappingMismatch() {
        assertThatThrownBy(() ->
                new SqlMapping(randomName(), IMapSqlConnector.class)
                        .fields("id VARCHAR")
                        .options(OPTION_KEY_FORMAT, "int",
                                 OPTION_VALUE_FORMAT, PORTABLE_FORMAT,
                                 OPTION_VALUE_FACTORY_ID, PERSON_ID_FACTORY_ID,
                                 OPTION_VALUE_CLASS_ID, PERSON_ID_CLASS_ID,
                                 OPTION_VALUE_CLASS_VERSION, PERSON_ID_CLASS_VERSION)
                        .create())
                .hasMessage("Mismatch between declared and resolved type: id");

        String name = randomName();
        // We map a non-existent field. This works, but will fail at runtime
        new SqlMapping(name, IMapSqlConnector.class)
                .fields("foo INT")
                .options(OPTION_KEY_FORMAT, "int",
                        OPTION_VALUE_FORMAT, PORTABLE_FORMAT,
                        OPTION_VALUE_FACTORY_ID, PERSON_ID_FACTORY_ID,
                        OPTION_VALUE_CLASS_ID, PERSON_ID_CLASS_ID,
                        OPTION_VALUE_CLASS_VERSION, PERSON_ID_CLASS_VERSION)
                .create();

        assertThatThrownBy(() ->
                sqlService.execute("insert into " + name + "(__key, foo) values(1, 1)"))
                .hasMessage("Field \"foo\" doesn't exist in Portable class definition");

        sqlService.execute("insert into " + name + "(__key, foo) values(1, null)");
        sqlService.execute("insert into " + name + "(__key) values(2)");

        assertRowsAnyOrder("select __key, foo from " + name,
                rows(2,
                        1, null,
                        2, null));
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
