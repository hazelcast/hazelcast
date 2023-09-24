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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
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

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.compact;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static java.time.ZoneOffset.UTC;
import static java.util.Map.entry;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SqlCompactTest extends SqlTestSupport {
    private static final String PERSON_ID_TYPE_NAME = "personId";
    private static final String PERSON_TYPE_NAME = "person";
    private static final String ALL_TYPES_TYPE_NAME = "allTypes";

    private static SqlService sqlService;
    private static InternalSerializationService serializationService;

    @BeforeClass
    public static void setup() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");
        CompactSerializationConfig compactSerializationConfig =
                config.getSerializationConfig().getCompactSerializationConfig();

        // Registering this class to the member to see whether it has any effect
        // on the tests, none of which has the same schema.
        compactSerializationConfig.addSerializer(new CompactSerializer<Person>() {
            @Nonnull
            @Override
            public Person read(@Nonnull CompactReader reader) {
                Person person = new Person();
                if (reader.getFieldKind("surname") == FieldKind.STRING) {
                    person.surname = reader.readString("surname");
                } else {
                    person.surname = "NotAssigned";
                }
                return person;
            }

            @Override
            public void write(@Nonnull CompactWriter writer, @Nonnull Person person) {
                writer.writeString("surname", person.surname);
            }

            @Nonnull
            @Override
            public String getTypeName() {
                return PERSON_TYPE_NAME;
            }

            @Nonnull
            @Override
            public Class<Person> getCompactClass() {
                return Person.class;
            }
        });

        ClientConfig clientConfig = new ClientConfig();
        initializeWithClient(1, config, clientConfig);

        sqlService = instance().getSql();
        serializationService = Util.getSerializationService(instance());
    }

    private static SqlMapping compactMapping(String name, String keyCompactTypeName, String valueCompactTypeName) {
        return new SqlMapping(name, IMapSqlConnector.class)
                .options(OPTION_KEY_FORMAT, COMPACT_FORMAT,
                         OPTION_KEY_COMPACT_TYPE_NAME, keyCompactTypeName,
                         OPTION_VALUE_FORMAT, COMPACT_FORMAT,
                         OPTION_VALUE_COMPACT_TYPE_NAME, valueCompactTypeName);
    }

    @Test
    public void test_objectIsNotSupported() {
        String name = randomName();
        assertThatThrownBy(() ->
                compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                        .fields("key_id INT",
                                "object OBJECT")
                        .create())
                .hasMessageContaining("Cannot derive Compact type for 'object:OBJECT'");
    }

    @Test
    public void test_readToClassWithNonNulls() {
        String name = randomName();
        new SqlMapping(name, IMapSqlConnector.class)
                .fields(" __key INT",
                        "b BOOLEAN",
                        "bt TINYINT",
                        "s SMALLINT",
                        "i INTEGER",
                        "l BIGINT",
                        "f REAL",
                        "d DOUBLE")
                .options(OPTION_KEY_FORMAT, "integer",
                         OPTION_VALUE_FORMAT, COMPACT_FORMAT,
                         OPTION_VALUE_COMPACT_TYPE_NAME, Primitives.class.getName())
                .create();

        sqlService.execute("SINK INTO " + name + " VALUES (1, true, 2, 3, 4, 5, 12.321, 124.311)");

        IMap<Object, Object> map = client().getMap(name);
        assertEquals(new Primitives(true, (byte) 2, (short) 3, 4, 5, 12.321f, 124.311), map.get(1));
    }

    @Test
    public void test_insertNulls() throws IOException {
        String name = randomName();
        new SqlMapping(name, IMapSqlConnector.class)
                .fields("__key INT",
                        "b BOOLEAN",
                        "st VARCHAR",
                        "bt TINYINT",
                        "s SMALLINT",
                        "i INTEGER",
                        "l BIGINT",
                        "bd DECIMAL",
                        "f REAL",
                        "d DOUBLE",
                        "t TIME",
                        "dt DATE",
                        "tmstmp TIMESTAMP",
                        "tmstmpTz TIMESTAMP WITH TIME ZONE")
                .options(OPTION_KEY_FORMAT, "integer",
                         OPTION_VALUE_FORMAT, COMPACT_FORMAT,
                         OPTION_VALUE_COMPACT_TYPE_NAME, PERSON_TYPE_NAME)
                .create();

        sqlService.execute("SINK INTO " + name + " VALUES (1, null, null, null, "
                + "null, null, null, null, null, null, null, null, null, null )");

        Entry<Data, Data> entry = randomEntryFrom(name);

        GenericRecord expected = compact(PERSON_TYPE_NAME)
                .setNullableBoolean("b", null)
                .setString("st", null)
                .setNullableInt8("bt", null)
                .setNullableInt16("s", null)
                .setNullableInt32("i", null)
                .setNullableInt64("l", null)
                .setDecimal("bd", null)
                .setNullableFloat32("f", null)
                .setNullableFloat64("d", null)
                .setTime("t", null)
                .setDate("dt", null)
                .setTimestamp("tmstmp", null)
                .setTimestampWithTimezone("tmstmpTz", null)
                .build();
        GenericRecord actual = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertEquals(expected, actual);

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, null, null, null, null, null, null, null, null, null, null, null, null, null))
        );
    }

    @Test
    public void test_readNonNullKindsOfCompactViaSQL() {
        String name = randomName();
        new SqlMapping(name, IMapSqlConnector.class)
                .fields("__key INT",
                        "b BOOLEAN",
                        "st VARCHAR",
                        "bt TINYINT",
                        "s SMALLINT",
                        "i INTEGER",
                        "l BIGINT",
                        "bd DECIMAL",
                        "f REAL",
                        "d DOUBLE",
                        "t TIME",
                        "dt DATE",
                        "tmstmp TIMESTAMP",
                        "tmstmpTz TIMESTAMP WITH TIME ZONE")
                .options(OPTION_KEY_FORMAT, "integer",
                         OPTION_VALUE_FORMAT, COMPACT_FORMAT,
                         OPTION_VALUE_COMPACT_TYPE_NAME, PERSON_TYPE_NAME)
                .create();

        boolean b = true;
        String st = "test";
        byte bt = (byte) 12;
        short s = 1312;
        int i = 12314;
        long l = 23214141L;
        BigDecimal bd = BigDecimal.TEN;
        float f = 13221321.213213f;
        double d = 13221321.213213d;
        LocalDate dt = LocalDate.now();
        LocalTime t = LocalTime.now();
        LocalDateTime tmstmp = LocalDateTime.now();
        OffsetDateTime tmstmpTz = OffsetDateTime.now();

        GenericRecord record = compact(PERSON_TYPE_NAME)
                .setBoolean("b", b)
                .setString("st", st)
                .setInt8("bt", bt)
                .setInt16("s", s)
                .setInt32("i", i)
                .setInt64("l", l)
                .setDecimal("bd", bd)
                .setFloat32("f", f)
                .setFloat64("d", d)
                .setTime("t", t)
                .setDate("dt", dt)
                .setTimestamp("tmstmp", tmstmp)
                .setTimestampWithTimezone("tmstmpTz", tmstmpTz)
                .build();

        IMap<Object, Object> map = client().getMap(name);
        map.put(1, record);

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, b, st, bt, s, i, l, bd, f, d, t, dt, tmstmp, tmstmpTz))
        );
    }

    @Test
    public void test_emptyColumnListIsNotAllowed() {
        assertThatThrownBy(() -> compactMapping("test", PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME).create())
                .hasMessageContaining("Column list is required for Compact format");
    }

    @Test
    public void test_fieldsMapping() throws IOException {
        String name = randomName();
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("key_id INT EXTERNAL NAME \"__key.id\"",
                        "value_id INT EXTERNAL NAME \"this.id\"")
                .create();

        sqlService.execute("SINK INTO " + name + " (value_id, key_id) VALUES (2, 1)");

        Entry<Data, Data> entry = randomEntryFrom(name);
        assertEquals(
                compact(PERSON_ID_TYPE_NAME).setNullableInt32("id", 1).build(),
                serializationService.readAsInternalGenericRecord(entry.getKey()));
        assertEquals(
                compact(PERSON_TYPE_NAME).setNullableInt32("id", 2).build(),
                serializationService.readAsInternalGenericRecord(entry.getValue()));

        assertRowsAnyOrder(
                "SELECT key_id, value_id FROM " + name,
                List.of(new Row(1, 2))
        );
    }

    @Test
    public void test_schemaEvolution_fieldAdded() {
        String name = randomName();
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("key_id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        // alter schema
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT")
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
    public void test_schemaEvolution_fieldRemoved() {
        String name = randomName();
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT")
                .create();

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice', 123456789)");

        // alter schema
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .createOrReplace();

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES (2, 'Bob')");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
    }

    @Test
    public void test_allTypes() throws IOException {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = randomName();
        compactMapping(to, PERSON_ID_TYPE_NAME, ALL_TYPES_TYPE_NAME)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "\"character\" VARCHAR",
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
                .create();

        sqlService.execute("SINK INTO " + to + " SELECT 13, 'a', string, \"boolean\", byte, short, \"int\", long, "
                + "\"float\", \"double\", \"decimal\", \"time\", \"date\", \"timestamp\", timestampTz "
                + " FROM " + from + " f");

        GenericRecord expected = compact(ALL_TYPES_TYPE_NAME)
                .setString("string", "string")
                .setString("character", "a")
                .setNullableBoolean("boolean", true)
                .setNullableInt8("byte", (byte) 127)
                .setNullableInt16("short", (short) 32767)
                .setNullableInt32("int", 2147483647)
                .setNullableInt64("long", 9223372036854775807L)
                .setNullableFloat32("float", 1234567890.1F)
                .setNullableFloat64("double", 123451234567890.1D)
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
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC)
                ))
        );
    }

    @Test
    public void test_topLevelPathExtraction() {
        String name = randomName();
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        sqlService.execute("INSERT INTO " + name + " (id, name) VALUES (1, 'Alice')");

        assertRowsAnyOrder(client(),
                "SELECT __key, this FROM " + name,
                List.of(new Row(
                        compact(PERSON_ID_TYPE_NAME).setNullableInt32("id", 1).build(),
                        compact(PERSON_TYPE_NAME).setString("name", "Alice").build()
                ))
        );
    }

    @Test
    public void when_topLevelPathWithoutCustomType_then_fail() {
        assertThatThrownBy(() ->
                compactMapping("test", PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                        .fields("__key INT",
                                "name VARCHAR EXTERNAL NAME \"this.name\"")
                        .create())
                .hasMessage("'__key' field must be used with a user-defined type");

        assertThatThrownBy(() ->
                compactMapping("test", PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
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
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("__key PersonId",
                        "this Person")
                .create();

        GenericRecord id = compact(PERSON_ID_TYPE_NAME).setNullableInt32("id", 1).build();
        GenericRecord person = compact(PERSON_TYPE_NAME).setString("name", "Alice").build();

        sqlService.execute("INSERT INTO " + name + " VALUES (?, ?)", id, person);

        assertRowsAnyOrder(client(),
                "SELECT * FROM " + name,
                List.of(new Row(id, person))
        );
    }

    @Test
    public void test_writingToImplicitTopLevelPath() {
        String name = randomName();
        compactMapping(name, PERSON_ID_TYPE_NAME, PERSON_TYPE_NAME)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        sqlService.execute("INSERT INTO " + name + " (__key, name) VALUES (?, ?)",
                compact(PERSON_ID_TYPE_NAME).setNullableInt32("id", 1).build(),
                "Alice");
        sqlService.execute("INSERT INTO " + name + " (id, this) VALUES (?, ?)",
                2,
                compact(PERSON_TYPE_NAME).setString("name", "Bob").build());

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
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

    public static class Person {
        String surname;
    }

    @SuppressWarnings("unused")
    public static class Primitives {
        boolean b;
        byte bt;
        short s;
        int i;
        long l;
        float f;
        double d;

        public Primitives() { }

        public Primitives(boolean b, byte bt, short s, int i, long l, float f, double d) {
            this.b = b;
            this.bt = bt;
            this.s = s;
            this.i = i;
            this.l = l;
            this.f = f;
            this.d = d;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Primitives that = (Primitives) obj;
            return b == that.b && bt == that.bt && s == that.s && i == that.i
                    && l == that.l && f == that.f && d == that.d;
        }
    }
}
