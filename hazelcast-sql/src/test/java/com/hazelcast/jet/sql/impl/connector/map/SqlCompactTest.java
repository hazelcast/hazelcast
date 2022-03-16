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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
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
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SqlCompactTest extends SqlTestSupport {

    private static SqlService sqlService;
    private static SqlService clientSqlService;

    private static final String PERSON_ID_TYPE_NAME = "personId";
    private static final String PERSON_TYPE_NAME = "person";
    private static final String ALL_TYPES_TYPE_NAME = "allTypes";

    private static InternalSerializationService serializationService;

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        CompactSerializationConfig compactSerializationConfig =
                config.getSerializationConfig().getCompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        // registering this class to the member to see it does not affect any of the tests.
        // It has a different schema than all the tests
        compactSerializationConfig.register(Person.class, PERSON_TYPE_NAME, new CompactSerializer<Person>() {
            @Nonnull
            @Override
            public Person read(@Nonnull CompactReader in) {
                Person person = new Person();
                person.surname = in.readString("surname", "NotAssigned");
                return person;
            }

            @Override
            public void write(@Nonnull CompactWriter out, @Nonnull Person person) {
                out.writeString("surname", person.surname);
            }
        });

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        initializeWithClient(1, config, clientConfig);
        sqlService = instance().getSql();
        clientSqlService = client().getSql();

        serializationService = Util.getSerializationService(instance());
    }

    @Test
    public void test_objectIsNotSupported() {
        String name = randomName();
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT "
                + ", object OBJECT "
                + ") "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        ).updateCount()).hasMessageContaining("Cannot derive Compact type for 'OBJECT'");
    }

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
    }

    @Test
    public void test_readToClassWithNonNulls() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + " __key INT "
                + ", b BOOLEAN "
                + ", bt TINYINT "
                + ", s SMALLINT "
                + ", i INTEGER "
                + ", l BIGINT "
                + ", f REAL "
                + ", d DOUBLE "
                + ") "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='integer'"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + Primitives.class.getName() + '\''
                + ")"
        ).updateCount();

        sqlService.execute("SINK INTO " + name + " VALUES (1, true, 2, 3, 4, 5, 12.321, 124.311)").updateCount();

        IMap<Object, Object> map = client().getMap(name);
        Primitives primitives = (Primitives) map.get(1);

        assertEquals(primitives.b, true);
        assertEquals(primitives.bt, 2);
        assertEquals(primitives.s, 3);
        assertEquals(primitives.i, 4);
        assertEquals(primitives.l, 5);
        assertEquals(primitives.f, 12.321, 0.1);
        assertEquals(primitives.d, 124.311, 0.1);
    }


    @Test
    public void test_insertNulls() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "__key INT "
                + ", b BOOLEAN "
                + ", st VARCHAR "
                + ", bt TINYINT "
                + ", s SMALLINT "
                + ", i INTEGER "
                + ", l BIGINT "
                + ", bd DECIMAL "
                + ", f REAL "
                + ", d DOUBLE "
                + ", t TIME "
                + ", dt DATE "
                + ", tmstmp TIMESTAMP "
                + ", tmstmpTz TIMESTAMP WITH TIME ZONE "
                + ") "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='integer'"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        ).updateCount();

        sqlService.execute("SINK INTO " + name + " VALUES (1, null, null, null, "
                + "null, null, null, null, null, null, null, null, null, null )").updateCount();

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord valueRecord = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueRecord.getNullableBoolean("b")).isNull();
        assertThat(valueRecord.getString("st")).isNull();
        assertThat(valueRecord.getNullableInt8("bt")).isNull();
        assertThat(valueRecord.getNullableInt16("s")).isNull();
        assertThat(valueRecord.getNullableInt32("i")).isNull();
        assertThat(valueRecord.getNullableInt64("l")).isNull();
        assertThat(valueRecord.getDecimal("bd")).isNull();
        assertThat(valueRecord.getNullableFloat32("f")).isNull();
        assertThat(valueRecord.getNullableFloat64("d")).isNull();
        assertThat(valueRecord.getTime("t")).isNull();
        assertThat(valueRecord.getDate("dt")).isNull();
        assertThat(valueRecord.getTimestamp("tmstmp")).isNull();
        assertThat(valueRecord.getTimestampWithTimezone("tmstmpTz")).isNull();

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, null, null, null, null, null, null, null, null, null, null, null, null, null)));
    }

    @Test
    public void test_readNonNullKindsOfCompactViaSQL() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + " __key INT "
                + ", b BOOLEAN "
                + ", st VARCHAR "
                + ", bt TINYINT "
                + ", s SMALLINT "
                + ", i INTEGER "
                + ", l BIGINT "
                + ", bd DECIMAL "
                + ", f REAL "
                + ", d DOUBLE "
                + ", t TIME "
                + ", dt DATE "
                + ", tmstmp TIMESTAMP "
                + ", tmstmpTz TIMESTAMP WITH TIME ZONE "
                + ") "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='integer'"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        ).updateCount();

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

        GenericRecord record = GenericRecordBuilder.compact(PERSON_TYPE_NAME)
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
                singletonList(new Row(1, b, st, bt, s, i, l, bd, f, d, t, dt, tmstmp, tmstmpTz)));
    }

    @Test
    public void test_emptyColumnListIsNotAllowed() {
        String name = randomName();
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        ).iterator().next()).hasMessageContaining("Column list is required for Compact format");
    }

    @Test
    public void test_fieldsMapping() throws IOException {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + name + " (value_id, key_id) VALUES (2, 1)");

        Entry<Data, Data> entry = randomEntryFrom(name);

        InternalGenericRecord keyRecord = serializationService.readAsInternalGenericRecord(entry.getKey());
        assertThat(keyRecord.getNullableInt32("id")).isEqualTo(1);

        InternalGenericRecord valueRecord = serializationService.readAsInternalGenericRecord(entry.getValue());
        assertThat(valueRecord.getNullableInt32("id")).isEqualTo(2);

        assertRowsAnyOrder(
                "SELECT key_id, value_id FROM " + name,
                singletonList(new Row(1, 2))
        );
    }

    @Test
    public void test_schemaEvolution_fieldAdded() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR "
                + ") "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        );

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + '('
                + "id INT EXTERNAL NAME \"__key.id\" "
                + ", name VARCHAR"
                + ", ssn BIGINT"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + "'"
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
    public void test_schemaEvolution_fieldRemoved() {
        String name = randomName();
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + '('
                + "id INT EXTERNAL NAME \"__key.id\" "
                + ", name VARCHAR"
                + ", ssn BIGINT"
                + " )"
                + " TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        );

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice', 123456789)");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + '('
                + "id INT EXTERNAL NAME \"__key.id\" "
                + ", name VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        );

        // insert record against new schema/class definition
        sqlService.execute("SINK INTO " + name + " VALUES (2, 'Bob')");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
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
        sqlService.execute("CREATE MAPPING " + to + " ("
                + " id INT EXTERNAL NAME \"__key.id\" "
                + ", \"character\" VARCHAR "
                + ", string VARCHAR "
                + ", \"boolean\" BOOLEAN "
                + ", byte TINYINT "
                + ", short SMALLINT "
                + ", \"int\" INT "
                + ", long BIGINT "
                + ", \"float\" REAL "
                + ", \"double\" DOUBLE "
                + ", \"decimal\" DECIMAL "
                + ", \"time\" TIME "
                + ", \"date\" DATE "
                + ", \"timestamp\" TIMESTAMP "
                + ", timestampTz TIMESTAMP WITH TIME ZONE "
                + " ) TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + ALL_TYPES_TYPE_NAME + '\''
                + ")"
        );

        sqlService.execute("SINK INTO " + to + " SELECT 13, 'a', string, \"boolean\", byte, short, \"int\", long, "
                + "\"float\", \"double\", \"decimal\", \"time\", \"date\", \"timestamp\", timestampTz "
                + " FROM " + from + " f");

        InternalGenericRecord valueRecord = serializationService
                .readAsInternalGenericRecord(randomEntryFrom(to).getValue());
        assertThat(valueRecord.getString("string")).isEqualTo("string");
        assertThat(valueRecord.getString("character")).isEqualTo("a");
        assertThat(valueRecord.getNullableBoolean("boolean")).isTrue();
        assertThat(valueRecord.getNullableInt8("byte")).isEqualTo((byte) 127);
        assertThat(valueRecord.getNullableInt16("short")).isEqualTo((short) 32767);
        assertThat(valueRecord.getNullableInt32("int")).isEqualTo(2147483647);
        assertThat(valueRecord.getNullableInt64("long")).isEqualTo(9223372036854775807L);
        assertThat(valueRecord.getNullableFloat32("float")).isEqualTo(1234567890.1F);
        assertThat(valueRecord.getNullableFloat64("double")).isEqualTo(123451234567890.1D);
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
    public void test_writingToTopLevelWhileNestedFieldMapped_implicit() {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + "("
                + "__key INT"
                + ", name VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + "'\n"
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + "'\n"
                + ")"
        );

        assertThatThrownBy(() ->
                sqlService.execute("SINK INTO " + mapName + "(__key, this) VALUES(1, null)").iterator().next())
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        clientSqlService.execute("SINK INTO " + mapName + " VALUES (1, 'foo')");

        Iterator<SqlRow> resultIter = clientSqlService.execute("SELECT __key, this, name FROM " + mapName).iterator();
        SqlRow row = resultIter.next();
        assertEquals(1, (int) row.getObject(0));
        assertInstanceOf(GenericRecord.class, row.getObject(1));
        assertEquals("foo", row.getObject(2));
        assertFalse(resultIter.hasNext());
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + '('
                + "id INT EXTERNAL NAME \"__key.id\" "
                + ", name VARCHAR"
                + " ) "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                + ")"
        );
        clientSqlService.execute("SINK INTO " + name + " (id, name) VALUES (1, 'Alice')");

        Iterator<SqlRow> rowIterator = clientSqlService.execute("SELECT __key, this FROM " + name).iterator();
        SqlRow row = rowIterator.next();
        assertFalse(rowIterator.hasNext());

        assertEquals(
                GenericRecordBuilder.compact(PERSON_ID_TYPE_NAME).setNullableInt32("id", 1).build(),
                row.getObject(0)
        );
        assertEquals(
                GenericRecordBuilder.compact(PERSON_TYPE_NAME).setString("name", "Alice").build(),
                row.getObject(1)
        );
    }

    @Test
    public void when_explicitTopLevelField_then_fail_key() {
        when_explicitTopLevelField_then_fail("__key", "this");
    }

    @Test
    public void when_explicitTopLevelField_then_fail_this() {
        when_explicitTopLevelField_then_fail("this", "__key");
    }

    private void when_explicitTopLevelField_then_fail(String field, String otherField) {
        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING map ("
                        + field + " VARCHAR"
                        + ", f VARCHAR EXTERNAL NAME \"" + otherField + ".f\""
                        + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + COMPACT_FORMAT + '\''
                        + ", '" + OPTION_KEY_COMPACT_TYPE_NAME + "'='" + PERSON_ID_TYPE_NAME + '\''
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + '\''
                        + ", '" + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + PERSON_TYPE_NAME + '\''
                        + ")").iterator().next())
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("Cannot use the '" + field + "' field with Compact serialization");
    }

    @Test
    public void when_compactDisabled_then_compactFormatNotAllowed() {
        HazelcastInstance inst = createHazelcastInstance(smallInstanceConfig());
        assertFalse(inst.getConfig().getSerializationConfig().getCompactSerializationConfig().isEnabled());
        assertThatThrownBy(() -> inst.getSql().execute("create mapping m " +
                "type imap " +
                "options (" +
                "'keyFormat'='int', 'valueFormat'='compact', " +
                "'valueCompactTypeName'='foo')"))
                .hasMessage("Compact serialization is disabled in the config");
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
}
