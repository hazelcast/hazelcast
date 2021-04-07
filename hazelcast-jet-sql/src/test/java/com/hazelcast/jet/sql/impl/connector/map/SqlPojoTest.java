/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.AllTypesValue;
import com.hazelcast.jet.sql.impl.connector.map.model.InsuredPerson;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.map.model.PersonId;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SqlPojoTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_insertIntoDiscoveredMap() {
        String mapName = randomName();
        instance().getMap(mapName).put(new PersonId(1), new Person(1, "Alice"));

        assertMapEventually(
                mapName,
                "SINK INTO partitioned." + mapName + " VALUES (2, 'Bob')",
                createMap(new PersonId(1), new Person(1, "Alice"), new PersonId(2), new Person(null, "Bob"))
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + mapName,
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
    }

    @Test
    public void test_nulls() {
        String name = randomName();
        sqlService.execute(javaSerializableMapDdl(name, PersonId.class, Person.class));

        assertMapEventually(
                name,
                "SINK INTO " + name + " VALUES (null, null)",
                createMap(new PersonId(), new Person())
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsShadowing() {
        String name = randomName();
        sqlService.execute(javaSerializableMapDdl(name, PersonId.class, Person.class));

        assertMapEventually(
                name,
                "SINK INTO " + name + " (id, name) VALUES (1, 'Alice')",
                createMap(new PersonId(1), new Person(null, "Alice"))
        );

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = randomName();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ", name VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + '\''
                + ")"
        );

        assertMapEventually(
                name,
                "SINK INTO " + name + " (value_id, key_id, name) VALUES (2, 1, 'Alice')",
                createMap(new PersonId(1), new Person(2, "Alice"))
        );
        assertRowsAnyOrder(
                "SELECT key_id, value_id, name FROM " + name,
                singletonList(new Row(1, 2, "Alice"))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = randomName();
        sqlService.execute(javaSerializableMapDdl(name, PersonId.class, Person.class));

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + ' '
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + InsuredPerson.class.getName() + '\''
                + ")"
        );

        // insert record against new schema
        sqlService.execute("SINK INTO " + name + " (id, name, ssn) VALUES (2, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT id, name, ssn FROM " + name,
                asList(
                        new Row(1, "Alice", null),
                        new Row(2, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void test_fieldsExtensions() {
        String name = randomName();

        Map<PersonId, InsuredPerson> map = instance().getMap(name);
        map.put(new PersonId(1), new InsuredPerson(1, "Alice", 123456789L));

        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\","
                + "name VARCHAR,"
                // the "ssn" field isn't defined in the `Person` class, but in the subclass
                + "ssn BIGINT"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + '\''
                + ")"
        );

        assertMapEventually(
                name,
                "SINK INTO " + name + " (id, name, ssn) VALUES (2, 'Bob', null)",
                createMap(
                        new PersonId(1), new InsuredPerson(1, "Alice", 123456789L),
                        new PersonId(2), new Person(null, "Bob")
                )
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                asList(
                        new Row(1, "Alice", 123456789L),
                        new Row(2, "Bob", null)
                )
        );
    }

    @Test
    public void test_allTypes() {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = randomName();
        sqlService.execute(javaSerializableMapDdl(to, BigInteger.class, AllTypesValue.class));

        assertMapEventually(
                to,
                "SINK INTO " + to + " ("
                        + "__key"
                        + ", string"
                        + ", character0"
                        + ", boolean0"
                        + ", byte0"
                        + ", short0"
                        + ", int0"
                        + ", long0"
                        + ", float0"
                        + ", double0"
                        + ", bigDecimal"
                        + ", bigInteger"
                        + ", \"localTime\""
                        + ", localDate"
                        + ", localDateTime"
                        + ", \"date\""
                        + ", calendar"
                        + ", instant"
                        + ", zonedDateTime"
                        + ", offsetDateTime"
                        + ", object"
                        + ") SELECT "
                        + "CAST(1 AS DECIMAL)"
                        + ", string"
                        + ", SUBSTRING(string, 1, 1)"
                        + ", \"boolean\""
                        + ", byte"
                        + ", short"
                        + ", \"int\""
                        + ", long"
                        + ", \"float\""
                        + ", \"double\""
                        + ", \"decimal\""
                        + ", \"decimal\""
                        + ", \"time\""
                        + ", \"date\""
                        + ", \"timestamp\""
                        + ", \"timestampTz\""
                        + ", \"timestampTz\""
                        + ", \"timestampTz\""
                        + ", \"timestampTz\""
                        + ", \"timestampTz\""
                        + ", object"
                        + " FROM " + from,
                createMap(BigInteger.valueOf(1), AllTypesValue.testValue()));

        assertRowsAnyOrder(
                "SELECT"
                        + " __key"
                        + ", string"
                        + ", character0"
                        + ", boolean0"
                        + ", byte0"
                        + ", short0"
                        + ", int0"
                        + ", long0"
                        + ", bigDecimal"
                        + ", bigInteger"
                        + ", float0"
                        + ", double0"
                        + ", \"localTime\""
                        + ", localDate"
                        + ", localDateTime"
                        + ", \"date\""
                        + ", calendar"
                        + ", instant"
                        + ", zonedDateTime"
                        + ", offsetDateTime "
                        + ", object "
                        + "FROM " + to,
                singletonList(new Row(
                        BigDecimal.valueOf(1),
                        "string",
                        "s",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        new BigDecimal("9223372036854775.123"),
                        new BigDecimal("9223372036854775"),
                        1234567890.1f,
                        123451234567890.1,
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        OffsetDateTime.ofInstant(Date.from(ofEpochMilli(1586953414200L)).toInstant(), systemDefault()),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        OffsetDateTime.ofInstant(ofEpochMilli(1586953414200L), systemDefault()),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        null
                )));
    }

    @Test
    public void when_fieldWithInitialValueUnmapped_then_initialValuePreserved() {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + "(__key INT)"
                + " TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "',\n"
                + '\'' + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "',\n"
                + '\'' + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "',\n"
                + '\'' + OPTION_VALUE_CLASS + "'='" + ClassInitialValue.class.getName() + "'\n"
                + ")");
        sqlService.execute("SINK INTO " + mapName + "(__key) VALUES (1)");

        ClassInitialValue val = instance().<Integer, ClassInitialValue>getMap(mapName).get(1);
        assertEquals(Integer.valueOf(42), val.field);
    }

    @Test
    public void when_fieldWithInitialValueNotUsed_then_valueOverwritten() {
        // I'm not sure this behavior is the best, but it's defensible at least.
        // The class assigns initial value of 42 to age. The SINK INTO statement doesn't write to the `age`
        // field. One could expect that the field will be left alone. On the other hand, we can say that all mapped fields
        // are always overwritten: if they're not present, we'll write null. We don't support DEFAULT values yet, but
        // it behaves as if the DEFAULT was null.
        String mapName = randomName();
        sqlService.execute(javaSerializableMapDdl(mapName, Integer.class, ClassInitialValue.class));
        sqlService.execute("SINK INTO " + mapName + "(__key) VALUES (1)");
        assertRowsAnyOrder("SELECT * FROM " + mapName, singletonList(new Row(1, null)));
    }

    @Test
    public void when_fieldWithInitialValueAssignedNull_then_isNull() {
        String mapName = randomName();
        sqlService.execute(javaSerializableMapDdl(mapName, Integer.class, ClassInitialValue.class));
        sqlService.execute("SINK INTO " + mapName + "(__key, field) VALUES (1, null)");
        assertRowsAnyOrder("SELECT * FROM " + mapName, singletonList(new Row(1, null)));
    }

    @Test
    public void test_writingToTopLevelWhileNestedFieldMapped_explicit() {
        test_writingToTopLevel(true);
    }

    @Test
    public void test_writingToTopLevelWhileNestedFieldMapped_implicit() {
        test_writingToTopLevel(false);
    }

    private void test_writingToTopLevel(boolean explicit) {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + "("
                + "__key INT"
                + (explicit ? ", this OBJECT" : "")
                + ", name VARCHAR"
                + ") TYPE " + IMapSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + "'\n"
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

        assertRowsAnyOrder("SELECT __key, this, name FROM " + mapName,
                singletonList(new Row(1, new Person(null, "foo"), "foo")));
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = randomName();
        sqlService.execute(javaSerializableMapDdl(name, PersonId.class, Person.class));
        sqlService.execute("SINK INTO " + name + " (id, name) VALUES (1, 'Alice')");

        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                singletonList(new Row(new PersonId(1), new Person(null, "Alice")))
        );
    }

    @Test
    public void test_nestedField() {
        String mapName = randomName();
        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING " + mapName + "("
                        + "__key INT,"
                        + "petName VARCHAR,"
                        + "\"owner.name\" VARCHAR) "
                        + "TYPE " + IMapSqlConnector.TYPE_NAME
                ))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid external name: this.owner.name");
    }

    public static class ClassInitialValue implements Serializable {

        public Integer field = 42;
    }
}
