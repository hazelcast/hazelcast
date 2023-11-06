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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.AllTypesValue;
import com.hazelcast.jet.sql.impl.connector.map.model.InsuredPerson;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.jet.sql.impl.connector.map.model.PersonId;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.schema.Mapping;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SqlPojoTest extends SqlTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setup() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    private static SqlMapping javaMapping(String name, Class<?> keyClass, Class<?> valueClass) {
        return new SqlMapping(name, IMapSqlConnector.class)
                .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                         OPTION_KEY_CLASS, keyClass.getName(),
                         OPTION_VALUE_FORMAT, JAVA_FORMAT,
                         OPTION_VALUE_CLASS, valueClass.getName());
    }

    @Test
    public void test_nulls() {
        String name = randomName();
        javaMapping(name, PersonId.class, Person.class).create();

        assertMapEventually(
                name,
                "SINK INTO " + name + " VALUES (1, null)",
                createMap(new PersonId(1), new Person())
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, null))
        );
    }

    @Test
    public void when_nullIntoPrimitive_then_fails() {
        String name = randomName();
        javaMapping(name, PersonId.class, Person.class).create();

        assertThatThrownBy(() -> sqlService.execute("SINK INTO " + name + " VALUES (null, 'Alice')"))
                .hasMessageContaining("Cannot pass NULL to a method with a primitive argument");
    }

    @Test
    public void when_wrongClass_then_canDrop() {
        String name = randomName();
        createBrokenMapping(name);

        IMap<String, Object> catalog = instance().getMap(SQL_CATALOG_MAP_NAME);
        assertThat(catalog.containsKey(name)).isTrue();

        sqlService.execute("DROP MAPPING " + name);
        assertThat(catalog.containsKey(name)).isFalse();
    }

    @Test
    public void when_wrongClass_then_canNotQueryTable() {
        String badName = randomName();
        createBrokenMapping(badName);

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " + badName))
                .hasMessage("Mapping '%s' is invalid: com.hazelcast.sql.impl.QueryException: "
                                + "Unable to load class 'com.hazelcast.NoSuchClass'", badName);
    }

    @Test
    public void when_wrongClass_then_canQueryOtherTables() {
        String badName = randomName();
        String goodName = randomName();
        createBrokenMapping(badName);
        javaMapping(goodName, PersonId.class, Person.class).create();

        assertThat(sqlService.execute("SELECT * FROM " + goodName)).hasSize(0);
    }

    /**
     * Simulates creation of a mapping for class that was later unloaded.
     */
    private static void createBrokenMapping(String name) {
        javaMapping(name, PersonId.class, Person.class).create();

        IMap<String, Mapping> catalog = instance().getMap(SQL_CATALOG_MAP_NAME);
        Mapping m = catalog.get(name);
        HashMap<String, String> brokenOptions = new HashMap<>(m.options());
        brokenOptions.put(OPTION_VALUE_CLASS, "com.hazelcast.NoSuchClass");
        catalog.put(name, new Mapping(
                m.name(),
                m.externalName(),
                null,
                m.connectorType(),
                null,
                new ArrayList<>(m.fields()),
                brokenOptions));
    }

    @Test
    public void test_fieldsShadowing() {
        String name = randomName();
        javaMapping(name, PersonId.class, Person.class).create();

        assertMapEventually(
                name,
                "SINK INTO " + name + " (id, name) VALUES (1, 'Alice')",
                createMap(new PersonId(1), new Person(null, "Alice"))
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = randomName();
        javaMapping(name, PersonId.class, Person.class)
                .fields("key_id INT EXTERNAL NAME \"__key.id\"",
                        "value_id INT EXTERNAL NAME \"this.id\"",
                        "name VARCHAR")
                .create();

        assertMapEventually(
                name,
                "SINK INTO " + name + " (value_id, key_id, name) VALUES (2, 1, 'Alice')",
                createMap(new PersonId(1), new Person(2, "Alice"))
        );
        assertRowsAnyOrder(
                "SELECT key_id, value_id, name FROM " + name,
                List.of(new Row(1, 2, "Alice"))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = randomName();
        javaMapping(name, PersonId.class, Person.class).create();

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        // alter schema
        javaMapping(name, PersonId.class, InsuredPerson.class).createOrReplace();

        // insert record against new schema
        sqlService.execute("SINK INTO " + name + " (id, name, ssn) VALUES (2, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT id, name, ssn FROM " + name,
                List.of(
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

        javaMapping(name, PersonId.class, Person.class)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT" /* defined in the subclass */)
                .create();

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
                List.of(
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
        javaMapping(to, BigInteger.class, AllTypesValue.class).create();

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
                        + ", map"
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
                        + ", map"
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
                        + ", map"
                        + ", object "
                        + "FROM " + to,
                List.of(new Row(
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
                        ImmutableMap.of(42, 43),
                        null
                )));
    }

    @Test
    public void when_fieldWithInitialValueUnmapped_then_initialValuePreserved() {
        String mapName = randomName();
        javaMapping(mapName, Integer.class, ClassInitialValue.class)
                .fields("__key INT")
                .create();
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
        javaMapping(mapName, Integer.class, ClassInitialValue.class).create();
        sqlService.execute("SINK INTO " + mapName + "(__key) VALUES (1)");
        assertRowsAnyOrder(
                "SELECT * FROM " + mapName,
                List.of(new Row(1, null))
        );
    }

    @Test
    public void when_fieldWithInitialValueAssignedNull_then_isNull() {
        String mapName = randomName();
        javaMapping(mapName, Integer.class, ClassInitialValue.class).create();
        sqlService.execute("SINK INTO " + mapName + "(__key, field) VALUES (1, null)");
        assertRowsAnyOrder(
                "SELECT * FROM " + mapName,
                List.of(new Row(1, null))
        );
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
        javaMapping(mapName, Integer.class, Person.class)
                .fields("__key INT")
                .fieldsIf(explicit, "this OBJECT")
                .fields("name VARCHAR")
                .create();

        if (explicit) {
            assertThatThrownBy(() -> sqlService.execute("SINK INTO " + mapName + " VALUES(1, null, 'foo')"))
                    .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");
        }

        assertThatThrownBy(() -> sqlService.execute("SINK INTO " + mapName + "(__key, this) VALUES(1, null)"))
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        sqlService.execute("SINK INTO " + mapName + (explicit ? "(__key, name)" : "") + " VALUES (1, 'foo')");

        assertRowsAnyOrder(
                "SELECT __key, this, name FROM " + mapName,
                List.of(new Row(1, new Person(null, "foo"), "foo"))
        );
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = randomName();
        javaMapping(name, PersonId.class, Person.class).create();
        sqlService.execute("SINK INTO " + name + " (id, name) VALUES (1, 'Alice')");

        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(new PersonId(1), new Person(null, "Alice")))
        );
    }

    @Test
    public void test_nestedField() {
        String mapName = randomName();
        assertThatThrownBy(() ->
                new SqlMapping(mapName, IMapSqlConnector.class)
                        .fields("__key INT",
                                "petName VARCHAR",
                                "\"owner.name\" VARCHAR")
                        .create())
                .hasMessageContaining("Invalid external name: this.owner.name");
    }

    @Test
    public void when_noFieldsResolved_then_wholeValueMapped() {
        String name = randomName();
        javaMapping(name, Object.class, Object.class).create();

        Person key = new Person(1, "foo");
        Person value = new Person(2, "bar");
        instance().getMap(name).put(key, value);

        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(key, value))
        );
    }

    @Test
    public void when_keyHasKeyField_then_fieldIsSkipped() {
        String name = randomName();
        javaMapping(name, ClassWithKey.class, Integer.class).create();

        instance().getMap(name).put(new ClassWithKey(), 0);

        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(0))
        );
        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(new ClassWithKey(), 0))
        );
    }

    @Test
    public void test_classWithMapField() {
        final String name = randomName();
        final ClassWithMapField obj = new ClassWithMapField(100L, "k", "v");

        javaMapping(name, Long.class, ClassWithMapField.class).create();

        instance().getSql().execute("SINK INTO " + name + " VALUES (?, ?, ?)", 1L, obj.id, obj.props);
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1L, obj.id, obj.props))
        );
    }

    public static class ClassInitialValue implements Serializable {
        public Integer field = 42;
    }

    public static class ClassWithKey implements Serializable {
        public int __key;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassWithKey that = (ClassWithKey) o;
            return __key == that.__key;
        }

        @Override
        public int hashCode() {
            return Objects.hash(__key);
        }
    }

    public static class ClassWithMapField implements Serializable {
        private Long id;
        private Map<String, String> props;

        @SuppressWarnings("unused")
        public ClassWithMapField() { }

        public ClassWithMapField(Long id, String... values) {
            this.id = id;
            this.props = new HashMap<>();
            for (int i = 0; i < values.length; i += 2) {
                props.put(values[i], values[i + 1]);
            }
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Map<String, String> getProps() {
            return props;
        }

        public void setProps(Map<String, String> props) {
            this.props = props;
        }
    }
}
