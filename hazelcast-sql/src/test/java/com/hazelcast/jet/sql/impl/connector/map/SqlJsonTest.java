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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.pipeline.file.JsonFileFormat.FORMAT_JSON;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJsonTest extends SqlTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setup() {
        initialize(1, smallInstanceConfig().setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true"));
        sqlService = instance().getSql();
    }

    private static SqlMapping jsonMapping(String name) {
        return new SqlMapping(name, IMapSqlConnector.class)
                .options(OPTION_KEY_FORMAT, JSON_FLAT_FORMAT,
                         OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT);
    }

    @Test
    public void test_nulls() {
        String name = randomName();
        jsonMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR EXTERNAL NAME \"this.name\"")
                .create();

        assertMapEventually(
                name,
                "SINK INTO " + name + " VALUES (null, null)",
                Map.of(new HazelcastJsonValue("{\"id\":null}"), new HazelcastJsonValue("{\"name\":null}"))
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = randomName();
        jsonMapping(name)
                .fields("key_name VARCHAR EXTERNAL NAME \"__key.name\"",
                        "value_name VARCHAR EXTERNAL NAME \"this.name\"")
                .create();

        assertMapEventually(
                name,
                "SINK INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                Map.of(new HazelcastJsonValue("{\"name\":\"Alice\"}"), new HazelcastJsonValue("{\"name\":\"Bob\"}"))
        );
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row("Alice", "Bob"))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = randomName();
        new SqlMapping(name, IMapSqlConnector.class)
                .fields("__key INT",
                        "name VARCHAR")
                .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                         OPTION_KEY_CLASS, Integer.class.getName(),
                         OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT)
                .create();

        // insert initial record
        sqlService.execute("SINK INTO " + name + " VALUES (13, 'Alice')");

        // alter schema
        new SqlMapping(name, IMapSqlConnector.class)
                .fields("__key INT",
                        "name VARCHAR",
                        "ssn BIGINT")
                .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                         OPTION_KEY_CLASS, Integer.class.getName(),
                         OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT)
                .createOrReplace();

        // insert record against new schema
        sqlService.execute("SINK INTO " + name + " VALUES (69, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(13, "Alice", null),
                        new Row(69, "Bob", 123456789L)
                )
        );
    }

    @Test
    public void test_allTypes() {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = randomName();
        jsonMapping(to)
                .fields("id VARCHAR EXTERNAL NAME \"__key.id\"",
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
                        "timestampTz TIMESTAMP WITH TIME ZONE",
                        "object OBJECT",
                        "map OBJECT")
                .create();

        sqlService.execute("SINK INTO " + to + " SELECT '1', f.* FROM " + from + " f");

        assertRowsAnyOrder(
                "SELECT * FROM " + to,
                List.of(new Row(
                        "1",
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
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        "{42=43}", // map converted to a string in JSON format
                        null
                ))
        );
    }

    @Test
    public void test_topLevelPathExtraction() {
        String name = randomName();
        jsonMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR EXTERNAL NAME \"this.name\"")
                .create();

        sqlService.execute("SINK INTO " + name + " VALUES (1, 'Alice')");

        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(
                        new HazelcastJsonValue("{\"id\":1}"),
                        new HazelcastJsonValue("{\"name\":\"Alice\"}")
                ))
        );
    }

    @Test
    public void when_explicitTopLevelPath_then_fail() {
        assertThatThrownBy(() ->
                jsonMapping("test")
                        .fields("__key INT",
                                "name VARCHAR EXTERNAL NAME \"this.name\"")
                        .create())
                .hasMessage("Cannot use '__key' field with JSON serialization");

        assertThatThrownBy(() ->
                jsonMapping("test")
                        .fields("id INT EXTERNAL NAME \"this.id\"",
                                "this VARCHAR")
                        .create())
                .hasMessage("Cannot use 'this' field with JSON serialization");
    }

    @Test
    public void test_writingToImplicitTopLevelPath() {
        String name = randomName();
        jsonMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        sqlService.execute("INSERT INTO " + name + " (__key, name) VALUES (?, ?)",
                new HazelcastJsonValue("{\"id\":1}"), "Alice");
        sqlService.execute("INSERT INTO " + name + " (id, this) VALUES (?, ?)",
                2, new HazelcastJsonValue("{\"name\":\"Bob\"}"));

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
    }

    @Test
    public void test_jsonType() {
        String name = randomName();
        new SqlMapping(name, IMapSqlConnector.class)
                .options(OPTION_KEY_FORMAT, FORMAT_JSON,
                         OPTION_VALUE_FORMAT, FORMAT_JSON)
                .create();

        sqlService.execute("SINK INTO " + name + " VALUES (CAST('[1,2,3]' AS JSON), CAST('[4,5,6]' AS JSON))");

        assertRowsAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(
                        new HazelcastJsonValue("[1,2,3]"),
                        new HazelcastJsonValue("[4,5,6]")
                ))
        );
    }
}
