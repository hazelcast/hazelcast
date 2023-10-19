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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.pipeline.file.JsonFileFormat.FORMAT_JSON;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJsonTest extends KafkaSqlTestSupport {
    private static final int INITIAL_PARTITION_COUNT = 4;

    private static SqlMapping kafkaMapping(String name) {
        return new SqlMapping(name, KafkaSqlConnector.class)
                .options(OPTION_KEY_FORMAT, JSON_FLAT_FORMAT,
                         OPTION_VALUE_FORMAT, JSON_FLAT_FORMAT,
                         "bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                         "auto.offset.reset", "earliest");
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        kafkaMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR EXTERNAL NAME \"this.name\"")
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                Map.of("{\"id\":null}", "{\"name\":null}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        kafkaMapping(name)
                .fields("key_name VARCHAR EXTERNAL NAME \"__key.name\"",
                        "value_name VARCHAR EXTERNAL NAME \"this.name\"")
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                Map.of("{\"name\":\"Alice\"}", "{\"name\":\"Bob\"}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row("Alice", "Bob"))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = createRandomTopic();
        kafkaMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        // insert initial record
        sqlService.execute("INSERT INTO " + name + " VALUES (13, 'Alice')");

        // alter schema
        kafkaMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT")
                .createOrReplace();

        // insert record against new schema
        sqlService.execute("INSERT INTO " + name + " VALUES (69, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsEventuallyInAnyOrder(
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

        String to = createRandomTopic();
        kafkaMapping(to)
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
                        "map OBJECT",
                        "object OBJECT")
                .create();

        sqlService.execute("INSERT INTO " + to + " SELECT '1', f.* FROM " + from + " f");

        assertRowsEventuallyInAnyOrder(
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
                        Map.of("42", 43), // JSON serializer stores maps as JSON objects, the key is converted to a string
                        null
                ))
        );
    }

    @Test
    public void when_createMappingNoColumns_then_fail() {
        assertThatThrownBy(() -> kafkaMapping("kafka").create())
                .hasMessage("Column list is required for JSON format");
    }

    @Test
    public void test_jsonType() {
        String name = createRandomTopic();
        kafkaMapping(name)
                .options(OPTION_KEY_FORMAT, FORMAT_JSON,
                         OPTION_VALUE_FORMAT, FORMAT_JSON)
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES ('[1,2,3]', '[4,5,6]')",
                Map.of("[1,2,3]", "[4,5,6]")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(
                        new HazelcastJsonValue("[1,2,3]"),
                        new HazelcastJsonValue("[4,5,6]")
                ))
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
                kafkaMapping("kafka")
                        .fields(field + " VARCHAR",
                                "f VARCHAR EXTERNAL NAME \"" + otherField + ".f\"")
                        .create())
                .hasMessage("Cannot use '" + field + "' field with JSON serialization");
    }

    @Test
    public void test_writingToTopLevel() {
        String mapName = randomName();
        kafkaMapping(mapName)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + mapName + "(__key, name) VALUES ('{\"id\":1}', null)"))
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + mapName + "(id, this) VALUES (1, '{\"name\":\"foo\"}')"))
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = createRandomTopic();
        kafkaMapping(name)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(
                        Map.of("id", 1),
                        Map.of("name", "Alice")
                ))
        );
    }

    @Test
    public void test_explicitKeyAndValueSerializers() {
        String name = createRandomTopic();
        kafkaMapping(name)
                .fields("key_name VARCHAR EXTERNAL NAME \"__key.name\"",
                        "value_name VARCHAR EXTERNAL NAME \"this.name\"")
                .options("key.serializer", ByteArraySerializer.class.getCanonicalName(),
                         "key.deserializer", ByteArrayDeserializer.class.getCanonicalName(),
                         "value.serializer", ByteArraySerializer.class.getCanonicalName(),
                         "value.deserializer", ByteArrayDeserializer.class.getCanonicalName())
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                Map.of("{\"name\":\"Alice\"}", "{\"name\":\"Bob\"}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row("Alice", "Bob"))
        );
    }

    private static String createRandomTopic() {
        return createRandomTopic(INITIAL_PARTITION_COUNT);
    }

    private static void assertTopicEventually(String name, String sql, Map<String, String> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(
                name,
                expected,
                StringDeserializer.class,
                StringDeserializer.class
        );
    }
}
