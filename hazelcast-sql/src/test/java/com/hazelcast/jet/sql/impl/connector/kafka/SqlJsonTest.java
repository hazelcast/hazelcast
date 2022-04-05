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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJsonTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();

        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
    }

    @AfterClass
    public static void tearDownClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                createMap("{\"id\":null}", "{\"name\":null}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_name VARCHAR EXTERNAL NAME \"__key.name\""
                + ", value_name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                createMap("{\"name\":\"Alice\"}", "{\"name\":\"Bob\"}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row("Alice", "Bob"))
        );
    }

    @Test
    public void test_schemaEvolution() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        // insert initial record
        sqlService.execute("INSERT INTO " + name + " VALUES (13, 'Alice')");

        // alter schema
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ", ssn BIGINT"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        // insert record against new schema
        sqlService.execute("INSERT INTO " + name + " VALUES (69, 'Bob', 123456789)");

        // assert both - initial & evolved - records are correctly read
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(
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
        sqlService.execute("CREATE MAPPING " + to + " ("
                + "id VARCHAR EXTERNAL NAME \"__key.id\""
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
                + ", map OBJECT"
                + ", object OBJECT"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + to + " SELECT '1', f.* FROM " + from + " f");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + to,
                singletonList(new Row(
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
                        ImmutableMap.of("42", 43), // JSON serializer stores maps as JSON objects, the key is converted to a string
                        null
                ))
        );
    }

    @Test
    public void when_createMappingNoColumns_then_fail() {
        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING kafka "
                        + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ('valueFormat'='" + JSON_FLAT_FORMAT + "')"))
                .hasMessage("Column list is required for JSON format");
    }

    @Test
    public void when_explicitTopLevelField_then_fail_key() {
        when_explicitTopLevelField_then_fail("__key", "this");
    }

    @Test
    public void when_explicitTopLevelField_then_fail_this() {
        when_explicitTopLevelField_then_fail("this", "__key");
    }

    @Test
    public void test_jsonType() {
        String name = createRandomTopic();

        String createSql = format("CREATE MAPPING %s TYPE %s ", name, KafkaSqlConnector.TYPE_NAME)
                + "OPTIONS ( "
                + format("'%s' = 'json'", OPTION_KEY_FORMAT)
                + format(", '%s' = 'json'", OPTION_VALUE_FORMAT)
                + format(", 'bootstrap.servers' = '%s'", kafkaTestSupport.getBrokerConnectionString())
                + ", 'auto.offset.reset' = 'earliest'"
                + ")";

        sqlService.execute(createSql);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES ('[1,2,3]', '[4,5,6]')",
                createMap("[1,2,3]", "[4,5,6]")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(
                        new HazelcastJsonValue("[1,2,3]"),
                        new HazelcastJsonValue("[4,5,6]")
                ))
        );
    }

    private void when_explicitTopLevelField_then_fail(String field, String otherField) {
        assertThatThrownBy(() ->
                sqlService.execute("CREATE MAPPING kafka ("
                        + field + " VARCHAR"
                        + ", f VARCHAR EXTERNAL NAME \"" + otherField + ".f\""
                        + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                        + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                        + ", 'auto.offset.reset'='earliest'"
                        + ")"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("Cannot use the '" + field + "' field with JSON serialization");
    }

    @Test
    public void test_writingToTopLevel() {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + "("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + mapName + "(__key, name) VALUES('{\"id\":1}', null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + mapName + "(id, this) VALUES(1, '{\"name\":\"foo\"}')"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, this FROM " + name,
                singletonList(new Row(
                        ImmutableMap.of("id", 1),
                        ImmutableMap.of("name", "Alice")
                ))
        );
    }

    @Test
    public void test_explicitKeyAndValueSerializers() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_name VARCHAR EXTERNAL NAME \"__key.name\""
                + ", value_name VARCHAR EXTERNAL NAME \"this.name\""
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FLAT_FORMAT + '\''
                + ", 'key.serializer'='" + ByteArraySerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + ByteArrayDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + ByteArraySerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + ByteArrayDeserializer.class.getCanonicalName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                createMap("{\"name\":\"Alice\"}", "{\"name\":\"Bob\"}")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row("Alice", "Bob"))
        );
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
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
