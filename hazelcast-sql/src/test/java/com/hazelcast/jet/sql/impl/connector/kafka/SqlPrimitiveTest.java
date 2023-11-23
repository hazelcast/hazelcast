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

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.ResultIterator;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlPrimitiveTest extends KafkaSqlTestSupport {
    private static final int INITIAL_PARTITION_COUNT = 4;

    private static SqlMapping kafkaMapping(String name, boolean simple) {
        return new SqlMapping(name, KafkaSqlConnector.class)
                .options("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                         "auto.offset.reset", "earliest")
                .optionsIf(simple,
                           OPTION_KEY_FORMAT, "int",
                           OPTION_VALUE_FORMAT, "varchar")
                .optionsIf(!simple,
                           OPTION_KEY_FORMAT, JAVA_FORMAT,
                           OPTION_KEY_CLASS, Integer.class.getName(),
                           OPTION_VALUE_FORMAT, JAVA_FORMAT,
                           OPTION_VALUE_CLASS, String.class.getName());
    }

    private static void createMapping(String name, boolean simple) {
        kafkaMapping(name, simple).create();
    }

    @Test
    public void test_insertSelect() {
        String name = createRandomTopic();
        createMapping(name, false);

        String from = randomName();
        TestBatchSqlConnector.create(sqlService, from, 4);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " SELECT v, 'value-' || v FROM " + from,
                createMap(0, "value-0", 1, "value-1", 2, "value-2", 3, "value-3")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name + " WHERE __key > 0 AND __key < 3",
                asList(new Row(1, "value-1"), new Row(2, "value-2"))
        );
    }

    @Test
    public void createKafkaMappingWithDataConnection() {
        String dlName = randomName();
        sqlService.execute("CREATE DATA CONNECTION " + dlName + " TYPE Kafka NOT SHARED " + String.format(
                "OPTIONS ( " +
                        "'bootstrap.servers' = '%s', " +
                        "'key.deserializer' = '%s', " +
                        "'key.serializer' = '%s', " +
                        "'value.serializer' = '%s', " +
                        "'value.deserializer' = '%s', " +
                        "'auto.offset.reset' = 'earliest') ",
                kafkaTestSupport.getBrokerConnectionString(),
                IntegerDeserializer.class.getCanonicalName(),
                IntegerSerializer.class.getCanonicalName(),
                StringSerializer.class.getCanonicalName(),
                StringDeserializer.class.getCanonicalName()));

        String name = randomName();
        new SqlMapping(name, dlName)
                .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                         OPTION_KEY_CLASS, Integer.class.getName(),
                         OPTION_VALUE_FORMAT, JAVA_FORMAT,
                         OPTION_VALUE_CLASS, String.class.getName())
                .create();

        String from = randomName();
        TestBatchSqlConnector.create(sqlService, from, 4);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " SELECT v, 'value-' || v FROM " + from,
                createMap(0, "value-0", 1, "value-1", 2, "value-2", 3, "value-3")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name + " WHERE __key > 0 AND __key < 3",
                asList(new Row(1, "value-1"), new Row(2, "value-2"))
        );
    }


    @Test
    public void test_insertValues() {
        String name = createRandomTopic();
        createMapping(name, false);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (__key, this) VALUES (1, '2')",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_insertSink() {
        String name = createRandomTopic();
        createMapping(name, false);

        assertTopicEventually(
                name,
                "SINK INTO " + name + " (this, __key) VALUES ('2', 1)",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_insertSink_simpleKeyFormat() {
        String name = createRandomTopic();
        createMapping(name, true);

        assertTopicEventually(
                name,
                "SINK INTO " + name + " (this, __key) VALUES ('2', 1)",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_insertNulls() {
        String name = createRandomTopic();
        createMapping(name, false);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                createMap(null, null)
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_insertWithProject() {
        String name = createRandomTopic();
        createMapping(name, false);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (this, __key) VALUES ('2', CAST(0 + 1 AS INT))",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_insertWithDynamicParameters() {
        String name = createRandomTopic();
        createMapping(name, false);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (this, __key) VALUES (?, CAST(0 + ? AS INT))",
                asList("2", 1),
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_selectWithDynamicParameters() {
        String name = createRandomTopic();
        createMapping(name, false);

        sqlService.execute("INSERT INTO " + name + " VALUES (1, '1'),  (2, '2')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key + ?, ABS(__key + ?), this FROM " + name + " WHERE __key + ? >= ?",
                asList(2, -10, 2, 4),
                singletonList(new Row(4L, 8L, "2"))
        );
    }

    @Test
    public void test_renameKey() {
        assertThatThrownBy(() ->
                kafkaMapping("map", true)
                        .fields("id INT EXTERNAL NAME __key",
                                "this VARCHAR")
                        .create())
                .hasMessageContaining("Cannot rename field: '__key'");

        assertThatThrownBy(() ->
                kafkaMapping("map", true)
                        .fields("__key INT EXTERNAL NAME renamed",
                                "this VARCHAR")
                        .create())
                .hasMessageContaining("Cannot rename field: '__key'");
    }

    @Test
    public void test_renameThis() {
        assertThatThrownBy(() ->
                kafkaMapping("map", true)
                        .fields("__key INT",
                                "name VARCHAR EXTERNAL NAME this")
                        .create())
                .hasMessageContaining("Cannot rename field: 'this'");

        assertThatThrownBy(() ->
                kafkaMapping("map", true)
                        .fields("__key INT",
                                "this VARCHAR EXTERNAL NAME renamed")
                        .create())
                .hasMessageContaining("Cannot rename field: 'this'");
    }

    @Test
    public void test_objectAndMappingNameDifferent() {
        String topicName = createRandomTopic();
        String tableName = randomName();

        kafkaMapping(tableName, false)
                .externalName(topicName)
                .create();

        kafkaTestSupport.produce(topicName, 1, "Alice");
        kafkaTestSupport.produce(topicName, 2, "Bob");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + tableName,
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + tableName,
                asList(new Row(1, "Alice"), new Row(2, "Bob"))
        );
    }

    @Test
    public void test_explicitKeyAndThis() {
        String topicName = createRandomTopic();
        kafkaMapping(topicName, false)
                .fields("__key INT",
                        "this VARCHAR")
                .create();

        kafkaTestSupport.produce(topicName, 1, "Alice");
        kafkaTestSupport.produce(topicName, 2, "Bob");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + topicName,
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + topicName,
                asList(new Row(1, "Alice"), new Row(2, "Bob"))
        );
    }

    @Test
    public void test_explicitKeyAndThisWithExternalNames() {
        String topicName = createRandomTopic();
        kafkaMapping(topicName, false)
                .fields("__key INT EXTERNAL NAME __key",
                        "this VARCHAR EXTERNAL NAME this")
                .create();

        kafkaTestSupport.produce(topicName, 1, "Alice");
        kafkaTestSupport.produce(topicName, 2, "Bob");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + topicName,
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Bob")
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + topicName,
                asList(new Row(1, "Alice"), new Row(2, "Bob"))
        );
    }

    @Test
    public void test_explicitKeyAndValueSerializers() {
        String name = createRandomTopic();
        kafkaMapping(name, false)
                .options("key.serializer", IntegerSerializer.class.getCanonicalName(),
                         "key.deserializer", IntegerDeserializer.class.getCanonicalName(),
                         "value.serializer", StringSerializer.class.getCanonicalName(),
                         "value.deserializer", StringDeserializer.class.getCanonicalName())
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (__key, this) VALUES (1, '2')",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_noKeyFormat() {
        String topicName = createRandomTopic();
        new SqlMapping(topicName, KafkaSqlConnector.class)
                .options(OPTION_VALUE_FORMAT, JAVA_FORMAT,
                         OPTION_VALUE_CLASS, Integer.class.getName(),
                         "bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                         "auto.offset.reset", "earliest")
                .create();

        sqlService.execute("INSERT INTO " + topicName + " VALUES(42)");

        assertRowsEventuallyInAnyOrder("SELECT * FROM " + topicName, singletonList(new Row(42)));
    }

    @Test
    public void test_noValueFormat() {
        assertThatThrownBy(() ->
                new SqlMapping("kafka", KafkaSqlConnector.class)
                        .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                                 OPTION_KEY_CLASS, String.class.getName())
                        .create())
                .hasMessage("Missing 'valueFormat' option");
    }

    @Test
    public void test_multipleFieldsForPrimitive_key() {
        test_multipleFieldsForPrimitive("__key");
    }

    @Test
    public void test_multipleFieldsForPrimitive_value() {
        test_multipleFieldsForPrimitive("this");
    }

    private void test_multipleFieldsForPrimitive(String fieldName) {
        assertThatThrownBy(() ->
                new SqlMapping("kafka", KafkaSqlConnector.class)
                        .fields(fieldName + " INT",
                                "field INT EXTERNAL NAME \"" + fieldName + ".field\"")
                        .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                                 OPTION_KEY_CLASS, Integer.class.getName(),
                                 OPTION_VALUE_FORMAT, JAVA_FORMAT,
                                 OPTION_VALUE_CLASS, Integer.class.getName())
                        .create())
                .hasMessage("The field '" + fieldName + "' is of type INTEGER, you can't map '"
                        + fieldName + ".field' too");
    }

    // test for https://github.com/hazelcast/hazelcast/issues/21455
    @Test
    public void test_nonExistentTopic() {
        String name = "nonExistentTopic";
        createMapping(name, false);

        ResultIterator<SqlRow> result = (ResultIterator<SqlRow>) sqlService.execute("select * from " + name).iterator();
        result.hasNext(500, TimeUnit.MILLISECONDS);
    }

    private static String createRandomTopic() {
        return createRandomTopic(INITIAL_PARTITION_COUNT);
    }

    private static void assertTopicEventually(String name, String sql, Map<Integer, String> expected) {
        assertTopicEventually(name, sql, emptyList(), expected);
    }

    private static void assertTopicEventually(
            String name,
            String sql,
            List<Object> arguments,
            Map<Integer, String> expected
    ) {
        SqlStatement statement = new SqlStatement(sql);
        arguments.forEach(statement::addParameter);

        sqlService.execute(statement);

        kafkaTestSupport.assertTopicContentsEventually(name, expected, false);
    }
}
