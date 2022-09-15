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

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.ResultIterator;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
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

public class SqlPrimitiveTest extends SqlTestSupport {

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
    public void test_insertSelect() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + name + " VALUES (1, '1'),  (2, '2')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key + ?, ABS(__key + ?), this FROM " + name + " WHERE __key + ? >= ?",
                asList(2, -10, 2, 4),
                singletonList(new Row(4L, 8L, "2"))
        );
    }

    @Test
    public void test_renameKey() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "id INT EXTERNAL NAME __key"
                + ", this VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        )).hasMessageContaining("Cannot rename field: '__key'");

        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "__key INT EXTERNAL NAME renamed"
                + ", this VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        )).hasMessageContaining("Cannot rename field: '__key'");
    }

    @Test
    public void test_renameThis() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "__key INT"
                + ", name VARCHAR EXTERNAL NAME this"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        )).hasMessageContaining("Cannot rename field: 'this'");

        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING map ("
                + "__key INT"
                + ", this VARCHAR EXTERNAL NAME renamed"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        )).hasMessageContaining("Cannot rename field: 'this'");
    }

    @Test
    public void test_objectAndMappingNameDifferent() {
        String topicName = createRandomTopic();
        String tableName = randomName();

        sqlService.execute("CREATE MAPPING " + tableName + " EXTERNAL NAME " + topicName + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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

        sqlService.execute("CREATE MAPPING " + topicName + '('
                + "__key INT,"
                + "this VARCHAR" +
                ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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

        sqlService.execute("CREATE MAPPING " + topicName + '('
                + "__key INT EXTERNAL NAME __key,"
                + "this VARCHAR EXTERNAL NAME this" +
                ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'key.serializer'='" + IntegerSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + IntegerDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + StringSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + StringDeserializer.class.getCanonicalName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

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
        sqlService.execute("CREATE MAPPING " + topicName + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "',"
                + '\'' + OPTION_VALUE_CLASS + "'='" + Integer.class.getName() + "'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + topicName + " VALUES(42)");

        assertRowsEventuallyInAnyOrder("SELECT * FROM " + topicName, singletonList(new Row(42)));
    }

    @Test
    public void test_noValueFormat() {
        assertThatThrownBy(
                () -> sqlService.execute("CREATE MAPPING kafka "
                        + "TYPE " + KafkaSqlConnector.TYPE_NAME + " "
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "',"
                        + '\'' + OPTION_KEY_CLASS + "'='" + String.class.getName() + "'"
                        + ")"))
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
        assertThatThrownBy(
                () -> sqlService.execute("CREATE MAPPING kafka ("
                        + fieldName + " INT"
                        + ", field INT EXTERNAL NAME \"" + fieldName + ".field\""
                        + ") TYPE " + KafkaSqlConnector.TYPE_NAME
                        + " OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                        + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                        + ", '" + OPTION_VALUE_CLASS + "'='" + Integer.class.getName() + '\''
                        + ")")
        ).hasMessage("The field '" + fieldName + "' is of type INTEGER, you can't map '" + fieldName + ".field' too");
    }

    // test for https://github.com/hazelcast/hazelcast/issues/21455
    @Test
    public void test_nonExistentTopic() {
        String name = "nonExistentTopic";
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + String.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        ResultIterator<SqlRow> result = (ResultIterator<SqlRow>) sqlService.execute("select * from " + name).iterator();
        result.hasNext(500, TimeUnit.MILLISECONDS);
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
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
