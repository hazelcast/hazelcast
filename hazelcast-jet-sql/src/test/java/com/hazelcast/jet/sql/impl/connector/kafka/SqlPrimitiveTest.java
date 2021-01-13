/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
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

        kafkaTestSupport = new KafkaTestSupport();
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
        TestBatchSqlConnector.create(sqlService, from, 2);

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " SELECT v, 'value-' || v FROM " + from,
                createMap(0, "value-0", 1, "value-1")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                asList(new Row(0, "value-0"), new Row(1, "value-1"))
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
                "INSERT INTO " + name + " (this, __key) VALUES (2, CAST(0 + 1 AS INT))",
                createMap(1, "2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "2"))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME __key"
                + ", name VARCHAR EXTERNAL NAME this"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
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
                "INSERT INTO " + name + " (id, name) VALUES (2, 'value-2')",
                createMap(2, "value-2")
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(2, "value-2"))
        );
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

        sqlService.execute("SINK INTO " + topicName + " VALUES(42)");

        assertRowsEventuallyInAnyOrder("SELECT * FROM " + topicName, singletonList(new Row(42)));
    }

    @Test
    public void test_noValueFormat() {
        String topicName = randomName();
        assertThatThrownBy(
                () -> sqlService.execute("CREATE MAPPING " + topicName + " TYPE " + KafkaSqlConnector.TYPE_NAME + " "
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
        String mapName = randomName();
        assertThatThrownBy(
                () -> sqlService.execute("CREATE MAPPING " + mapName + "("
                        + fieldName + " INT"
                        + ", field INT EXTERNAL NAME \"" + fieldName + ".field\""
                        + ") TYPE " + KafkaSqlConnector.TYPE_NAME
                        + " OPTIONS ("
                        + '\'' + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "',"
                        + '\'' + OPTION_VALUE_CLASS + "'='" + Integer.class.getName() + "',"
                        + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "',"
                        + '\'' + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'"
                        + ")")
        ).hasMessage("The field '" + fieldName + "' is of type INTEGER, you can't map '" + fieldName + ".field' too");
    }

    @Test
    public void test_valueFieldMappedUnderTopLevelKeyName() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + "(\n"
                + "__key BIGINT EXTERNAL NAME id\n"
                + ", field BIGINT\n"
                + ')' + "TYPE " + KafkaSqlConnector.TYPE_NAME + " \n"
                + "OPTIONS (\n"
                + '\''  + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JSON_FORMAT + "'\n"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + "'\n"
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        kafkaTestSupport.produce(name, 1, "{\"id\":123,\"field\":456}");

        assertRowsEventuallyInAnyOrder(
                "select __key, field from " + name,
                singletonList(new Row(123L, 456L))
        );
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void assertTopicEventually(String name, String sql, Map<Integer, String> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(name, expected, false);
    }
}
