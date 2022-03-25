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
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValue;
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValueDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValueSerializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.Person;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonId;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonIdDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonIdSerializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonSerializer;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlPojoTest extends SqlTestSupport {

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
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'key.serializer'='" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + PersonSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                createMap(new PersonId(), new Person())
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsShadowing() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'key.serializer'='" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + PersonSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (1, 'Alice')",
                createMap(new PersonId(1), new Person(null, "Alice"))
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "key_id INT EXTERNAL NAME \"__key.id\""
                + ", value_id INT EXTERNAL NAME \"this.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'key.serializer'='" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + PersonSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_id, key_id) VALUES (2, 1)",
                createMap(new PersonId(1), new Person(2, null))
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT  key_id, value_id FROM " + name,
                singletonList(new Row(1, 2))
        );
    }

    @Test
    public void test_allTypes() {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + to + ' '
                + " TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + AllCanonicalTypesValue.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'key.serializer'='" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + AllCanonicalTypesValueSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + AllCanonicalTypesValueDeserializer.class.getCanonicalName() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + to + "("
                + "id"
                + ", string"
                + ", boolean0"
                + ", byte0"
                + ", short0"
                + ", int0"
                + ", long0"
                + ", float0"
                + ", double0"
                + ", \"decimal\""
                + ", \"time\""
                + ", \"date\""
                + ", \"timestamp\""
                + ", timestampTz"
                + ", object"
                + ") SELECT "
                + "CAST(1 AS INT)"
                + ", string, \"boolean\", byte, short, \"int\", long, \"float\", \"double\", \"decimal\", " +
                "\"time\", \"date\", \"timestamp\", timestampTz, \"object\""
                + " FROM " + from + " f"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT "
                        + "id"
                        + ", string"
                        + ", boolean0"
                        + ", byte0"
                        + ", short0"
                        + ", int0"
                        + ", long0"
                        + ", float0"
                        + ", double0"
                        + ", \"decimal\""
                        + ", \"time\""
                        + ", \"date\""
                        + ", \"timestamp\""
                        + ", timestampTz"
                        + ", object"
                        + " FROM " + to,
                singletonList(new Row(
                        1,
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
                        null
                ))
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

    public void test_writingToTopLevel(boolean explicit) {
        String topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + "("
                + "__key INT"
                + (explicit ? ", this OBJECT" : "")
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + "\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + "'\n"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'key.serializer'='" + IntegerSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + IntegerDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + PersonSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        if (explicit) {
            assertThatThrownBy(() ->
                    sqlService.execute("INSERT INTO " + topicName + " VALUES(1, null, 'foo')"))
                    .isInstanceOf(HazelcastSqlException.class)
                    .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");
        }

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + topicName + "(__key, this) VALUES(1, null)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        sqlService.execute("INSERT INTO " + topicName + (explicit ? "(__key, name)" : "") + " VALUES (1, 'foo')");

        assertRowsEventuallyInAnyOrder("SELECT __key, this, name FROM " + topicName,
                singletonList(new Row(1, new Person(null, "foo"), "foo")));
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_KEY_CLASS + "'='" + PersonId.class.getName() + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + JAVA_FORMAT + '\''
                + ", '" + OPTION_VALUE_CLASS + "'='" + Person.class.getName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'key.serializer'='" + PersonIdSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + PersonIdDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + PersonSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + PersonDeserializer.class.getCanonicalName() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, this FROM " + name,
                singletonList(new Row(new PersonId(1), new Person(null, "Alice")))
        );
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void assertTopicEventually(String name, String sql, Map<PersonId, Person> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(
                name,
                expected,
                PersonIdDeserializer.class,
                PersonDeserializer.class
        );
    }
}
