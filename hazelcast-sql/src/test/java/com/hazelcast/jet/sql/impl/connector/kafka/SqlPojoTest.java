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

import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValue;
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValueDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.AllCanonicalTypesValueSerializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.JavaDeserializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.JavaSerializer;
import com.hazelcast.jet.sql.impl.connector.kafka.model.Person;
import com.hazelcast.jet.sql.impl.connector.kafka.model.PersonId;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlPojoTest extends KafkaSqlTestSupport {
    private static final int INITIAL_PARTITION_COUNT = 4;

    @BeforeClass
    public static void setup() throws Exception {
        setup(1, null);
    }

    private static <K, V> SqlMapping kafkaMapping(String name, Class<K> keyClass, Class<V> valueClass,
                                                  Class<? extends Serializer<? super K>> keySerializerClass,
                                                  Class<? extends Deserializer<? super K>> keyDeserializerClass,
                                                  Class<? extends Serializer<? super V>> valueSerializerClass,
                                                  Class<? extends Deserializer<? super V>> valueDeserializerClass) {
        return new SqlMapping(name, KafkaSqlConnector.class)
                .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                         OPTION_KEY_CLASS, keyClass.getName(),
                         OPTION_VALUE_FORMAT, JAVA_FORMAT,
                         OPTION_VALUE_CLASS, valueClass.getName(),
                         "bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                         "key.serializer", keySerializerClass.getCanonicalName(),
                         "key.deserializer", keyDeserializerClass.getCanonicalName(),
                         "value.serializer", valueSerializerClass.getCanonicalName(),
                         "value.deserializer", valueDeserializerClass.getCanonicalName(),
                         "auto.offset.reset", "earliest");
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        kafkaMapping(name, PersonId.class, Person.class,
                        JavaSerializer.class, JavaDeserializer.class,
                        JavaSerializer.class, JavaDeserializer.class)
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                Map.of(new PersonId(), new Person())
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(null, null))
        );
    }

    @Test
    public void test_fieldsShadowing() {
        String name = createRandomTopic();
        kafkaMapping(name, PersonId.class, Person.class,
                        JavaSerializer.class, JavaDeserializer.class,
                        JavaSerializer.class, JavaDeserializer.class)
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (1, 'Alice')",
                Map.of(new PersonId(1), new Person(null, "Alice"))
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, "Alice"))
        );
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        kafkaMapping(name, PersonId.class, Person.class,
                        JavaSerializer.class, JavaDeserializer.class,
                        JavaSerializer.class, JavaDeserializer.class)
                .fields("key_id INT EXTERNAL NAME \"__key.id\"",
                        "value_id INT EXTERNAL NAME \"this.id\"",
                        "name VARCHAR")
                .create();

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_id, key_id) VALUES (2, 1)",
                Map.of(new PersonId(1), new Person(2, null))
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT  key_id, value_id FROM " + name,
                List.of(new Row(1, 2))
        );
    }

    @Test
    public void test_allTypes() {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = createRandomTopic();
        kafkaMapping(to, PersonId.class, AllCanonicalTypesValue.class,
                        JavaSerializer.class, JavaDeserializer.class,
                        AllCanonicalTypesValueSerializer.class, AllCanonicalTypesValueDeserializer.class)
                .create();

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
                List.of(new Row(
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
        kafkaMapping(topicName, Integer.class, Person.class,
                        IntegerSerializer.class, IntegerDeserializer.class,
                        JavaSerializer.class, JavaDeserializer.class)
                .fields("__key INT")
                .fieldsIf(explicit, "this OBJECT")
                .fields("name VARCHAR")
                .create();

        if (explicit) {
            assertThatThrownBy(() ->
                    sqlService.execute("INSERT INTO " + topicName + " VALUES(1, null, 'foo')"))
                    .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");
        }

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + topicName + "(__key, this) VALUES(1, null)"))
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        sqlService.execute("INSERT INTO " + topicName + (explicit ? "(__key, name)" : "") + " VALUES (1, 'foo')");

        assertRowsEventuallyInAnyOrder("SELECT __key, this, name FROM " + topicName,
                List.of(new Row(1, new Person(null, "foo"), "foo")));
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = createRandomTopic();
        kafkaMapping(name, PersonId.class, Person.class,
                        JavaSerializer.class, JavaDeserializer.class,
                        JavaSerializer.class, JavaDeserializer.class)
                .create();

        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(new PersonId(1), new Person(null, "Alice")))
        );
    }

    @Test
    public void test_customType() {
        new SqlType("person_type").create();
        kafkaMapping("m", Integer.class, ClzWithPerson.class,
                        IntegerSerializer.class, IntegerDeserializer.class,
                        JavaSerializer.class, JavaDeserializer.class)
                .fields("__key INT",
                        "outerField INT",
                        "person person_type")
                .create();

        sqlService.execute("insert into m values (0, 1, (2, 'foo'))");

        assertRowsEventuallyInAnyOrder("select outerField, (person).id, (person).name from m",
            rows(3, 1, 2, "foo"));
    }

    private static String createRandomTopic() {
        return createRandomTopic(INITIAL_PARTITION_COUNT);
    }

    private static void assertTopicEventually(String name, String sql, Map<PersonId, Person> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(
                name,
                expected,
                JavaDeserializer.class,
                JavaDeserializer.class
        );
    }

    public static class ClzWithPerson implements Serializable {
        @SuppressWarnings("unused")
        public int outerField;
        public Person person;
    }
}
