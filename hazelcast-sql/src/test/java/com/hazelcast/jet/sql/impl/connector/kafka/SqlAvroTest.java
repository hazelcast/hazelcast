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
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.jetty.server.Server;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SqlAvroTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static KafkaTestSupport kafkaTestSupport;
    private static Server schemaRegistry;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() throws Exception {
        initialize(1, null);
        sqlService = instance().getSql();

        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();

        Properties properties = new Properties();
        properties.put("listeners", "http://0.0.0.0:0");
        properties.put("kafkastore.bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        SchemaRegistryConfig config = new SchemaRegistryConfig(properties);
        SchemaRegistryRestApplication schemaRegistryApplication = new SchemaRegistryRestApplication(config);
        schemaRegistry = schemaRegistryApplication.createServer();
        schemaRegistry.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (schemaRegistry != null) {
            schemaRegistry.stop();
        }
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
        }
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + " ("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " VALUES (null, null)",
                createMap(
                        new GenericRecordBuilder(intSchema("id")).build(),
                        new GenericRecordBuilder(stringSchema("name")).set("name", null).build()
                )
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                createMap(
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Alice").build(),
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Bob").build()
                )
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
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
                        "{42=43}", // object values are stored as strings in avro format
                        null
                ))
        );
    }

    @Test
    public void when_createMappingNoColumns_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING kafka "
                        + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ('valueFormat'='avro')"))
                .hasMessage("Column list is required for Avro format");
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
                sqlService.execute("CREATE MAPPING kafka ("
                        + field + " VARCHAR"
                        + ", f VARCHAR EXTERNAL NAME \"" + otherField + ".f\""
                        + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ("
                        + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                        + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                        + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                        + ", 'auto.offset.reset'='earliest'"
                        + ")"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("Cannot use the '" + field + "' field with Avro serialization");
    }

    @Test
    public void test_writingToTopLevel() {
        String mapName = randomName();
        sqlService.execute("CREATE MAPPING " + mapName + "("
                + "id INT EXTERNAL NAME \"__key.id\""
                + ", name VARCHAR"
                + ") TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ("
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        sqlService.execute("INSERT INTO " + name + " VALUES (1, 'Alice')");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, this FROM " + name,
                singletonList(new Row(
                        new GenericRecordBuilder(intSchema("id")).set("id", 1).build(),
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Alice").build()
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
                + '\'' + OPTION_KEY_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", '" + OPTION_VALUE_FORMAT + "'='" + AVRO_FORMAT + '\''
                + ", 'key.serializer'='" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", 'key.deserializer'='" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", 'value.serializer'='" + KafkaAvroSerializer.class.getCanonicalName() + '\''
                + ", 'value.deserializer'='" + KafkaAvroDeserializer.class.getCanonicalName() + '\''
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        assertTopicEventually(
                name,
                "INSERT INTO " + name + " (value_name, key_name) VALUES ('Bob', 'Alice')",
                createMap(
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Alice").build(),
                        new GenericRecordBuilder(stringSchema("name")).set("name", "Bob").build()
                )
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                singletonList(new Row("Alice", "Bob"))
        );
    }

    @Test
    public void test_schemaIdForTwoQueriesIsEqual() {
        String topicName = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + topicName + " (__key INT, field1 VARCHAR) "
                + "TYPE Kafka "
                + "OPTIONS ("
                + "'keyFormat'='java'"
                + ", 'keyJavaClass'='java.lang.Integer'"
                + ", 'valueFormat'='avro'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'schema.registry.url'='" + schemaRegistry.getURI() + '\''
                + ", 'auto.offset.reset'='earliest')"
        );

        sqlService.execute("INSERT INTO " + topicName + " VALUES(42, 'foo')");
        sqlService.execute("INSERT INTO " + topicName + " VALUES(43, 'bar')");

        try (KafkaConsumer<Integer, byte[]> consumer = kafkaTestSupport.createConsumer(
                IntegerDeserializer.class, ByteArrayDeserializer.class, emptyMap(), topicName)
        ) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            List<Integer> schemaIds = new ArrayList<>();
            while (schemaIds.size() < 2) {
                if (System.nanoTime() > timeLimit) {
                    Assert.fail("Timeout waiting for the records from Kafka");
                }
                ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    int id = Bits.readInt(record.value(), 0, true);
                    schemaIds.add(id);
                }
            }
            assertEquals("The schemaIds of the two records don't match", schemaIds.get(0), schemaIds.get(1));
        }
    }

    private static String createRandomTopic() {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }

    private static void assertTopicEventually(String name, String sql, Map<Object, Object> expected) {
        sqlService.execute(sql);

        kafkaTestSupport.assertTopicContentsEventually(
                name,
                expected,
                KafkaAvroDeserializer.class,
                KafkaAvroDeserializer.class,
                ImmutableMap.of("schema.registry.url", schemaRegistry.getURI().toString())
        );
    }

    private static Schema intSchema(String fieldName) {
        return SchemaBuilder.record("jet.sql")
                            .fields()
                            .name(fieldName).type().unionOf().nullType().and().intType().endUnion().nullDefault()
                            .endRecord();
    }

    private static Schema stringSchema(String fieldName) {
        return SchemaBuilder.record("jet.sql")
                            .fields()
                            .name(fieldName).type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                            .endRecord();
    }
}
