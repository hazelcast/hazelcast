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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Properties;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class KafkaSqlTestSupport extends SqlTestSupport {
    protected static KafkaTestSupport kafkaTestSupport;
    protected static SqlService sqlService;

    @BeforeClass
    public static void setup() throws Exception {
        setup(1, null);
    }

    protected static void setup(int memberCount, Config config) throws Exception {
        initialize(memberCount, config);
        createKafkaCluster();
    }

    protected static void setupWithClient(int memberCount, Config config, ClientConfig clientConfig) throws Exception {
        initializeWithClient(memberCount, config, clientConfig);
        createKafkaCluster();
    }

    private static void createKafkaCluster() throws Exception {
        sqlService = instance().getSql();
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
    }

    protected static void createSchemaRegistry() throws Exception {
        Properties properties = new Properties();
        properties.put("listeners", "http://0.0.0.0:0");
        properties.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG,
                kafkaTestSupport.getBrokerConnectionString());
        // We increase the timeout (default is 500 ms) because when Kafka is under load,
        // the schema registry may give "RestClientException: Register operation timed out".
        properties.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, "5000");
        SchemaRegistryConfig config = new SchemaRegistryConfig(properties);
        kafkaTestSupport.createSchemaRegistry(config);
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownSchemaRegistry();
            kafkaTestSupport.shutdownKafkaCluster();
        }
    }

    protected static String createRandomTopic(int partitionCount) {
        String topicName = "t_" + randomString().replace('-', '_');
        kafkaTestSupport.createTopic(topicName, partitionCount);
        return topicName;
    }

    public static GenericRecord createRecord(Schema schema, Type.Field[] fields, Object[] values) {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        for (int i = 0; i < fields.length; i++) {
            Schema.Field field = schema.getField(fields[i].name);
            if (values[i] == null) {
                record.set(field, null);
            } else {
                Schema fieldSchema = unwrapNullableType(field.schema());
                record.set(field, fieldSchema.getType() == Schema.Type.RECORD
                        ? createRecord(fieldSchema, fields[i].type.fields, (Object[]) values[i])
                        : values[i]);
            }
        }
        return record.build();
    }

    public static GenericRecord createRecord(Schema schema, Object... values) {
        return createRecord(schema, new Type(schema).fields, values);
    }

    protected static void createSqlKafkaDataConnection(String dlName, boolean isShared, String options) {
        try (SqlResult result = instance().getSql().execute(
                "CREATE DATA CONNECTION " + dlName + " TYPE Kafka "
                        + (isShared ? " SHARED " : " NOT SHARED ") + options
        )) {
            assertThat(result.updateCount()).isZero();
        }

    }

    protected static void createSqlKafkaDataConnection(String dlName, boolean isShared) {
        createSqlKafkaDataConnection(dlName, isShared, isShared ? defaultSharedOptions() : defaultNotSharedOptions());
    }

    protected static void createKafkaMappingUsingDataConnection(String name, String dataConnection, String sqlOptions) {
        try (SqlResult result = instance().getSql().execute("CREATE OR REPLACE MAPPING " + name
                + " DATA CONNECTION " + quoteName(dataConnection) + "\n" + sqlOptions
        )) {
            assertThat(result.updateCount()).isZero();
        }
    }

    protected static String defaultSharedOptions() {
        return String.format("OPTIONS ( " +
                        "'bootstrap.servers' = '%s', " +
                        "'key.serializer' = '%s', " +
                        "'key.deserializer' = '%s', " +
                        "'value.serializer' = '%s', " +
                        "'value.deserializer' = '%s', " +
                        "'auto.offset.reset' = 'earliest') ",
                kafkaTestSupport.getBrokerConnectionString(),
                IntegerSerializer.class.getCanonicalName(),
                IntegerDeserializer.class.getCanonicalName(),
                StringSerializer.class.getCanonicalName(),
                StringDeserializer.class.getCanonicalName());
    }

    protected static String defaultNotSharedOptions() {
        return String.format("OPTIONS ( "
                        + "'bootstrap.servers' = '%s', "
                        + "'auto.offset.reset' = 'earliest') ",
                kafkaTestSupport.getBrokerConnectionString());
    }

    protected static String constructMappingOptions(String keyFormat, String valueFormat) {
        return "OPTIONS ('" + OPTION_KEY_FORMAT + "'='" + keyFormat + "'"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + valueFormat + "')";
    }
}
