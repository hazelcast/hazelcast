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

import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.kafka.SqlAvroTest.ID_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.kafka.SqlAvroTest.NAME_SCHEMA;
import static java.util.Arrays.asList;
import static java.util.Arrays.copyOf;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class SqlAvroSchemaEvolutionTest extends KafkaSqlTestSupport {
    static final Schema NAME_SSN_SCHEMA = SchemaBuilder.record("jet.sql")
            .fields()
            .optionalString("name")
            .optionalLong("ssn")
            .endRecord();
    static final Schema NAME_SSN_SCHEMA2 = SchemaBuilder.record("jet.sql2")
            .fields()
            .optionalString("name")
            .optionalLong("ssn")
            .endRecord();

    @Parameters(name = "{0}, updateMapping=[{1}]")
    public static Iterable<Object[]> parameters() {
        return parameters(
                asList("TopicNameStrategy", "TopicRecordNameStrategy", "RecordNameStrategy"),
                asList(false, true));
    }

    @Parameter(0)
    public String subjectNameStrategy;
    @Parameter(1)
    public boolean updateMapping;

    /**
     * Indicates whether {@link #subjectNameStrategy} is {@code TopicNameStrategy}.
     */
    private boolean topicNameStrategy;
    /**
     * In format {@code <topic>-value}, {@code <topic>-<record>} or {@code <record>}
     * depending on {@link #subjectNameStrategy}.
     */
    private String valueSubjectName;
    /**
     * Name of the current Kafka topic and corresponding mapping.
     */
    private String name;

    @BeforeClass
    public static void initialize() throws Exception {
        createSchemaRegistry();
    }

    @Before
    public void before() throws Exception {
        name = createRandomTopic(1);
        kafkaTestSupport.setProducerProperties(name, Map.of(
                "schema.registry.url", kafkaTestSupport.getSchemaRegistryURI().toString(),
                "value.subject.name.strategy", "io.confluent.kafka.serializers.subject." + subjectNameStrategy
        ));
        topicNameStrategy = subjectNameStrategy.equals("TopicNameStrategy");
        switch (subjectNameStrategy) {
            case "TopicNameStrategy":       valueSubjectName = name + "-value"; break;
            case "TopicRecordNameStrategy": valueSubjectName = name + "-jet.sql"; break;
            case "RecordNameStrategy":      valueSubjectName = "jet.sql";
        }
    }

    private SqlMapping kafkaMapping() {
        return new SqlMapping(name, KafkaSqlConnector.class).options(
                OPTION_KEY_FORMAT, AVRO_FORMAT,
                OPTION_VALUE_FORMAT, AVRO_FORMAT,
                "bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                "schema.registry.url", kafkaTestSupport.getSchemaRegistryURI(),
                "auto.offset.reset", "earliest",
                "value.subject.name.strategy", "io.confluent.kafka.serializers.subject." + subjectNameStrategy
        );
    }

    @Test
    public void test_autoRegisterSchema() throws SchemaRegistryException {
        kafkaMapping()
            .fields("id INT EXTERNAL NAME \"__key.id\"",
                    "name VARCHAR")
            .create();

        insertInitialRecordAndAlterSchema();

        if (updateMapping) {
            kafkaMapping()
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT")
                .optionsIf(!topicNameStrategy,
                           "valueAvroRecordName", "jet.sql2")
                .createOrReplace();
        }

        insertAndAssertRecords();
    }

    @Test
    public void test_useLatestSchema() throws SchemaRegistryException {
        // create initial schema
        kafkaTestSupport.registerSchema(name + "-key", ID_SCHEMA);
        kafkaTestSupport.registerSchema(valueSubjectName, NAME_SCHEMA);

        kafkaMapping()
            .fields("id INT EXTERNAL NAME \"__key.id\"",
                    "name VARCHAR")
            .options("auto.register.schemas", false,
                     "use.latest.version", true)
            .create();

        insertInitialRecordAndAlterSchema();

        if (updateMapping) {
            kafkaMapping()
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT")
                .options("auto.register.schemas", false,
                         "use.latest.version", true)
                .optionsIf(!topicNameStrategy,
                           "valueAvroRecordName", "jet.sql2")
                .createOrReplace();
        }

        if (topicNameStrategy && !updateMapping) {
            // insert record against mapping's schema
            assertThatThrownBy(() -> insertRecord(29, "Bob"))
                    .hasMessageContaining("Error serializing Avro message");
        } else {
            insertAndAssertRecords();
        }
    }

    @Test
    public void test_useSpecificSchema() throws SchemaRegistryException {
        // create initial schema
        int keySchemaId = kafkaTestSupport.registerSchema(name + "-key", ID_SCHEMA);
        int valueSchemaId = kafkaTestSupport.registerSchema(valueSubjectName, NAME_SCHEMA);

        kafkaMapping()
            .fields("id INT EXTERNAL NAME \"__key.id\"",
                    "name VARCHAR")
            .options("auto.register.schemas", false,
                     "key.schema.id", keySchemaId,
                     "value.schema.id", valueSchemaId)
            .create();

        int valueSchemaId2 = insertInitialRecordAndAlterSchema();

        if (updateMapping) {
            kafkaMapping()
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT")
                .options("auto.register.schemas", false,
                         "key.schema.id", keySchemaId,
                         "value.schema.id", valueSchemaId2)
                .optionsIf(!topicNameStrategy,
                           "valueAvroRecordName", "jet.sql2")
                .createOrReplace();
        }

        insertAndAssertRecords();
    }

    private int insertInitialRecordAndAlterSchema() throws SchemaRegistryException {
        // insert initial record
        insertRecord(13, "Alice");
        assertEquals(1, kafkaTestSupport.getLatestSchemaVersion(valueSubjectName));

        // alter schema externally
        int valueSchemaId;
        if (topicNameStrategy) {
            valueSchemaId = kafkaTestSupport.registerSchema(valueSubjectName, NAME_SSN_SCHEMA);
            assertEquals(2, kafkaTestSupport.getLatestSchemaVersion(valueSubjectName));
        } else {
            valueSchemaId = kafkaTestSupport.registerSchema(valueSubjectName + "2", NAME_SSN_SCHEMA2);
            assertEquals(1, kafkaTestSupport.getLatestSchemaVersion(valueSubjectName));
            assertEquals(1, kafkaTestSupport.getLatestSchemaVersion(valueSubjectName + "2"));
        }
        return valueSchemaId;
    }

    private void insertAndAssertRecords() throws SchemaRegistryException {
        int fields = updateMapping ? 3 : 2;

        // insert record against mapping's schema
        insertRecord(copyOf(row(29, "Bob", 123456789L), fields));

        // insert record against old schema externally
        kafkaTestSupport.produce(name, createRecord(ID_SCHEMA, 31), createRecord(NAME_SCHEMA, "Carol"));

        // insert record against new schema externally
        kafkaTestSupport.produce(name, createRecord(ID_SCHEMA, 47),
                createRecord(topicNameStrategy ? NAME_SSN_SCHEMA : NAME_SSN_SCHEMA2, "Dave", 123456789L));

        // insert record against mapping's schema again
        insertRecord(copyOf(row(53, "Erin", 987654321L), fields));

        if (topicNameStrategy) {
            assertEquals(2, kafkaTestSupport.getLatestSchemaVersion(valueSubjectName));
        } else {
            assertEquals(1, kafkaTestSupport.getLatestSchemaVersion(valueSubjectName));
            assertEquals(1, kafkaTestSupport.getLatestSchemaVersion(valueSubjectName + "2"));
        }

        // assert both initial & evolved records are correctly read
        Object[][] records = {
                { 13, "Alice", null },
                { 29, "Bob", 123456789L },
                { 31, "Carol", null },
                { 47, "Dave", 123456789L },
                { 53, "Erin", 987654321L }
        };
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                Arrays.stream(records).map(record -> new Row(copyOf(record, fields))).collect(toList())
        );
    }

    private void insertRecord(Object... values) {
        insertLiterals(instance(), name, values);
    }
}
