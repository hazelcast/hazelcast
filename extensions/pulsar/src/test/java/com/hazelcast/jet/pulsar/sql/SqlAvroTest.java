/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.jet.pulsar.PulsarTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.SerialTest;
import org.apache.avro.SchemaBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.SqlTestSupport.assertRowsEventuallyInAnyOrder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@SerialTest
public class SqlAvroTest extends PulsarTestSupport {

    private HazelcastInstance[] instances;
    private SqlService sql;
    private String topicName;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        var conf = smallInstanceConfig();
        conf.addDataConnectionConfig(PulsarDataConnection.pulsarDataConnectionConf("testPulsar",
                                                                                   pulsarContainer.getPulsarBrokerUrl(),
                                                                                   pulsarContainer.getHttpServiceUrl(),
                                                                                   true));
        instances = createHazelcastInstances(conf, 2);
        sql = instances[0].getSql();
        topicName = "SqlAvroTest_" + sanitizedTestName(testInfo);
    }

    private void createMapping(String schemaJson) {
        var schema = schemaJson == null ? "" : schemaJson.replace("'", "''");
        new SqlTestSupport.SqlMapping("SqlAvroTest", "testPulsar").externalName(topicName)
            .fields("__key varchar", "name varchar", "favoriteNumber int")
            .options("keyFormat", "varchar", "valueFormat", "avro")
            .optionsIf(schemaJson != null, "valueAvroSchema", schema)
            .createOrReplace(instances[0]);
    }

    @Test
    public void readsSqlDataFromJava() throws Exception {
        createMapping(new String(Schema.AVRO(AvroUser.class).getSchemaInfo().getSchema(), UTF_8));
        Map<String, AvroUser> results = Collections.synchronizedMap(new HashMap<>());
        try (var reader = newReader(topicName, Schema.AVRO(AvroUser.class), results)) {
            List<AvroUser> users = new ArrayList<>();
            users.add(new AvroUser("Tomasz", 3));
            users.add(new AvroUser("Mary", 12));
            users.add(new AvroUser(null, null));

            int i = 1;
            for (AvroUser user : users) {
                sql.executeUpdate("INSERT INTO SqlAvroTest (__key, name, favoriteNumber) VALUES (?, ?, ?)",
                                  String.valueOf(i++), user.getName(), user.getFavoriteNumber());
            }

            reader.hasMessageAvailable();
            assertTrueEventually(() -> {
                assertThat(results).containsAllEntriesOf(Map.of(
                    "1", users.get(0),
                    "2", users.get(1),
                    "3", users.get(2)
                ));
            }, 30);
        }
    }

    @Test
    public void readsJavaDataFromSql() throws Exception {
        createMapping(new String(Schema.AVRO(AvroUser.class).getSchemaInfo().getSchema(), UTF_8));
        try (Producer<AvroUser> producer = getClient()
                    .newProducer(Schema.AVRO(AvroUser.class))
                    .topic(topicName)
                    .create()) {
            List<AvroUser> users = new ArrayList<>();
            users.add(new AvroUser("Tomasz", 3));
            users.add(new AvroUser("Mary", 12));
            users.add(new AvroUser(null, null));

            List<SqlTestSupport.Row> expected = new ArrayList<>();
            int i = 1;
            for (AvroUser user : users) {
                int id = i++;
                producer.newMessage().key(String.valueOf(id)).value(user).send();
                expected.add(new SqlTestSupport.Row(String.valueOf(id), user.getName(), user.getFavoriteNumber()));
            }
            assertRowsEventuallyInAnyOrder(instances[0], "SELECT * FROM SqlAvroTest", expected);
        }
    }

    @Test
    public void sendsAndReadsMessages() {
        createMapping(null);
        List<AvroUser> users = new ArrayList<>();
        users.add(new AvroUser("Tomasz", 3));
        users.add(new AvroUser("Mary", 12));
        users.add(new AvroUser(null, null));

        int i = 1;
        for (AvroUser user : users) {
            sql.executeUpdate("INSERT INTO SqlAvroTest VALUES (?, ?, ?)",
                              String.valueOf(i++), user.getName(), user.getFavoriteNumber());
        }

        i = 1;
        List<SqlTestSupport.Row> expected = new ArrayList<>();
        for (AvroUser user : users) {
            expected.add(new SqlTestSupport.Row(String.valueOf(i++), user.getName(), user.getFavoriteNumber()));
        }
        assertRowsEventuallyInAnyOrder(instances[0], "SELECT * FROM SqlAvroTest", expected);
    }

    @Test
    public void readsGenericRecordsWithDifferentSchemaVersions() throws Exception {
        createMapping(null);
        var oldSchema = SchemaBuilder.record("jet.sql").fields().optionalString("name").endRecord();
        var newSchema = SchemaBuilder.record("jet.sql").fields()
                .optionalInt("favoriteNumber")
                .optionalString("name")
                .endRecord();

        produceGenericRecord(oldSchema, "1", "Tomasz", null);
        produceGenericRecord(newSchema, "2", "Mary", 12);

        assertRowsEventuallyInAnyOrder(instances[0], "SELECT * FROM SqlAvroTest", List.of(
                new SqlTestSupport.Row("1", "Tomasz", null),
                new SqlTestSupport.Row("2", "Mary", 12)));
    }

    @Test
    public void writesSqlDataAsGenericRecords() throws Exception {
        createMapping(null);
        Map<String, org.apache.pulsar.client.api.schema.GenericRecord> results =
                Collections.synchronizedMap(new HashMap<>());
        try (var reader = newReader(topicName, Schema.AUTO_CONSUME(), results)) {
            sql.executeUpdate("INSERT INTO SqlAvroTest VALUES ('1', 'Tomasz', 3)");
            assertTrueEventually(() -> {
                assertThat(results).containsKey("1");
                assertThat(results.get("1").getField("name")).isEqualTo("Tomasz");
                assertThat(results.get("1").getField("favoriteNumber")).isEqualTo(3);
            });
        }
    }

    private void produceGenericRecord(org.apache.avro.Schema schema, String key, String name, Integer number)
            throws Exception {
        var pulsarSchema = Schema.generic(SchemaInfo.builder()
                .name(schema.getFullName())
                .type(SchemaType.AVRO)
                .schema(schema.toString().getBytes(UTF_8))
                .build());
        var record = pulsarSchema.newRecordBuilder().set("name", name);
        if (schema.getField("favoriteNumber") != null) {
            record.set("favoriteNumber", number);
        }
        try (var producer = getClient().newProducer(pulsarSchema).topic(topicName).create()) {
            producer.newMessage().key(key).value(record.build()).send();
        }
    }
}
