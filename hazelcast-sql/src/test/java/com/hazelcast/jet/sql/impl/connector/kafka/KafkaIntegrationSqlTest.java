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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.SqlResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaIntegrationSqlTest extends KafkaSqlTestSupport {

    @Test
    public void should_read_from_imap_to_kafka() {
        String topicName = "testTopic";
        kafkaTestSupport.createTopic(topicName, 1);
        createConfluentKafkaMapping(topicName);
        String mapName = "testMap";
        createMap(mapName);

        executeSql("INSERT INTO " + mapName + " VALUES\n" +
                   "  (1, 'ABCD', 5.5, 10),\n" +
                   "  (2, 'EFGH', 14, 20);");

        assertMapContents(mapName, ImmutableMap.of(
                1, new HazelcastJsonValue("{\"ticker\":\"ABCD\",\"price\":\"5.5\",\"amount\":10}"),
                2, new HazelcastJsonValue("{\"ticker\":\"EFGH\",\"price\":\"14\",\"amount\":20}"))
        );

        executeSql("CREATE JOB testJob\n" +
                   "OPTIONS (\n" +
                   "  'processingGuarantee' = 'exactlyOnce'\n" +
                   ") AS\n" +
                   "SINK INTO " + topicName + "\n" +
                   "SELECT __key, ticker, price, amount FROM " + mapName);

        assertRowsEventuallyInAnyOrder(
                "SELECT __key,this FROM " + topicName,
                ImmutableList.of(new Row(
                        1,
                        ImmutableMap.of(
                                "ticker", "ABCD",
                                "price", "5.5",
                                "amount", 10)
                ), new Row(
                        2,
                        ImmutableMap.of(
                                "ticker", "EFGH",
                                "price", "14",
                                "amount", 20)
                ))
        );

        try (KafkaConsumer<Integer, String> consumer = kafkaTestSupport.createConsumer(topicName)) {
            assertTopicContentsEventually(consumer, ImmutableMap.of(
                    1, "{\"ticker\":\"ABCD\",\"price\":\"5.5\",\"amount\":10}",
                    2, "{\"ticker\":\"EFGH\",\"price\":\"14\",\"amount\":20}"));
        }
    }

    private static void assertMapContents(String mapName, Map<Integer, HazelcastJsonValue> expected) {
        Map<Integer, HazelcastJsonValue> mapContents = new HashMap<>(instance().getMap(mapName));
        assertTrueEventually(() -> assertThat(mapContents).containsAllEntriesOf(expected));
    }

    private void createMap(String mapName) {
        executeSql("CREATE OR REPLACE MAPPING " + mapName + " (\n" +
                   "            __key INT,\n" +
                   "            ticker VARCHAR,\n" +
                   "            price DECIMAL,\n" +
                   "            amount BIGINT)\n" +
                   "        TYPE IMap\n" +
                   "        OPTIONS (\n" +
                   "            'keyFormat'='int',\n" +
                   "    'valueFormat'='json-flat'\n" +
                   ");");
    }

    private void createConfluentKafkaMapping(String topicName) {
        String createMappingQuery =
                format("CREATE OR REPLACE MAPPING %s (\n" +
                       "                    __key INT,\n" +
                       "                    ticker VARCHAR,\n" +
                       "                    price DECIMAL,\n" +
                       "                    amount BIGINT)\n" +
                       "        TYPE Kafka\n" +
                       "        OPTIONS (\n" +
                       "            'keyFormat'='int',\n" +
                       "            'valueFormat' = 'json-flat',\n" +
                       "            'bootstrap.servers' = '%s',\n" +
                       "            'auto.offset.reset' = 'earliest',\n" +
                       "            'session.timeout.ms' = '45000',\n" +
                       "            'acks' = 'all'\n" +
                       ");", topicName, kafkaTestSupport.getBrokerConnectionString());
        executeSql(createMappingQuery);
    }

    private void executeSql(String query) {
        logger.info("Execute sql: " + query);
        try {
            try (SqlResult ignored = sqlService.execute(query)) {
                //nop
            }
        } catch (Exception ex) {
            logger.warning("Error while executing SQL: " + query, ex);
            throw ex;
        }
    }

    public void assertTopicContentsEventually(KafkaConsumer<Integer, String> consumer, Map<Integer, String> expected) {
        Map<Integer, String> collected = new HashMap<>();
        assertTrueEventually(() -> {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(5));
            logger.info("Polled records: " + records.count());
            for (ConsumerRecord<Integer, String> record : records) {
                collected.put(record.key(), record.value());
            }
            assertThat(collected).containsAllEntriesOf(expected);
        });
    }
}
