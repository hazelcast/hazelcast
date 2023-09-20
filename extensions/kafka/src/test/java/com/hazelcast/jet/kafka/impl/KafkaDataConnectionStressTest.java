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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.impl.connector.SinkStressTestUtil;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.jet.kafka.impl.KafkaTestSupport.KAFKA_MAX_BLOCK_MS;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KafkaDataConnectionStressTest extends SimpleTestInClusterSupport {

    private static final int PARTITION_COUNT = 20;

    private static KafkaTestSupport kafkaTestSupport;

    @Parameter
    public boolean shared;

    @Parameter(1)
    public boolean exactlyOnce;

    @Parameter(2)
    public boolean graceful;

    private final String sourceIMapName = randomMapName();
    private Properties properties;
    private String topic;
    private String dataConnectionName;
    private IMap<Integer, String> sourceIMap;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
        initialize(2, null);
    }

    @Parameters(name = "shared:{0}, exactlyOnce:{1}, graceful:{2}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{
                // shared = false
                {false, false, false},
                {false, false, true},
                {false, true, false},
                {false, true, true},

                // shared = true - can't use exactly-once
                {true, false, false},
                {true, false, true},
        });
    }

    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.serializer", IntegerSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("max.block.ms", String.valueOf(KAFKA_MAX_BLOCK_MS));

        topic = randomName();
        kafkaTestSupport.createTopic(topic, PARTITION_COUNT);

        sourceIMap = instance().getMap(sourceIMapName);
        for (int i = 0; i < 20; i++) {
            sourceIMap.put(i, String.valueOf(i));
        }

        dataConnectionName = "kafka-data-connection-" + randomName();
        instance().getConfig().addDataConnectionConfig(
                new DataConnectionConfig(dataConnectionName)
                        .setShared(shared)
                        .setType("Kafka")
                        .setProperties(properties)
        );
    }

    @AfterClass
    public static void afterClass() {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
            kafkaTestSupport = null;
        }
    }

    @Test
    public void stressTest() {
        String topicLocal = topic;
        Sink<Integer> sink = KafkaSinks.<Integer>kafka(DataConnectionRef.dataConnectionRef(dataConnectionName))
                .toRecordFn(v -> new ProducerRecord<>(topicLocal, 0, null, v.toString()))
                .exactlyOnce(exactlyOnce)
                .build();

        try (KafkaConsumer<Integer, String> consumer = kafkaTestSupport.createConsumer(topic)) {
            List<Integer> actualSinkContents = new ArrayList<>();
            SinkStressTestUtil.test_withRestarts(instance(), logger, sink, graceful, exactlyOnce, () -> {
                for (ConsumerRecords<Integer, String> records;
                     !(records = consumer.poll(Duration.ofMillis(10))).isEmpty();
                ) {
                    for (ConsumerRecord<Integer, String> record : records) {
                        actualSinkContents.add(Integer.parseInt(record.value()));
                    }
                }
                return actualSinkContents;
            });
        }
    }
}
