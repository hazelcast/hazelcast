/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.connector.kafka.KafkaTestSupport;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class KafkaSinkTest extends KafkaTestSupport {

    private Properties properties;
    private String brokerConnectionString;

    @Before
    public void setup() throws IOException {
        brokerConnectionString = createKafkaCluster();
        properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerConnectionString);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
    }

    @Test
    public void testWriteToTopic() throws Exception {
        Map<String, String> map = range(0, 20)
                .mapToObj(Integer::toString)
                .collect(Collectors.toMap(m -> m, m -> m));

        JetInstance instance = createJetMember();
        IStreamMap<Object, Object> sourceMap = instance.getMap("source");
        sourceMap.putAll(map);

        final String topic = randomName();
        createTopic(topic, 1);

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(sourceMap.getName()))
         .drainTo(KafkaSinks.kafka(properties, topic));
        instance.newJob(p).join();

        try (KafkaConsumer<String, String> consumer = createConsumer(brokerConnectionString, topic)) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                assertTrue(map.containsValue(record.value()));
            }
        }
    }

    @Test
    public void testWriteToSpecificPartitions() throws Exception {
        int itemCount = 20;
        Map<String, String> map = range(0, itemCount)
                .mapToObj(Integer::toString)
                .collect(Collectors.toMap(m -> m, m -> m));

        JetInstance instance = createJetMember();
        IStreamMap<Object, Object> sourceMap = instance.getMap("source");
        sourceMap.putAll(map);

        final String topic = randomName();
        createTopic(topic, itemCount);

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String, String>map(sourceMap.getName()))
         .drainTo(KafkaSinks.kafka(properties, e ->
                 new ProducerRecord<>(topic, Integer.valueOf(e.getKey()), e.getKey(), e.getValue()))
         );
        instance.newJob(p).join();

        try (KafkaConsumer<String, String> consumer = createConsumer(brokerConnectionString, topic)) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                assertTrue(map.containsValue(record.value()));
            }
        }
    }
}
