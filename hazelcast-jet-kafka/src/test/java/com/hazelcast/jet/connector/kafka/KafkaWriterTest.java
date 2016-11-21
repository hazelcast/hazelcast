/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.connector.kafka;


import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.IMapReader;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class KafkaWriterTest extends HazelcastTestSupport {

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create(-1, -1,
            new Properties() {{
                put("num.partitions", "100");
            }}));
    private static String zkConnStr;
    private static int brokerPort;
    private static String brokerConnectionString;

    @BeforeClass
    public static void setUp() throws Exception {
        zkConnStr = kafkaRule.helper().zookeeperConnectionString();
        brokerPort = kafkaRule.helper().kafkaPort();
        brokerConnectionString = "localhost:" + brokerPort;
    }

    @Test
    public void testWriteToTopic() throws Exception {
        final String topic = randomName();
        final String producerGroup = "test";
        HazelcastInstance instance = createHazelcastInstance();
        InternalSerializationService serializationService = getSerializationService(instance);
        int messageCount = 20;
        Map<Integer, Integer> map = IntStream.range(0, messageCount).boxed().collect(Collectors.toMap(m -> m, m -> m));
        instance.getMap("producer").putAll(map);
        JetEngine jetEngine = JetEngine.get(instance, randomName());
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", IMapReader.supplier("producer"))
                .parallelism(1);

        Vertex consumer = new Vertex("consumer", KafkaWriter.supplier(zkConnStr, producerGroup, topic, brokerConnectionString))
                .parallelism(4);

        dag.addVertex(producer)
                .addVertex(consumer)
                .addEdge(new Edge(producer, consumer));

        Future<Void> future = jetEngine.newJob(dag).execute();
        assertCompletesEventually(future);

        KafkaConsumer<byte[], byte[]> byteConsumer = kafkaRule.helper().createByteConsumer();
        ListenableFuture<List<ConsumerRecord<byte[], byte[]>>> f = kafkaRule.helper().consume(topic, byteConsumer, messageCount);
        List<ConsumerRecord<byte[], byte[]>> consumerRecords = f.get();
        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            Object value = serializationService.toObject(new HeapData(record.value()));
            Assert.assertTrue(map.containsValue(value));
        }
    }

    public void send(InternalSerializationService ss, String topic, Object... values) {
        KafkaProducer<byte[], byte[]> byteProducer = kafkaRule.helper().createByteProducer();
        for (Object value : values) {
            byteProducer.send(new ProducerRecord<>(topic, ss.toBytes(value)));
        }
    }
}