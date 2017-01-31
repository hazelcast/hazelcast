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

package com.hazelcast.jet.connector.kafka;


import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Edge.between;
import static java.util.stream.IntStream.range;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class KafkaWriterTest extends JetTestSupport {

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create(-1, -1,
            new Properties() {{
                put("num.partitions", "10");
                put("session.timeout.ms", "5000");
            }}));
    private static String zkConnStr;
    private static String brokerConnectionString;

    @BeforeClass
    public static void setUp() throws Exception {
        zkConnStr = kafkaRule.helper().zookeeperConnectionString();
        int brokerPort = kafkaRule.helper().kafkaPort();
        brokerConnectionString = "localhost:" + brokerPort;
    }

    @Test
    public void testWriteToTopic() throws Exception {
        final String topic = randomName();
        final String producerGroup = "test";
        JetInstance instance = createJetMember();

        int messageCount = 20;
        Map<String, String> map = range(0, messageCount)
                .mapToObj(Integer::toString)
                .collect(Collectors.toMap(m -> m, m -> m));

        instance.getMap("producer").putAll(map);
        DAG dag = new DAG();
        Vertex producer = dag.newVertex("producer", IMapReader.supplier("producer"))
                             .localParallelism(1);

        Function<String, byte[]> serializer = String::getBytes;
        Vertex consumer = dag.newVertex("consumer", KafkaWriter.supplier(zkConnStr,
                producerGroup, topic, brokerConnectionString, serializer, serializer))
                             .localParallelism(4);

        dag.edge(between(producer, consumer));

        Future<Void> future = instance.newJob(dag).execute();
        assertCompletesEventually(future);

        KafkaConsumer<byte[], byte[]> byteConsumer = kafkaRule.helper().createByteConsumer(new Properties() {{
            put("session.timeout.ms", "5000");
        }});
        ListenableFuture<List<ConsumerRecord<byte[], byte[]>>> f = kafkaRule.helper().consume(topic, byteConsumer, messageCount);
        List<ConsumerRecord<byte[], byte[]>> consumerRecords = f.get();
        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            Assert.assertTrue(map.containsValue(new String(record.value())));
        }
    }
}
