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
import com.hazelcast.core.IList;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.connector.IListWriter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class KafkaReaderTest extends JetTestSupport {

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
    public void testReadTopic() throws Exception {
        final String topic = randomName();
        int messageCount = 20;
        final String consumerGroupId = "test";
        JetInstance instance = createJetInstance();
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", KafkaReader.supplier(zkConnStr, consumerGroupId, topic, brokerConnectionString))
                .localParallelism(4);

        Vertex consumer = new Vertex("consumer", IListWriter.supplier("consumer"))
                .localParallelism(1);

        dag.addVertex(producer)
           .addVertex(consumer)
           .addEdge(new Edge(producer, consumer));

        instance.newJob(dag).execute();
        sleepAtLeastSeconds(3);
        List<Integer> numbers = IntStream.range(0, messageCount).boxed().collect(toList());
        send(getSerializationService(instance.getHazelcastInstance()), topic, numbers);
        IList<Object> list = instance.getList("consumer");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(messageCount, list.size());
                assertTrue(numbers.stream().allMatch(n -> list.contains(new AbstractMap.SimpleImmutableEntry<>(null, n))));
            }
        });
    }

    public void send(InternalSerializationService ss, String topic, List values) {
        KafkaProducer<byte[], byte[]> byteProducer = kafkaRule.helper().createByteProducer();
        for (Object value : values) {
            byteProducer.send(new ProducerRecord<>(topic, ss.toBytes(value)));
        }
    }
}
