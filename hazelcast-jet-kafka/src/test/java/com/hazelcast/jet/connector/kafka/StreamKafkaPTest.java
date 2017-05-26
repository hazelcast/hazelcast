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

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.processor.Sinks.writeList;
import static com.hazelcast.jet.connector.kafka.StreamKafkaP.streamKafka;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class StreamKafkaPTest extends KafkaTestSupport {

    @Test
    public void testReadTopic() throws Exception {
        String brokerConnectionString = createKafkaCluster();

        final String topic1 = randomString();
        final String topic2 = randomString();
        createTopic(topic1);
        createTopic(topic2);

        int messageCount = 20;
        JetInstance instance = createJetMember();
        DAG dag = new DAG();

        Properties properties = getProperties(brokerConnectionString, IntegerDeserializer.class, StringDeserializer.class);
        Vertex source = dag.newVertex("source",
                streamKafka(properties, topic1, topic2)).localParallelism(4);

        Vertex sink = dag.newVertex("sink", writeList("sink"))
                         .localParallelism(1);

        dag.edge(between(source, sink));

        Future<Void> jobFuture = instance.newJob(dag).execute();
        sleepAtLeastSeconds(3);
        range(0, messageCount).forEach(i -> {
            produce(topic1, i, Integer.toString(i));
            produce(topic2, i - messageCount, Integer.toString(i - messageCount));
        });
        IList<Object> list = instance.getList("sink");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(messageCount * 2, list.size());
                range(0, messageCount).forEach(i -> {
                    assertTrue(list.contains(createEntry(i)));
                    assertTrue(list.contains(createEntry(i - messageCount)));
                });
            }
        });

        assertFalse(jobFuture.isDone());

        // cancel the job
        jobFuture.cancel(true);
        assertTrueEventually(() -> assertTrue(jobFuture.isDone()));
    }

    private Properties getProperties(String brokerConnectionString, Class keyDeserializer, Class valueDeserializer) {
        Properties properties = new Properties();
        properties.setProperty("group.id", randomString());
        properties.setProperty("bootstrap.servers", brokerConnectionString);
        properties.setProperty("key.deserializer", keyDeserializer.getCanonicalName());
        properties.setProperty("value.deserializer", valueDeserializer.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

    private static Map.Entry<Integer, String> createEntry(int i) {
        return new SimpleImmutableEntry<>(i, Integer.toString(i));
    }
}
