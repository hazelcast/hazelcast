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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.kafka.KafkaTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
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
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class KafkaSinkTest extends KafkaTestSupport {

    private static final int ITEM_COUNT = 20;
    private static final String SOURCE_IMAP_NAME = "sourceMap";

    private Properties properties;
    private String brokerConnectionString;
    private JetInstance instance;
    private String topic;
    private IStreamMap<String, String> sourceIMap;

    @Before
    public void setup() throws IOException {
        brokerConnectionString = createKafkaCluster();
        properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerConnectionString);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        instance = createJetMember();
        topic = randomName();
        createTopic(topic, ITEM_COUNT);

        sourceIMap = instance.getMap(SOURCE_IMAP_NAME);
        for (int i = 0; i < 20; i++) {
            sourceIMap.put(String.valueOf(i), String.valueOf(i));
        }
    }

    @Test
    public void testWriteToTopic() throws Exception {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.map(SOURCE_IMAP_NAME))
         .drainTo(KafkaSinks.kafka(properties, topic));
        instance.newJob(p).join();

        assertTopicContentsEventually(sourceIMap, false);
    }

    @Test
    public void testWriteToSpecificPartitions() throws Exception {
        String localTopic = topic;

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String, String>map(SOURCE_IMAP_NAME))
         .drainTo(KafkaSinks.kafka(properties, e ->
                 new ProducerRecord<>(localTopic, Integer.valueOf(e.getKey()), e.getKey(), e.getValue()))
         );
        instance.newJob(p).join();

        assertTopicContentsEventually(sourceIMap, true);
    }

    @Test
    public void when_recordLingerEnabled_then_sentOnCompletion() throws Exception {
        // When
        properties.setProperty("linger.ms", "3600000"); // 1 hour

        // Given
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Entry<String, String>>batchFromProcessor("source",
                ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
         .drainTo(KafkaSinks.kafka(properties, topic));

        Job job = instance.newJob(p);

        // the event should not appear in the topic due to linger.ms
        try (KafkaConsumer<String, String> consumer = createConsumer(brokerConnectionString, topic)) {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(100).count()), 2);
        }

        // Then
        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
        System.out.println("Job finished");
        assertTopicContentsEventually(singletonMap("k", "v"), false);
    }

    @Test
    public void when_recordLingerEnabled_then_sentOnSnapshot() throws Exception {
        // When
        properties.setProperty("linger.ms", "3600000"); // 1 hour

        // Given
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Entry<String, String>>batchFromProcessor("source",
                ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
         .drainTo(KafkaSinks.kafka(properties, topic));

        Job job = instance.newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(4000));

        // the event should not appear in the topic due to linger.ms
        try (KafkaConsumer<String, String> consumer = createConsumer(brokerConnectionString, topic)) {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(100).count()), 2);
        }

        // Then
        ProcessorWithEntryAndLatch.allowSnapshot = true;
        assertTopicContentsEventually(singletonMap("k", "v"), false);

        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
    }

    private void assertTopicContentsEventually(Map<String, String> expectedMap, boolean assertPartitionEqualsKey) {
        try (KafkaConsumer<String, String> consumer = createConsumer(brokerConnectionString, topic)) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            for (int totalRecords = 0; totalRecords < expectedMap.size() && System.nanoTime() < timeLimit; ) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    assertEquals(expectedMap.get(record.key()), record.value());
                    if (assertPartitionEqualsKey) {
                        assertEquals(Integer.parseInt(record.key()), record.partition());
                    }
                    totalRecords++;
                }
            }
        }
    }

    private static final class ProcessorWithEntryAndLatch extends AbstractProcessor {
        static volatile boolean isDone;
        static volatile boolean allowSnapshot;

        private Traverser<Entry<String, String>> t = Traverser.over(entry("k", "v"));

        private ProcessorWithEntryAndLatch() {
            // reset so that values from previous run don't remain
            isDone = false;
            allowSnapshot = false;
            setCooperative(false);
        }

        @Override
        public boolean saveToSnapshot() {
            return allowSnapshot;
        }

        @Override
        public boolean complete() {
            // emit the item and wait for the latch to complete
            emitFromTraverser(t);
            return isDone;
        }
    }
}
