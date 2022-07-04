/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.connector.SinkStressTestUtil;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteKafkaPTest extends SimpleTestInClusterSupport {

    private static final int PARTITION_COUNT = 20;

    private static KafkaTestSupport kafkaTestSupport;

    private String sourceIMapName = randomMapName();
    private Properties properties;
    private String topic;
    private IMap<Integer, String> sourceIMap;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
        initialize(2, null);
    }

    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.serializer", IntegerSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        topic = randomName();
        kafkaTestSupport.createTopic(topic, PARTITION_COUNT);

        sourceIMap = instance().getMap(sourceIMapName);
        for (int i = 0; i < 20; i++) {
            sourceIMap.put(i, String.valueOf(i));
        }
    }

    @AfterClass
    public static void afterClass() {
        if (kafkaTestSupport != null) {
            kafkaTestSupport.shutdownKafkaCluster();
            kafkaTestSupport = null;
        }
    }

    @Test
    public void testWriteToTopic() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(sourceIMap))
         .writeTo(KafkaSinks.kafka(properties, topic));
        instance().getJet().newJob(p).join();

        kafkaTestSupport.assertTopicContentsEventually(topic, sourceIMap, false);
    }

    @Test
    public void testWriteToSpecificPartitions() {
        String localTopic = topic;

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(sourceIMap))
         .writeTo(KafkaSinks.kafka(properties, e ->
                 new ProducerRecord<>(localTopic, e.getKey(), e.getKey(), e.getValue()))
         );
        instance().getJet().newJob(p).join();

        kafkaTestSupport.assertTopicContentsEventually(topic, sourceIMap, true);
    }

    @Test
    public void when_recordLingerEnabled_then_sentOnCompletion() {
        // When
        properties.setProperty("linger.ms", "3600000"); // 1 hour

        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Entry<String, String>>batchFromProcessor("source",
                ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
         .writeTo(KafkaSinks.kafka(properties, topic));

        Job job = instance().getJet().newJob(p);

        // the event should not appear in the topic due to linger.ms
        try (KafkaConsumer<Integer, String> consumer = kafkaTestSupport.createConsumer(topic)) {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(Duration.ofMillis(100)).count()), 2);
        }

        // Then
        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
        logger.info("Job finished");
        kafkaTestSupport.assertTopicContentsEventually(topic, singletonMap(0, "v"), false);
    }

    @Test
    public void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot_exactlyOnce() {
        when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(true);
    }

    @Test
    public void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot_atLeastOnce() {
        when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(false);
    }

    private void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(boolean exactlyOnce) {
        // When
        properties.setProperty("linger.ms", "" + HOURS.toMillis(1));

        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Entry<String, String>>batchFromProcessor("source",
                ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
         .writeTo(KafkaSinks.<Entry<String, String>>kafka(properties)
                            .topic(topic)
                            .extractKeyFn(Entry::getKey)
                            .extractValueFn(Entry::getValue)
                            .exactlyOnce(exactlyOnce)
                            .build());

        Job job = instance().getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(4000));

        // the event should not appear in the topic due to linger.ms
        try (KafkaConsumer<Integer, String> consumer = kafkaTestSupport.createConsumer(topic)) {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(Duration.ofMillis(100)).count()), 2);
        }

        // Then
        ProcessorWithEntryAndLatch.allowSnapshot = true;
        kafkaTestSupport.assertTopicContentsEventually(topic, singletonMap(0, "v"), false);

        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
    }

    @Test
    public void stressTest_graceful_exOnce() {
        stressTest(true, true);
    }

    @Test
    public void stressTest_forceful_exOnce() {
        stressTest(false, true);
    }

    @Test
    public void stressTest_graceful_atLeastOnce() {
        stressTest(false, false);
    }

    @Test
    public void stressTest_forceful_atLeastOnce() {
        stressTest(false, false);
    }

    private void stressTest(boolean graceful, boolean exactlyOnce) {
        String topicLocal = topic;
        Sink<Integer> sink = KafkaSinks.<Integer>kafka(properties)
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

    @Test
    public void test_resumeTransaction() throws Exception {
        properties.put("transactional.id", "txn.resumeTransactionTest");

        // produce items
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.initTransactions();
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topic, 0, null, "0")).get();
        producer.send(new ProducerRecord<>(topic, 0, null, "1")).get();
        long producerId = ResumeTransactionUtil.getProducerId(producer);
        short epoch = ResumeTransactionUtil.getEpoch(producer);

        // close the producer immediately to avoid aborting transaction
        producer.close(Duration.ZERO);

        // verify items are not visible
        KafkaConsumer<Integer, String> consumer = kafkaTestSupport.createConsumer(topic);
        ConsumerRecords<Integer, String> polledRecords = consumer.poll(Duration.ofSeconds(2));
        assertEquals(0, polledRecords.count());

        // recover and commit
        producer = new KafkaProducer<>(properties);
        ResumeTransactionUtil.resumeTransaction(producer, producerId, epoch);
        producer.commitTransaction();

        // verify items are visible
        StringBuilder actualContents = new StringBuilder();
        for (int receivedCount = 0; receivedCount < 2; ) {
            polledRecords = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<Integer, String> record : polledRecords) {
                actualContents.append(record.value()).append('\n');
                receivedCount++;
            }
            logger.info("Received " + receivedCount + " records so far");
        }
        assertEquals("0\n1\n", actualContents.toString());

        producer.close();
        consumer.close();
    }

    @Test
    public void when_transactionRolledBackHeuristically_then_sinkIgnoresIt() throws Exception {
        /*
        Design of the test:
        We'll create a processor, process 1 item and do phase-1 of the snapshot and then throw
        it away. Then we'll create a new processor and will try to restore the snapshot. It should
        try to commit the transaction from the previous processor, but that transaction timed out,
        which should be logged and ignored.
         */
        int txnTimeout = 2000;
        properties.setProperty("transaction.timeout.ms", String.valueOf(txnTimeout));
        Processor processor = WriteKafkaP.supplier(properties, o -> new ProducerRecord<>(topic, o), true).get();
        TestOutbox outbox = new TestOutbox(new int[0], 1024);
        TestProcessorContext procContext = new TestProcessorContext()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        processor.init(outbox, procContext);
        TestInbox inbox = new TestInbox();
        inbox.add("foo");
        processor.process(0, inbox);
        assertEquals("inbox size", 0, inbox.size());
        assertTrue(processor.saveToSnapshot());
        processor.close();

        inbox.addAll(outbox.snapshotQueue());

        // transaction.abort.timed.out.transaction.cleanup.interval.ms is set to 200, allow it to kick in
        sleepMillis(txnTimeout + 1000);

        // create the 2nd processor
        processor = WriteKafkaP.supplier(properties, o -> new ProducerRecord<>(topic, o), true).get();
        processor.init(outbox, procContext);
        processor.restoreFromSnapshot(inbox);
        processor.finishSnapshotRestore();
    }

    private static final class ProcessorWithEntryAndLatch extends AbstractProcessor {
        static volatile boolean isDone;
        static volatile boolean allowSnapshot;

        private Traverser<Entry<Integer, String>> t = Traversers.singleton(entry(0, "v"));

        private ProcessorWithEntryAndLatch() {
            // reset so that values from previous run don't remain
            isDone = false;
            allowSnapshot = false;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            return allowSnapshot;
        }

        @Override
        public boolean complete() {
            // emit the item and wait for the latch to complete
            return emitFromTraverser(t) && isDone;
        }
    }
}
