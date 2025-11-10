/*
 * Copyright 2025 Hazelcast Inc.
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

import com.hazelcast.collection.IList;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.kafka.TopicsConfig;
import com.hazelcast.jet.kafka.TopicsConfig.TopicConfig;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class StreamKafkaPTest extends SimpleTestInClusterSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;
    private static final long LAG = 3;

    private static KafkaTestSupport kafkaTestSupport;

    private String topic1Name;
    private String topic2Name;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
        initialize(2, null);
    }

    @Before
    public void before() {
        topic1Name = randomString();
        topic2Name = randomString();
        kafkaTestSupport.createTopic(topic1Name, INITIAL_PARTITION_COUNT);
        kafkaTestSupport.createTopic(topic2Name, INITIAL_PARTITION_COUNT);
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
        kafkaTestSupport = null;
    }

    // test for https://github.com/hazelcast/hazelcast/issues/21455
    @Test
    public void test_nonExistentTopic() {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(properties(), "nonExistentTopic"))
                .withoutTimestamps()
                .writeTo(Sinks.list("test_nonExistentTopic"));

        Job job = instance().getJet().newJob(p);

        assertThat(job).eventuallyHasStatus(RUNNING);
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 3);
    }

    @Test
    public void when_projectionFunctionProvided_then_appliedToReadRecords() {
        int messageCount = 20;
        String sinkListName = randomName();
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.<Integer, String, String>kafka(properties(), rec -> rec.value() + "-x", topic1Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkListName));

        instance().getJet().newJob(p);
        sleepAtLeastSeconds(3);
        for (int i = 0; i < messageCount; i++) {
            kafkaTestSupport.produce(topic1Name, i, Integer.toString(i));
        }
        IList<String> list = instance().getList(sinkListName);
        assertTrueEventually(() -> {
            assertEquals(messageCount, list.size());
            for (int i = 0; i < messageCount; i++) {
                String value = i + "-x";
                assertTrue("missing entry: " + value, list.contains(value));
            }
        });
    }

    @Test
    public void when_processingGuaranteeAtLeastOnce_then_readFromPartitionsInitialOffsets() throws Exception {
        testWithPartitionsInitialOffsets(AT_LEAST_ONCE);
    }

    @Test
    public void when_processingGuaranteeExactlyOnce_then_readFromPartitionsInitialOffsets() throws Exception {
        testWithPartitionsInitialOffsets(EXACTLY_ONCE);
    }

    private void testWithPartitionsInitialOffsets(ProcessingGuarantee guarantee) throws Exception {
        int expectedRecordsReadFromTopic1 = 80;
        int expectedRecordsReadFromTopic2 = 90;
        String sinkListName = randomName();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(kafkaTestSupport.produce(topic1Name, i, String.valueOf(i)));
            futures.add(kafkaTestSupport.produce(topic2Name, i, String.valueOf(i)));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        sleepAtLeastSeconds(3);

        TopicsConfig topicsConfig = new TopicsConfig()
                .addTopicConfig(new TopicConfig(topic1Name)
                        // 20 total records will be skipped from topic1
                        .addPartitionInitialOffset(0, 5L)
                        .addPartitionInitialOffset(1, 5L)
                        .addPartitionInitialOffset(2, 5L)
                        .addPartitionInitialOffset(3, 5L))
                .addTopicConfig(new TopicConfig(topic2Name)
                        // 10 total records will be skipped from topic2
                        .addPartitionInitialOffset(0, 5L)
                        .addPartitionInitialOffset(2, 5L));

        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.<Integer, String, Tuple2<String, String>>kafka(
                        properties(), r -> tuple2(r.value(), r.topic()), topicsConfig
                ))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkListName));

        instance().getJet().newJob(p, new JobConfig().setProcessingGuarantee(guarantee));
        sleepAtLeastSeconds(3);

        IList<Tuple2<String, String>> list = instance().getList(sinkListName);
        int totalRecordsRead = expectedRecordsReadFromTopic1 + expectedRecordsReadFromTopic2;
        assertTrueEventually(() -> assertEquals(totalRecordsRead, list.size()));

        // group retrieved records by topic and check if expected number of records were skipped
        Map<String, List<String>> recordsByTopic = list.stream()
                .collect(groupingBy(Tuple2::requiredF1, mapping(Tuple2::f0, toList())));

        assertThat(recordsByTopic.get(topic1Name).size())
                .isEqualTo(expectedRecordsReadFromTopic1);
        assertThat(recordsByTopic.get(topic2Name).size())
                .isEqualTo(expectedRecordsReadFromTopic2);
    }

    @Test
    public void when_processingGuaranteeAtLeastOnceAndJobResumedAfterSuspension_then_readFromPartitionsInitialOffsets() {
        testSuspendResumeWithPartitionInitialOffsets(10, AT_LEAST_ONCE);
    }

    @Test
    public void when_processingExactlyOnceAndJobResumedAfterSuspension_then_readFromPartitionsInitialOffsets() {
        testSuspendResumeWithPartitionInitialOffsets(20, EXACTLY_ONCE);
    }

    private void testSuspendResumeWithPartitionInitialOffsets(int recordsCount, ProcessingGuarantee processingGuarantee) {
        String sinkListName = randomName();

        // Send a batch of records to a single partition and wait for acks to ensure the partition offset is set correctly
        IntStream.range(0, recordsCount)
                 .mapToObj(i -> kafkaTestSupport.produce(topic1Name, 0, currentTimeMillis(), i, String.valueOf(i))).toList()
                 .forEach(future -> {
                     try {
                         future.get();
                     } catch (ExecutionException | InterruptedException e) {
                         throw new RuntimeException("Failed to insert initial records", e);
                     }
                 });

        // skip all records that exists in given kafka topic's partition before the job starts
        TopicsConfig topicsConfig = new TopicsConfig()
                .addTopicConfig(new TopicConfig(topic1Name)
                        .addPartitionInitialOffset(0, recordsCount));

        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.<Integer, String, Tuple2<String, String>>kafka(
                        properties(), r -> tuple2(r.value(), r.topic()), topicsConfig
                ))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkListName));

        Job job = instance().getJet().newJob(p, new JobConfig().setProcessingGuarantee(processingGuarantee));
        sleepAtLeastSeconds(3);

        // make sure nothing was consumed from the topic due to initialOffset
        assertTrueEventually(() -> assertEquals(0, instance().getList(sinkListName).size()), 5);
        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);

        job.resume();
        assertThat(job).eventuallyHasStatus(RUNNING);

        // produce another batch of records
        for (int i = recordsCount; i < 2 * recordsCount; i++) {
            kafkaTestSupport.produce(topic1Name, i, String.valueOf(i));
        }
        sleepAtLeastSeconds(3);

        // make sure only newly produced records were consumed from the topic
        assertTrueEventually(() -> assertEquals(recordsCount, instance().getList(sinkListName).size()), 5);
    }

    @Test
    public void when_atLeastOnce_then_continueFromLastReadMessageAfterJobRestart() {
        TopicsConfig topicsConfig = new TopicsConfig().addTopicConfig(new TopicConfig(topic1Name));
        int messageCount = 100;
        int expectedCountBeforeRestart = 100;

        // for processing guarantee different from NONE, when the job is restarted, consumption should be resumed
        // from the last successfully consumed message
        int expectedCountAfterRestart = 200;

        testWithJobRestart(messageCount, topicsConfig, AT_LEAST_ONCE,
                expectedCountBeforeRestart, expectedCountAfterRestart);
    }

    @Test
    public void when_atLeastOnceWithInitialOffsets_then_continueFromLastReadMessageAfterJobRestart() {
        TopicsConfig topicsConfig = new TopicsConfig()
                .addTopicConfig(new TopicConfig(topic1Name)
                        .addPartitionInitialOffset(0, 5L)
                        .addPartitionInitialOffset(1, 5L)
                        .addPartitionInitialOffset(2, 5L)
                        .addPartitionInitialOffset(3, 5L)
                );
        int messageCount = 100;

        // 20 messages will be skipped, because of initial offsets' configuration
        int expectedCountBeforeRestart = 80;

        // for processing guarantee different from NONE, when the job is restarted, consumption should be resumed
        // from the last successfully consumed message (i.e. initial offsets' configuration should be ignored while
        // restoring the job from snapshot)
        int expectedCountAfterRestart = 180;

        testWithJobRestart(messageCount, topicsConfig, AT_LEAST_ONCE,
                expectedCountBeforeRestart, expectedCountAfterRestart);
    }

    private void testWithJobRestart(
            int messageCount,
            TopicsConfig topicsConfig,
            ProcessingGuarantee processingGuarantee,
            int expectedCountBeforeRestart,
            int expectedCountAfterRestart
    ) {
        testWithJobRestart(messageCount, topicsConfig, processingGuarantee,
                expectedCountBeforeRestart, expectedCountAfterRestart, properties());
    }

    private void testWithJobRestart(
            int messageCount,
            TopicsConfig topicsConfig,
            ProcessingGuarantee processingGuarantee,
            int expectedCountBeforeRestart,
            int expectedCountAfterRestart,
            Properties kafkaProperties
    ) {
        String sinkListName = randomName();
        for (int i = 0; i < messageCount; i++) {
            kafkaTestSupport.produceSync(topic1Name, i, String.valueOf(i));
        }
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.<Integer, String, String>kafka(kafkaProperties, ConsumerRecord::value, topicsConfig))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkListName));

        Job job = instance().getJet().newJob(p, new JobConfig().setProcessingGuarantee(processingGuarantee));
        long oldExecutionId = assertThat(job).eventuallyJobRunning(instance(), null);
        assertTrueEventually(() -> assertEquals(expectedCountBeforeRestart, instance().getList(sinkListName).size()));

        job.restart();

        for (int i = messageCount; i < messageCount * 2; i++) {
            kafkaTestSupport.produceSync(topic1Name, i, String.valueOf(i));
        }
        assertThat(job).eventuallyJobRunning(instance(), oldExecutionId);
        assertTrueEventually(() -> assertEquals(expectedCountAfterRestart, instance().getList(sinkListName).size()));
    }

    @Test
    public void integrationTest_noSnapshotting() throws Exception {
        integrationTest(ProcessingGuarantee.NONE);
    }

    @Test
    public void integrationTest_withSnapshotting() throws Exception {
        integrationTest(EXACTLY_ONCE);
    }

    private void integrationTest(ProcessingGuarantee guarantee) throws Exception {
        int messageCount = 20;
        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createHazelcastInstance());
        String sinkListName = randomName();

        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(properties(), topic1Name, topic2Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkListName));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(guarantee);
        config.setSnapshotIntervalMillis(500);
        Job job = instances[0].getJet().newJob(p, config);
        sleepSeconds(3);
        for (int i = 0; i < messageCount; i++) {
            kafkaTestSupport.produceSync(topic1Name, i, Integer.toString(i));
            kafkaTestSupport.produceSync(topic2Name, i - messageCount, Integer.toString(i - messageCount));
        }
        IList<Object> list = instances[0].getList(sinkListName);

        assertTrueEventually(() -> {
            assertEquals(messageCount * 2, list.size());
            for (int i = 0; i < messageCount; i++) {
                Entry<Integer, String> entry1 = createEntry(i);
                Entry<Integer, String> entry2 = createEntry(i - messageCount);
                assertTrue("missing entry: " + entry1, list.contains(entry1));
                assertTrue("missing entry: " + entry2, list.contains(entry2));
            }
        });

        if (guarantee != ProcessingGuarantee.NONE) {
            // wait until a new snapshot appears
            JobRepository jr = new JobRepository(instances[0]);
            long currentMax = jr.getJobExecutionRecord(job.getId()).snapshotId();
            assertTrueEventually(() -> {
                JobExecutionRecord jobExecutionRecord = jr.getJobExecutionRecord(job.getId());
                assertNotNull("jobExecutionRecord == null", jobExecutionRecord);
                long newMax = jobExecutionRecord.snapshotId();
                assertTrue("no snapshot produced", newMax > currentMax);
                System.out.println("snapshot " + newMax + " found, previous was " + currentMax);
            });

            // Bring down one member. Job should restart and drain additional items (and maybe
            // some of the previous duplicately).
            instances[1].getLifecycleService().terminate();
            Thread.sleep(500);

            for (int i = messageCount; i < 2 * messageCount; i++) {
                kafkaTestSupport.produceSync(topic1Name, i, Integer.toString(i));
                kafkaTestSupport.produceSync(topic2Name, i - messageCount, Integer.toString(i - messageCount));
            }

            assertTrueEventually(() -> {
                assertTrue("Not all messages were received", list.size() >= messageCount * 4);
                for (int i = 0; i < 2 * messageCount; i++) {
                    Entry<Integer, String> entry1 = createEntry(i);
                    Entry<Integer, String> entry2 = createEntry(i - messageCount);
                    assertTrue("missing entry: " + entry1, list.contains(entry1));
                    assertTrue("missing entry: " + entry2, list.contains(entry2));
                }
            });
        }

        assertFalse(job.getFuture().isDone());

        // cancel the job
        job.cancel();
        assertTrueEventually(() -> assertTrue(job.getFuture().isDone()));
    }

    @Test
    public void when_eventsInAllPartitions_then_watermarkOutputImmediately() throws Exception {
        var processor = createProcessor(properties(), 1, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());

        for (int i = 0; i < INITIAL_PARTITION_COUNT; i++) {
            Entry<Integer, String> event = entry(i + 100, Integer.toString(i));
            System.out.println("produced event " + event);
            //Wait for the event to be published to Kafka, the processor can access Kafka metadata
            kafkaTestSupport.produce(topic1Name, i, null, event.getKey(), event.getValue()).get();
            if (i == INITIAL_PARTITION_COUNT - 1) {
                assertEquals(new Watermark(100 - LAG), consumeEventually(processor, outbox));
            }
            assertEquals(event, consumeEventually(processor, outbox));
        }
    }

    @Test
    public void when_noAssignedPartitionAndAddedLater_then_resumesFromIdle() throws Exception {
        // we ask to create 5th out of 5 processors, but we have only 4 partitions and 1 topic
        // --> our processor will have nothing assigned
        var processor = createProcessor(properties(), 1, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext()
                .setTotalParallelism(INITIAL_PARTITION_COUNT + 1)
                .setGlobalProcessorIndex(INITIAL_PARTITION_COUNT));

        assertTrue(processor.currentAssignment.isEmpty());
        assertEquals(IDLE_MESSAGE, consumeEventually(processor, outbox));

        // add a partition and produce an event to it
        kafkaTestSupport.setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 1);
        Entry<Integer, String> value = produceEventToNewPartition();

        Object actualEvent;
        do {
            actualEvent = consumeEventually(processor, outbox);
        } while (actualEvent instanceof Watermark);
        assertEquals(value, actualEvent);
    }

    @Test
    public void when_eventsInSinglePartition_then_watermarkAfterIdleTime() throws Exception {
        // When
        var processor = createProcessor(properties(), 2, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        kafkaTestSupport.produceSync(topic1Name, 10, "foo");

        // Then
        assertEquals(entry(10, "foo"), consumeEventually(processor, outbox));
        long time1 = System.nanoTime();
        assertEquals(new Watermark(10 - LAG), consumeEventually(processor, outbox));
        long time2 = System.nanoTime();
        long elapsedMs = NANOSECONDS.toMillis(time2 - time1);
        assertBetween("elapsed time", elapsedMs, 3000, 30_000);
    }

    @Test
    public void when_snapshotSaved_then_offsetsRestored() throws Exception {
        var processor = createProcessor(properties(), 2, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(EXACTLY_ONCE));

        kafkaTestSupport.produceSync(topic1Name, 0, "0");
        assertEquals(entry(0, "0"), consumeEventually(processor, outbox));

        // create snapshot
        TestInbox snapshot = saveSnapshot(processor, outbox);
        Set<Entry<Object, Object>> snapshotItems = unwrapBroadcastKey(snapshot.queue());

        // consume one more item
        kafkaTestSupport.produceSync(topic1Name, 1, "1");
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        // create new processor and restore snapshot
        processor = createProcessor(properties(), 2, r -> entry(r.key(), r.value()), 10_000);
        outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(EXACTLY_ONCE));

        // restore snapshot
        processor.restoreFromSnapshot(snapshot);
        assertTrue("snapshot not fully processed", snapshot.isEmpty());

        TestInbox snapshot2 = saveSnapshot(processor, outbox);
        assertEquals("new snapshot not equal after restore", snapshotItems, unwrapBroadcastKey(snapshot2.queue()));

        // the second item should be produced one more time
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        assertNoMoreItems(processor, outbox);
    }

    @Test
    public void when_duplicateTopicsProvide_then_uniqueTopicsSubscribed() throws Exception {
        HazelcastInstance[] instances = instances();
        assertClusterSizeEventually(2, instances);
        String sinkListName = randomName();

        // need new topic because we want 2 partitions only
        String topic = randomString();
        kafkaTestSupport.createTopic(topic, 2);

        Pipeline p = Pipeline.create();
        // Pass the same topic twice
        p.readFrom(KafkaSources.kafka(properties(), topic, topic))
                .withoutTimestamps()
                .setLocalParallelism(1)
                .writeTo(Sinks.list(sinkListName));

        JobConfig config = new JobConfig();
        Job job = instances[0].getJet().newJob(p, config);

        assertThat(job).eventuallyHasStatus(RUNNING);

        int messageCount = 1000;
        Future<?>[] futures = new Future[messageCount];
        for (int i = 0; i < messageCount; i++) {
            futures[i] = kafkaTestSupport.produce(topic, i, Integer.toString(i));
        }
        for (Future<?> future : futures) {
            future.get();
        }

        IList<Object> list = instances[0].getList(sinkListName);
        try {
            // Wait for all messages
            assertTrueEventually(() -> assertThat(list).hasSize(messageCount));
            // Check there are no more messages (duplicates..)
            assertTrueAllTheTime(() -> assertThat(list).hasSize(messageCount), 1);
        } finally {
            job.cancel();
        }
    }

    private <T> StreamKafkaP<Integer, String, T> createProcessor(
            Properties properties,
            int numTopics,
            @Nonnull FunctionEx<ConsumerRecord<Integer, String>, T> projectionFn,
            long idleTimeoutMillis
    ) {
        assert numTopics == 1 || numTopics == 2;
        List<String> topics = numTopics == 1
                ? singletonList(topic1Name)
                : asList(topic1Name, topic2Name);
        TopicsConfig topicsConfig = new TopicsConfig().addTopics(topics);
        return createProcessor(properties, topicsConfig, projectionFn, idleTimeoutMillis);
    }

    private <T> StreamKafkaP<Integer, String, T> createProcessor(
            Properties properties,
            TopicsConfig topicsConfig,
            @Nonnull FunctionEx<ConsumerRecord<Integer, String>, T> projectionFn,
            long idleTimeoutMillis
    ) {
        ToLongFunctionEx<T> timestampFn = e ->
                e instanceof Entry
                        ? (int) ((Entry<?, ?>) e).getKey()
                        : currentTimeMillis();
        EventTimePolicy<T> eventTimePolicy = eventTimePolicy(
                timestampFn, limitingLag(LAG), 1, 0, idleTimeoutMillis);
        return new StreamKafkaP<>((c) -> new KafkaConsumer<>(properties), topicsConfig, projectionFn, eventTimePolicy);
    }

    @Test
    public void when_partitionAdded_then_consumedFromBeginning() throws Exception {
        Properties properties = properties();
        properties.setProperty("metadata.max.age.ms", "100");
        var processor = createProcessor(properties, 2, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());

        kafkaTestSupport.produceSync(topic1Name, 0, "0");
        assertEquals(entry(0, "0"), consumeEventually(processor, outbox));

        kafkaTestSupport.setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 2);

        boolean somethingInPartition1 = false;
        for (int i = 1; i < 11; i++) {
            Future<RecordMetadata> future = kafkaTestSupport.produce(topic1Name, i, Integer.toString(i));
            RecordMetadata recordMetadata = future.get();
            System.out.println("Entry " + i + " produced to partition " + recordMetadata.partition());
            somethingInPartition1 |= recordMetadata.partition() == 1;
        }
        assertTrue("nothing was produced to partition-1", somethingInPartition1);
        Set<Object> receivedEvents = new HashSet<>();
        for (int i = 1; i < 11;) {
            try {
                Object consumed = consumeEventually(processor, outbox);
                if (!(consumed instanceof Watermark)) {
                    receivedEvents.add(consumed);
                    i++;
                }
            } catch (AssertionError e) {
                throw new AssertionError("Unable to receive 10 items, events so far: " + receivedEvents, e);
            }
        }
        var expected = range(1, 11).mapToObj(i -> entry(i, Integer.toString(i))).collect(toSet());
        assertThat(receivedEvents).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void when_partitionAddedWhilePartitionsInitialOffsetsProvided_then_consumedFromBeginning() throws Exception {
        Properties properties = properties();
        properties.setProperty("metadata.max.age.ms", "100");
        TopicsConfig topicsConfig = new TopicsConfig()
                .addTopic(topic2Name)
                .addTopicConfig(new TopicConfig(topic1Name)
                        .addPartitionInitialOffset(0, 1L)
                        .addPartitionInitialOffset(1, 1L)
                        .addPartitionInitialOffset(2, 1L)
                        .addPartitionInitialOffset(3, 1L)
                        // specify initial offset for non-existing partitions as well
                        .addPartitionInitialOffset(4, 1L)
                        .addPartitionInitialOffset(5, 1L)
                );

        StreamKafkaP<Integer, String, Entry<Integer, String>> processor = createProcessor(
                properties, topicsConfig, r -> entry(r.key(), r.value()), 60_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        TestProcessorContext context = new TestProcessorContext();
        context.setProcessingGuarantee(AT_LEAST_ONCE);
        processor.init(outbox, context);

        kafkaTestSupport.produceSync(topic1Name, 0, "0"); // first record will be skipped due to topics config
        kafkaTestSupport.produceSync(topic1Name, 1, "1");
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        kafkaTestSupport.setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 2);

        boolean somethingInPartition1 = false;
        for (int i = 2; i < 12; i++) {
            Future<RecordMetadata> future = kafkaTestSupport.produce(topic1Name, i, Integer.toString(i));
            RecordMetadata recordMetadata = future.get();
            System.out.println("## Entry " + i + " produced to partition " + recordMetadata.partition());
            somethingInPartition1 |= recordMetadata.partition() == 1;
        }
        assertTrue("nothing was produced to partition-1", somethingInPartition1);
        Set<Object> receivedEvents = new LinkedHashSet<>();
        for (int i = 2; i < 12;) {
            try {
                Object consumed = consumeEventually(processor, outbox);
                if (!(consumed instanceof Watermark)) {
                    receivedEvents.add(consumed);
                    i++;
                }
            } catch (AssertionError e) {
                throw new AssertionError("Unable to receive 10 items, events so far: " + receivedEvents, e);
            }
        }
        var expected = range(2, 12).mapToObj(i -> entry(i, Integer.toString(i))).collect(toSet());
        assertThat(receivedEvents).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void when_partitionAddedWhileJobDown_then_consumedFromBeginning() throws Exception {
        String sinkListName = randomName();
        IList<Entry<Integer, String>> sinkList = instance().getList(sinkListName);
        Pipeline p = Pipeline.create();
        Properties properties = properties();
        properties.setProperty("auto.offset.reset", "latest");
        p.readFrom(KafkaSources.<Integer, String>kafka(properties, topic1Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        Job job = instance().getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
        assertTrueEventually(() -> {
            // This might add multiple `0` events to the topic - we need to do this because the source starts from
            // the latest position and we don't exactly know when it starts, so we try repeatedly
            kafkaTestSupport.produce(topic1Name, 0, "0");
            assertFalse(sinkList.isEmpty());
            assertEquals(entry(0, "0"), sinkList.get(0));
        });
        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);
        // Note that the job might not have consumed all the zeroes from the topic at this point

        // When
        kafkaTestSupport.setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 2);
        // We produce to a partition that didn't exist during the previous job execution.
        // The job must start reading the new partition from the beginning, otherwise it would miss this item.
        Entry<Integer, String> event = produceEventToNewPartition();

        job.resume();
        // All events after the resume will be loaded: the non-consumed zeroes, and the possibly multiple
        // events added in produceEventToNewPartition(). But they must include the event added to the new partition.
        assertTrueEventually(() -> assertThat(sinkList).contains(event));
    }

    @Test
    public void when_autoOffsetResetLatest_then_doesNotReadOldMessages() {
        String sinkListName = randomName();
        IList<Entry<Integer, String>> sinkList = instance().getList(sinkListName);
        Pipeline p = Pipeline.create();
        Properties properties = properties();
        properties.setProperty("auto.offset.reset", "latest");
        p.readFrom(KafkaSources.<Integer, String>kafka(properties, topic1Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        kafkaTestSupport.produceSync(topic1Name, 0, "0");
        instance().getJet().newJob(p);
        assertTrueAllTheTime(() -> assertTrue(sinkList.isEmpty()), 2);
    }

    @Test
    public void when_autoOffsetResetEarliest_then_startsFromEarliestAfterRestart() throws Exception {
        String sinkListName = randomName();
        IList<Entry<Integer, String>> sinkList = instance().getList(sinkListName);
        Pipeline p = Pipeline.create();
        Properties properties = properties();
        properties.setProperty("auto.offset.reset", "earliest");
        p.readFrom(KafkaSources.<Integer, String>kafka(properties, topic1Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        kafkaTestSupport.produceSync(topic1Name, 0, "0");
        Job job = instance().getJet().newJob(p);
        assertTrueEventually(() -> assertEquals(singletonList(entry(0, "0")), sinkList));
        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);
        kafkaTestSupport.produce(topic1Name, 0, "1").get();
        sinkList.clear();
        job.resume();
        assertTrueEventually(() ->
                assertEquals(asList(entry(0, "0"), entry(0, "1")), new ArrayList<>(sinkList)));
    }

    @Test
    public void when_autoOffsetResetEarliestAndGroupIdSet_then_startsFromCommittedOffsetAfterRestart() throws Exception {
        String sinkListName = randomName();
        IList<Entry<Integer, String>> sinkList = instance().getList(sinkListName);
        Pipeline p = Pipeline.create();
        Properties properties = properties();
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", randomString());
        p.readFrom(KafkaSources.<Integer, String>kafka(properties, topic1Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        kafkaTestSupport.produceSync(topic1Name, 0, "0");
        Job job = instance().getJet().newJob(p);
        assertTrueEventually(() -> assertEquals(singletonList(entry(0, "0")), sinkList));
        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);
        kafkaTestSupport.produceSync(topic1Name, 0, "1");
        sinkList.clear();
        job.resume();
        assertTrueEventually(() ->
                assertEquals(singletonList(entry(0, "1")), new ArrayList<>(sinkList)));
    }

    @Test
    public void when_noAssignedPartitions_then_emitIdleMsgImmediately() throws Exception {
        var processor = createProcessor(properties(), 2, r -> entry(r.key(), r.value()), 100_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        TestProcessorContext context = new TestProcessorContext()
                // Set global parallelism to higher number than number of partitions
                .setTotalParallelism(INITIAL_PARTITION_COUNT * 2 + 1)
                .setGlobalProcessorIndex(INITIAL_PARTITION_COUNT * 2);

        processor.init(outbox, context);
        processor.complete();

        assertEquals(IDLE_MESSAGE, outbox.queue(0).poll());
    }

    @Test
    public void when_customProjection_then_used() throws Exception {
        // When
        var processor = createProcessor(properties(), 2, r -> r.key() + "=" + r.value(), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        kafkaTestSupport.produceSync(topic1Name, 0, "0");

        // Then
        assertEquals("0=0", consumeEventually(processor, outbox));
    }

    @Test
    public void when_customProjectionToNull_then_filteredOut() throws Exception {
        // When
        EventTimePolicy<String> eventTimePolicy = eventTimePolicy(
                Long::parseLong,
                limitingLag(0),
                1, 0,
                0
        );
        var processor = new StreamKafkaP<Integer, String, String>(
                (c) -> new KafkaConsumer<>(properties()),
                singletonList(topic1Name),
                r -> "0".equals(r.value()) ? null : r.value(),
                eventTimePolicy
        );
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        kafkaTestSupport.produceSync(topic1Name, 0, "0");
        kafkaTestSupport.produceSync(topic1Name, 0, "1");

        // Then
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queue(0).isEmpty());
        });
        assertEquals("1", outbox.queue(0).poll());
        assertNull(outbox.queue(0).poll());
    }

    @Test
    public void when_topicDoesNotExist_then_partitionCountGreaterThanZero() {
        try (var c = kafkaTestSupport.createConsumer("non-existing-topic")) {
            assertGreaterOrEquals("partition count", c.partitionsFor("non-existing-topic",
                    Duration.ofSeconds(2)).size(), 1);
        }
    }

    @Test
    public void when_consumerCannotConnect_then_partitionForTimeout() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", "127.0.0.1:33333");
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        try (var c = new KafkaConsumer<Integer, String>(properties)) {
            assertThatThrownBy(() -> c.partitionsFor("t", Duration.ofMillis(100)))
                    .isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    public void when_dataConnectionRef_then_readMessages() throws Exception {
        instance().getConfig().addDataConnectionConfig(
                new DataConnectionConfig("kafka-config")
                        .setType("Kafka")
                        .setShared(false) // shared would eagerly create a producer, which needs serializer properties
                        .setProperties(properties())
        );

        String sinkListName = randomName();
        IList<Entry<Integer, String>> sinkList = instance().getList(sinkListName);
        Pipeline p = Pipeline.create();
        Properties properties = properties();
        properties.setProperty("auto.offset.reset", "latest");

        p.readFrom(KafkaSources.<Integer, String>kafka(new DataConnectionRef("kafka-config"), topic1Name))
         .withoutTimestamps()
         .writeTo(Sinks.list(sinkList));

        kafkaTestSupport.produce(topic1Name, 0, "0").get();
        instance().getJet().newJob(p);
        assertTrueEventually(() -> assertThat(sinkList).contains(entry(0, "0")));
    }

    @SuppressWarnings("unchecked")
    private <T> T consumeEventually(Processor processor, TestOutbox outbox) {
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queue(0).isEmpty());
        });
        return (T) outbox.queue(0).poll();
    }

    private void assertNoMoreItems(StreamKafkaP<?, ?, ?> processor, TestOutbox outbox) throws InterruptedException {
        Thread.sleep(1000);
        assertFalse(processor.complete());
        assertTrue("unexpected items in outbox: " + outbox.queue(0), outbox.queue(0).isEmpty());
    }

    @SuppressWarnings("unchecked")
    private Set<Entry<Object, Object>> unwrapBroadcastKey(Collection<?> c) {
        // BroadcastKey("x") != BroadcastKey("x") ==> we need to extract the key
        Set<Entry<Object, Object>> res = new HashSet<>();
        for (Object o : c) {
            Entry<BroadcastKey<?>, ?> entry = (Entry<BroadcastKey<?>, ?>) o;
            Object equalsSafeValue = entry.getValue() instanceof long[]
                    ? Arrays.toString((long[]) entry.getValue())
                    : entry.getValue();
            res.add(entry(entry.getKey().key(), equalsSafeValue));
        }
        return res;
    }

    private TestInbox saveSnapshot(StreamKafkaP streamKafkaP, TestOutbox outbox) {
        TestInbox snapshot = new TestInbox();
        assertTrue(streamKafkaP.saveToSnapshot());
        outbox.drainSnapshotQueueAndReset(snapshot.queue(), false);
        return snapshot;
    }

    public static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.deserializer", IntegerDeserializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

    private static Map.Entry<Integer, String> createEntry(int i) {
        return new SimpleImmutableEntry<>(i, Integer.toString(i));
    }

    private Entry<Integer, String> produceEventToNewPartition() throws Exception {
        String value;
        while (true) {
            value = UuidUtil.newUnsecureUuidString();
            Future<RecordMetadata> future = kafkaTestSupport.produce(topic1Name, INITIAL_PARTITION_COUNT, null, 0, value);
            RecordMetadata recordMetadata = future.get();
            if (recordMetadata.partition() == INITIAL_PARTITION_COUNT) {
                // if the event was added to the correct partition, stop
                break;
            }
            sleepMillis(250);
        }
        return entry(0, value);
    }
}
