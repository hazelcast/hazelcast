/*
 * Copyright 2026 Hazelcast Inc.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.kafka.TopicsConfig;
import com.hazelcast.jet.kafka.TopicsConfig.TopicConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
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
import static com.hazelcast.jet.core.JetTestSupport.ditchJob;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastSeconds;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
@SuppressWarnings("NewClassNamingConvention")
public class StreamKafkaP_InitialOffsetsTest {

    private static final int INITIAL_PARTITION_COUNT = 4;
    private static final long LAG = 3;

    private static KafkaTestSupport kafkaTestSupport;

    private static TestHazelcastFactory factory;
    private static HazelcastInstance[] instances;

    private String topic1Name;
    private String topic2Name;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();

        factory = new TestHazelcastFactory();
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getJetConfig().setEnabled(true);
        instances = factory.newInstances(config, 2);
        waitAllForSafeState(instances);
    }

    @Before
    public void before() throws IOException {
        topic1Name = randomString();
        topic2Name = randomString();
        kafkaTestSupport.createTopic(topic1Name, INITIAL_PARTITION_COUNT);
        kafkaTestSupport.createTopic(topic2Name, INITIAL_PARTITION_COUNT);
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
        shutdownHz();
    }

    public static void shutdownHz() {
        List<Job> jobs = instances[0].getJet().getJobs();
        for (Job job : jobs) {
            ditchJob(job, instances);
        }
        factory.terminateAll();
        factory = null;
        instances = null;
    }

    private HazelcastInstance instance() {
        return instances[0];
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
                e instanceof Entry<?, ?> entry
                        ? (int) entry.getKey()
                        : currentTimeMillis();
        EventTimePolicy<T> eventTimePolicy = eventTimePolicy(
                timestampFn, limitingLag(LAG), 1, 0, idleTimeoutMillis);
        return new StreamKafkaP<>((c) -> new KafkaConsumer<>(properties), topicsConfig, projectionFn, eventTimePolicy);
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

    @SuppressWarnings("unchecked")
    private <T> T consumeEventually(Processor processor, TestOutbox outbox) {
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queue(0).isEmpty());
        });
        return (T) outbox.queue(0).poll();
    }

    public static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.deserializer", IntegerDeserializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}
