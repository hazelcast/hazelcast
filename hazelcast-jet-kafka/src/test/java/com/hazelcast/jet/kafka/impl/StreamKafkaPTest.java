/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.core.IList;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.noThrottling;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class StreamKafkaPTest extends KafkaTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;
    private static final long LAG = 3;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Properties properties;
    private String topic1Name;
    private String topic2Name;

    @Before
    public void before() throws Exception {
        String brokerConnectionString = createKafkaCluster();
        properties = getProperties(brokerConnectionString, IntegerDeserializer.class, StringDeserializer.class);

        topic1Name = randomString();
        topic2Name = randomString();
        createTopic(topic1Name, INITIAL_PARTITION_COUNT);
        createTopic(topic2Name, INITIAL_PARTITION_COUNT);
    }

    @Test
    public void when_projectionFunctionProvided_thenAppliedToReadRecords() {
        int messageCount = 20;
        JetInstance[] instances = new JetInstance[2];
        Arrays.setAll(instances, i -> createJetMember());

        Pipeline p = Pipeline.create();
        p.drawFrom(KafkaSources.<Integer, String, String>kafka(properties, rec -> rec.value() + "-x", topic1Name))
         .drainTo(Sinks.list("sink"));

        instances[0].newJob(p);
        sleepAtLeastSeconds(3);
        for (int i = 0; i < messageCount; i++) {
            produce(topic1Name, i, Integer.toString(i));
        }
        IList<String> list = instances[0].getList("sink");
        assertTrueEventually(() -> {
            assertEquals(messageCount, list.size());
            for (int i = 0; i < messageCount; i++) {
                String value = Integer.toString(i) + "-x";
                assertTrue("missing entry: " + value, list.contains(value));
            }
        }, 5);

    }

    @Test
    public void integrationTest_noSnapshotting() throws Exception {
        integrationTest(ProcessingGuarantee.NONE);
    }

    @Test
    public void integrationTest_withSnapshotting() throws Exception {
        integrationTest(ProcessingGuarantee.EXACTLY_ONCE);
    }

    private void integrationTest(ProcessingGuarantee guarantee) throws Exception {
        int messageCount = 20;
        JetInstance[] instances = new JetInstance[2];
        Arrays.setAll(instances, i -> createJetMember());

        Pipeline p = Pipeline.create();
        p.drawFrom(KafkaSources.kafka(properties, topic1Name, topic2Name))
         .drainTo(Sinks.list("sink"));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(guarantee);
        config.setSnapshotIntervalMillis(500);
        Job job = instances[0].newJob(p, config);
        sleepAtLeastSeconds(3);
        for (int i = 0; i < messageCount; i++) {
            produce(topic1Name, i, Integer.toString(i));
            produce(topic2Name, i - messageCount, Integer.toString(i - messageCount));
        }
        IList<Object> list = instances[0].getList("sink");

        assertTrueEventually(() -> {
            assertEquals(messageCount * 2, list.size());
            for (int i = 0; i < messageCount; i++) {
                Entry<Integer, String> entry1 = createEntry(i);
                Entry<Integer, String> entry2 = createEntry(i - messageCount);
                assertTrue("missing entry: " + entry1, list.contains(entry1));
                assertTrue("missing entry: " + entry2, list.contains(entry2));
            }
        }, 5);

        if (guarantee != ProcessingGuarantee.NONE) {
            // wait until the items are consumed and a new snapshot appears
            assertTrueEventually(() -> assertEquals(list.size(), messageCount * 2));
            IMapJet<Long, Object> snapshotsMap =
                    instances[0].getMap(SnapshotRepository.snapshotsMapName(job.getId()));
            Long currentMax = maxSuccessfulSnapshot(snapshotsMap);
            assertTrueEventually(() -> {
                Long newMax = maxSuccessfulSnapshot(snapshotsMap);
                assertTrue("no snapshot produced", newMax != null && !newMax.equals(currentMax));
                System.out.println("snapshot " + newMax + " found, previous was " + currentMax);
            });

            // Bring down one member. Job should restart and drain additional items (and maybe
            // some of the previous duplicately).
            instances[1].getHazelcastInstance().getLifecycleService().terminate();
            Thread.sleep(500);

            for (int i = messageCount; i < 2 * messageCount; i++) {
                produce(topic1Name, i, Integer.toString(i));
                produce(topic2Name, i - messageCount, Integer.toString(i - messageCount));
            }

            assertTrueEventually(() -> {
                assertTrue("Not all messages were received", list.size() >= messageCount * 4);
                for (int i = 0; i < 2 * messageCount; i++) {
                    Entry<Integer, String> entry1 = createEntry(i);
                    Entry<Integer, String> entry2 = createEntry(i - messageCount);
                    assertTrue("missing entry: " + entry1.toString(), list.contains(entry1));
                    assertTrue("missing entry: " + entry2.toString(), list.contains(entry2));
                }
            }, 10);
        }

        assertFalse(job.getFuture().isDone());

        // cancel the job
        job.cancel();
        assertTrueEventually(() -> assertTrue(job.getFuture().isDone()));
    }

    /**
     * @return maximum ID of successful snapshot or null, if there is no successful snapshot.
     */
    private Long maxSuccessfulSnapshot(IMapJet<Long, Object> snapshotsMap) {
        return snapshotsMap.entrySet().stream()
                           .filter(e -> e.getValue() instanceof SnapshotRecord)
                           .map(e -> (SnapshotRecord) e.getValue())
                           .filter(SnapshotRecord::isSuccessful)
                           .map(SnapshotRecord::snapshotId)
                           .max(Comparator.naturalOrder())
                           .orElse(null);
    }

    @Test
    public void when_eventsInAllPartitions_then_watermarkOutputImmediately() {
        StreamKafkaP processor = createProcessor(1, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());

        for (int i = 0; i < INITIAL_PARTITION_COUNT; i++) {
            Entry<Integer, String> event = entry(i + 100, Integer.toString(i));
            System.out.println("produced event " + event);
            produce(topic1Name, i, event.getKey(), event.getValue());
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
        StreamKafkaP processor = createProcessor(1, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext()
                .setTotalParallelism(INITIAL_PARTITION_COUNT + 1)
                .setGlobalProcessorIndex(INITIAL_PARTITION_COUNT));

        assertTrue(processor.currentAssignment.isEmpty());
        assertEquals(IDLE_MESSAGE, consumeEventually(processor, outbox));

        setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 1);
        Thread.sleep(1000);
        resetProducer(); // this allows production to the added partition

        // produce events until the event happens to go to the added partition
        Entry<Integer, String> event;
        for (int i = 0; ; i++) {
            event = entry(i, Integer.toString(i));
            Future<RecordMetadata> future = produce(topic1Name, event.getKey(), event.getValue());
            RecordMetadata recordMetadata = future.get();
            if (recordMetadata.partition() == 4) {
                break;
            }
        }

        assertEquals(new Watermark(event.getKey() - LAG), consumeEventually(processor, outbox));
        assertEquals(event, consumeEventually(processor, outbox));
    }

    @Test
    public void when_eventsInSinglePartition_then_watermarkAfterIdleTime() {
        // When
        StreamKafkaP processor = createProcessor(2, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        produce(topic1Name, 10, "foo");

        // Then
        assertEquals(entry(10, "foo"), consumeEventually(processor, outbox));
        long time1 = System.nanoTime();
        assertEquals(new Watermark(10 - LAG), consumeEventually(processor, outbox));
        long time2 = System.nanoTime();
        long elapsedMs = NANOSECONDS.toMillis(time2 - time1);
        assertTrue("elapsed time: " + elapsedMs + " ms, should be larger", elapsedMs > 3000 && elapsedMs <= 10_000);
    }

    @Test
    public void when_snapshotSaved_then_offsetsRestored() throws Exception {
        StreamKafkaP processor = createProcessor(2, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE));

        produce(topic1Name, 0, "0");
        assertEquals(entry(0, "0"), consumeEventually(processor, outbox));

        // create snapshot
        TestInbox snapshot = saveSnapshot(processor, outbox);
        Set snapshotItems = unwrapBroadcastKey(snapshot.queue());

        // consume one more item
        produce(topic1Name, 1, "1");
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        // create new processor and restore snapshot
        processor = createProcessor(2, r -> entry(r.key(), r.value()), 10_000);
        outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE));

        // restore snapshot
        processor.restoreFromSnapshot(snapshot);
        assertTrue("snapshot not fully processed", snapshot.isEmpty());

        TestInbox snapshot2 = saveSnapshot(processor, outbox);
        assertEquals("new snapshot not equal after restore", snapshotItems, unwrapBroadcastKey(snapshot2.queue()));

        // the second item should be produced one more time
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        assertNoMoreItems(processor, outbox);
    }

    private <T> StreamKafkaP<Integer, String, T> createProcessor(
            int numTopics,
            @Nonnull DistributedFunction<ConsumerRecord<Integer, String>, T> projectionFn,
            long idleTimeoutMillis
    ) {
        assert numTopics == 1 || numTopics == 2;
        DistributedToLongFunction<T> timestampFn = e ->
                e instanceof Entry ?
                        (int) ((Entry) e).getKey()
                        :
                        System.currentTimeMillis();
        EventTimePolicy<T> eventTimePolicy = eventTimePolicy(
                timestampFn, limitingLag(LAG), noThrottling(), idleTimeoutMillis);
        List<String> topics = numTopics == 1 ?
                singletonList(topic1Name)
                :
                asList(topic1Name, topic2Name);
        return new StreamKafkaP<>(properties, topics, projectionFn, eventTimePolicy);
    }

    @Test
    public void when_partitionAdded_then_consumedFromBeginning() throws Exception {
        properties.setProperty("metadata.max.age.ms", "100");
        StreamKafkaP processor = createProcessor(2, r -> entry(r.key(), r.value()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());

        produce(topic1Name, 0, "0");
        assertEquals(entry(0, "0"), consumeEventually(processor, outbox));

        setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 2);
        Thread.sleep(1000);
        resetProducer(); // this allows production to the added partition

        boolean somethingInPartition1 = false;
        for (int i = 1; i < 11; i++) {
            Future<RecordMetadata> future = produce(topic1Name, i, Integer.toString(i));
            RecordMetadata recordMetadata = future.get();
            System.out.println("Entry " + i + " produced to partition " + recordMetadata.partition());
            somethingInPartition1 |= recordMetadata.partition() == 1;
        }
        assertTrue("nothing was produced to partition-1", somethingInPartition1);
        Set receivedEvents = new HashSet();
        for (int i = 1; i < 11; i++) {
            try {
                receivedEvents.add(consumeEventually(processor, outbox));
            } catch (AssertionError e) {
                throw new AssertionError("Unable to receive 10 items, events so far: " + receivedEvents);
            }
        }
        assertEquals(range(1, 11).mapToObj(i -> entry(i, Integer.toString(i))).collect(toSet()), receivedEvents);
    }

    @Test
    public void when_noAssignedPartitions_thenEmitIdleMsgImmediately() {
        StreamKafkaP processor = createProcessor(2, r -> entry(r.key(), r.value()), 100_000);
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
    public void when_customProjection_then_used() {
        // When
        StreamKafkaP processor = createProcessor(2, r -> r.key() + "=" + r.value(), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        produce(topic1Name, 0, "0");

        // Then
        assertEquals("0=0", consumeEventually(processor, outbox));
    }

    @Test
    public void when_customProjectionToNull_then_filteredOut() {
        // When
        EventTimePolicy<String> eventTimePolicy = eventTimePolicy(
                Long::parseLong,
                limitingLag(0),
                noThrottling(),
                0
        );
        StreamKafkaP processor = new StreamKafkaP<Integer, String, String>(
                properties, singletonList(topic1Name), r -> "0".equals(r.value()) ? null : r.value(), eventTimePolicy
        );
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        produce(topic1Name, 0, "0");
        produce(topic1Name, 0, "1");

        // Then
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queue(0).isEmpty());
        }, 3);
        assertEquals("1", outbox.queue(0).poll());
        assertNull(outbox.queue(0).poll());
    }

    private <T> T consumeEventually(Processor processor, TestOutbox outbox) {
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queue(0).isEmpty());
        }, 12);
        return (T) outbox.queue(0).poll();
    }

    private void assertNoMoreItems(StreamKafkaP processor, TestOutbox outbox) throws InterruptedException {
        Thread.sleep(1000);
        assertFalse(processor.complete());
        assertTrue("unexpected items in outbox: " + outbox.queue(0), outbox.queue(0).isEmpty());
    }

    private Set<Entry<TopicPartition, String>> unwrapBroadcastKey(Collection c) {
        // BroadcastKey("x") != BroadcastKey("x") ==> we need to extract the key
        Set<Entry<TopicPartition, String>> res = new HashSet<>();
        for (Object o : c) {
            Entry<BroadcastKey<TopicPartition>, long[]> entry = (Entry<BroadcastKey<TopicPartition>, long[]>) o;
            res.add(entry(entry.getKey().key(), Arrays.toString(entry.getValue())));
        }
        return res;
    }

    private TestInbox saveSnapshot(StreamKafkaP streamKafkaP, TestOutbox outbox) {
        TestInbox snapshot = new TestInbox();
        assertTrue(streamKafkaP.saveToSnapshot());
        outbox.drainSnapshotQueueAndReset(snapshot.queue(), false);
        return snapshot;
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
