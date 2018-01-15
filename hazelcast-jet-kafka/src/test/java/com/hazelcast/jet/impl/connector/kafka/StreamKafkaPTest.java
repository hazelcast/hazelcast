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

package com.hazelcast.jet.impl.connector.kafka;

import com.hazelcast.core.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestOutbox.MockData;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.SnapshotRepository;
import com.hazelcast.jet.impl.execution.SnapshotRecord;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class StreamKafkaPTest extends KafkaTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

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
        createTopic(topic1Name , INITIAL_PARTITION_COUNT);
        createTopic(topic2Name, INITIAL_PARTITION_COUNT);
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
        DAG dag = new DAG();

        Vertex source = dag.newVertex("source",
                streamKafkaP(properties, topic1Name, topic2Name)).localParallelism(4);

        Vertex sink = dag.newVertex("sink", writeListP("sink"))
                         .localParallelism(1);

        dag.edge(between(source, sink));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(guarantee);
        config.setSnapshotIntervalMillis(500);
        Job job = instances[0].newJob(dag, config);
        sleepAtLeastSeconds(3);
        for (int i = 0; i < messageCount; i++) {
            produce(topic1Name, i, Integer.toString(i));
            produce(topic2Name, i - messageCount, Integer.toString(i - messageCount));
        }
        IList<Object> list = instances[0].getList("sink");

        assertTrueEventually(() -> {
            assertEquals(messageCount * 2, list.size());
            for (int i = 0; i < messageCount; i++) {
                assertTrue(list.contains(createEntry(i)));
                assertTrue(list.contains(createEntry(i - messageCount)));
            }
        }, 5);

        if (guarantee != ProcessingGuarantee.NONE) {
            // wait until the items are consumed and a new snapshot appears
            assertTrueEventually(() -> assertTrue(list.size() == messageCount * 2));
            IStreamMap<Long, Object> snapshotsMap =
                    instances[0].getMap(SnapshotRepository.snapshotsMapName(job.getId()));
            Long currentMax = maxSuccessfulSnapshot(snapshotsMap);
            assertTrueEventually(() -> {
                Long newMax = maxSuccessfulSnapshot(snapshotsMap);
                assertTrue("no snapshot produced", newMax != null && !newMax.equals(currentMax));
                System.out.println("snapshot " + newMax + " found, previous was " + currentMax);
            });

            // Bring down one member. Job should restart and drain additional items (and maybe
            // some of the previous duplicately).
            instances[1].shutdown();
            Thread.sleep(500);

            for (int i = messageCount; i < 2 * messageCount; i++) {
                produce(topic1Name, i, Integer.toString(i));
                produce(topic2Name, i - messageCount, Integer.toString(i - messageCount));
            }

            assertTrueEventually(() -> {
                assertTrue("Not all messages were received", list.size() >= messageCount * 4);
                for (int i = 0; i < 2 * messageCount; i++) {
                    assertTrue(list.contains(createEntry(i)));
                    assertTrue(list.contains(createEntry(i - messageCount)));
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
    private Long maxSuccessfulSnapshot(IStreamMap<Long, Object> snapshotsMap) {
        return snapshotsMap.entrySet().stream()
                           .filter(e -> e.getValue() instanceof SnapshotRecord)
                           .map(e -> (SnapshotRecord) e.getValue())
                           .filter(SnapshotRecord::isSuccessful)
                           .map(SnapshotRecord::snapshotId)
                           .max(Comparator.naturalOrder())
                           .orElse(null);
    }

    @Test
    public void when_snapshotSaved_then_offsetsRestored() throws Exception {
        StreamKafkaP processor = new StreamKafkaP(properties, singletonList(topic1Name), Util::entry, 1, 60000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE));

        produce(topic1Name, 0, "0");
        assertEquals(entry(0, "0"), consumeEventually(processor, outbox));

        // create snapshot
        TestInbox snapshot = saveSnapshot(processor, outbox);
        Set snapshotItems = unwrapBroadcastKey(snapshot);

        // consume one more item
        produce(topic1Name, 1, "1");
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        // create new processor and restore snapshot
        processor = new StreamKafkaP(properties, asList(topic1Name, topic2Name), Util::entry, 1, 60000);
        outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE));

        // restore snapshot
        processor.restoreFromSnapshot(snapshot);
        assertTrue("snapshot not fully processed", snapshot.isEmpty());

        TestInbox snapshot2 = saveSnapshot(processor, outbox);
        assertEquals("new snapshot not equal after restore", snapshotItems, unwrapBroadcastKey(snapshot2));

        // the second item should be produced one more time
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        assertNoMoreItems(processor, outbox);
    }

    @Test
    public void when_partitionAdded_then_consumedFromBeginning() throws Exception {
        properties.setProperty("metadata.max.age.ms", "100");
        StreamKafkaP processor = new StreamKafkaP(properties, singletonList(topic1Name), Util::entry, 1, 100);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE));

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
    public void when_notEnoughPartitions_thenFail() throws Exception {
        // Set global parallelism to higher number than number of partitions
        StreamKafkaP processor = new StreamKafkaP<>(properties, Arrays.asList(topic1Name, topic2Name), Util::entry,
                INITIAL_PARTITION_COUNT * 2 + 1, 500);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        TestProcessorContext context = new TestProcessorContext()
                .setGlobalProcessorIndex(1)
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);

        expectedException.expectMessage("global parallelism");
        processor.init(outbox, context);
    }

    @Test
    public void when_customProjection_then_used() {
        // When
        StreamKafkaP processor = new StreamKafkaP(properties, singletonList(topic1Name), (k, v) -> k + "=" + v, 1, 500);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        produce(topic1Name, 0, "0");

        // Then
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queueWithOrdinal(0).isEmpty());
        }, 3);
        assertEquals("0=0", outbox.queueWithOrdinal(0).poll());
    }

    @Test
    public void when_customProjectionToNull_then_filteredOut() {
        // When
        StreamKafkaP processor = new StreamKafkaP(properties, singletonList(topic1Name),
                (k, v) -> "0".equals(v) ? null : v, 1, 500);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        produce(topic1Name, 0, "0");
        produce(topic1Name, 0, "1");

        // Then
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queueWithOrdinal(0).isEmpty());
        }, 3);
        assertEquals("1", outbox.queueWithOrdinal(0).poll());
        assertNull(outbox.queueWithOrdinal(0).poll());
    }

    private Entry<Integer, String> consumeEventually(Processor processor, TestOutbox outbox) {
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queueWithOrdinal(0).isEmpty());
        }, 3);
        return (Entry<Integer, String>) outbox.queueWithOrdinal(0).poll();
    }

    private void assertNoMoreItems(StreamKafkaP processor, TestOutbox outbox) throws InterruptedException {
        Thread.sleep(1000);
        assertFalse(processor.complete());
        assertTrue("unexpected items in outbox: " + outbox.queueWithOrdinal(0), outbox.queueWithOrdinal(0).isEmpty());
    }

    private Set unwrapBroadcastKey(Collection c) {
        // BroadcastKey("x") != BroadcastKey("x") ==> we need to extract the key
        Set res = new HashSet();
        for (Object o : c) {
            Entry<BroadcastKey<TopicPartition>, Long> entry = (Entry<BroadcastKey<TopicPartition>, Long>) o;
            res.add(entry(entry.getKey().key(), entry.getValue()));
        }
        return res;
    }

    private TestInbox saveSnapshot(StreamKafkaP streamKafkaP, TestOutbox outbox) {
        TestInbox snapshot = new TestInbox();
        assertTrue(streamKafkaP.saveToSnapshot());
        TestSupport.drainOutbox(outbox.snapshotQueue(), snapshot, false);
        snapshot = snapshot.stream().map(e -> (Entry<MockData, MockData>) e)
                           .map(e -> entry(e.getKey().getObject(), e.getValue().getObject()))
                           .collect(toCollection(TestInbox::new));
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
