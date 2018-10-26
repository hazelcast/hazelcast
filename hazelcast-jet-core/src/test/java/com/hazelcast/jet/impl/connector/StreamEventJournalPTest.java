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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.core.test.TestSupport.SAME_ITEMS_ANY_ORDER;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class StreamEventJournalPTest extends JetTestSupport {

    private static final int NUM_PARTITIONS = 2;
    private static final int CAPACITY_PER_PARTITION = 5;
    private static final int JOURNAL_CAPACITY = NUM_PARTITIONS * CAPACITY_PER_PARTITION;

    private MapProxyImpl<String, Integer> map;
    private DistributedSupplier<Processor> supplier;
    private JetInstance instance;
    private String key0;
    private String key1;

    @Before
    public void setUp() {
        JetConfig config = new JetConfig();

        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName("*")
                .setCapacity(JOURNAL_CAPACITY)
                .setEnabled(true);

        config.getHazelcastConfig().setProperty(PARTITION_COUNT.getName(), String.valueOf(NUM_PARTITIONS));
        config.getHazelcastConfig().addEventJournalConfig(journalConfig);
        instance = this.createJetMember(config);

        map = (MapProxyImpl<String, Integer>) instance.getHazelcastInstance().<String, Integer>getMap("test");

        List<Integer> allPartitions = IntStream.range(0, NUM_PARTITIONS).boxed().collect(toList());

        supplier = () -> new StreamEventJournalP<>(map, allPartitions, e -> true,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, false,
                eventTimePolicy(Integer::intValue, limitingLag(0), suppressAll(), -1));

        key0 = generateKeyForPartition(instance.getHazelcastInstance(), 0);
        key1 = generateKeyForPartition(instance.getHazelcastInstance(), 1);
    }

    private WatermarkEmissionPolicy suppressAll() {
        return (currentWm, lastEmittedWm) -> lastEmittedWm;
    }

    @Test
    public void smokeTest() {
        fillJournal(2);

        TestSupport.verifyProcessor(supplier)
                   .disableProgressAssertion() // no progress assertion because of async calls
                   .disableRunUntilCompleted(1000) // processor would never complete otherwise
                   .outputChecker(SAME_ITEMS_ANY_ORDER) // ordering is only per partition
                   .expectOutput(Arrays.asList(0, 1, 2, 3));
    }

    @Test
    public void when_newData() throws Exception {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        List<Object> actual = new ArrayList<>();
        Processor p = supplier.get();

        p.init(outbox, new TestProcessorContext());

        fillJournal(CAPACITY_PER_PARTITION);

        // consume
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertEquals("consumed different number of items than expected", JOURNAL_CAPACITY, actual.size());
            assertEquals(IntStream.range(0, JOURNAL_CAPACITY).boxed().collect(Collectors.toSet()), new HashSet<>(actual));
        }, 3);

        fillJournal(CAPACITY_PER_PARTITION);

        // consume again
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertEquals("consumed different number of items than expected", JOURNAL_CAPACITY + 2, actual.size());
            assertEquals(IntStream.range(0, JOURNAL_CAPACITY).boxed().collect(Collectors.toSet()), new HashSet<>(actual));
        }, 3);
    }

    @Test
    public void when_lostItems() throws Exception {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        Processor p = supplier.get();
        p.init(outbox, new TestProcessorContext());

        // overflow the journal
        fillJournal(CAPACITY_PER_PARTITION + 1);

        // fill and consume
        List<Object> actual = new ArrayList<>();
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertTrue("consumed different number of items than expected", actual.size() == JOURNAL_CAPACITY);
        }, 3);
    }

    @Test
    public void when_lostItems_afterRestore() throws Exception {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        final Processor p = supplier.get();
        p.init(outbox, new TestProcessorContext());
        List<Object> output = new ArrayList<>();

        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, output, true);
            assertTrue("consumed different number of items than expected", output.size() == 0);
        }, 3);

        assertTrueEventually(() -> {
            assertTrue("Processor did not finish snapshot", p.saveToSnapshot());
        }, 3);

        // overflow journal
        fillJournal(CAPACITY_PER_PARTITION + 1);

        List<Entry> snapshotItems = new ArrayList<>();
        outbox.drainSnapshotQueueAndReset(snapshotItems, false);

        logger.info("Restoring journal");
        // restore from snapshot
        assertRestore(snapshotItems);
    }

    @Test
    public void when_futureSequence_thenResetOffset() throws Exception {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        StreamEventJournalP p = (StreamEventJournalP) supplier.get();

        // fill journal so that it overflows
        fillJournal(CAPACITY_PER_PARTITION + 1);

        // initial offsets will be 5, since capacity per partition is 5
        p.init(outbox, new TestProcessorContext());

        // clear partitions before doing any read, but after initializing offsets
        map.destroy();

        // when we consume, we should not retrieve anything because we will ask for
        // offset 5, but current head is 0. This should not cause any error
        List<Object> actual = new ArrayList<>();

        // we should not receive any items, but the offset should be reset back to 0
        assertTrueFiveSeconds(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertTrue("consumed different number of items than expected", actual.size() == 0);
        });

        // add one item to each partition
        fillJournal(1);

        // receive the items we just added
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertTrue("consumed different number of items than expected", actual.size() == 2);
        });
    }

    @Test
    public void when_processorsWithNoPartitions_then_snapshotRestoreWorks() {
        DAG dag = new DAG();
        Vertex vertex = dag.newVertex("src",
                streamMapP(map.getName(), JournalInitialPosition.START_FROM_OLDEST, noEventTime()))
                           .localParallelism(8);
        int partitionCount = instance.getHazelcastInstance().getPartitionService().getPartitions().size();
        assertTrue("partition count should be lower than local parallelism",
                vertex.getLocalParallelism() > partitionCount);
        Job job = instance.newJob(dag, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(200_000));
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()), 25);
        job.restart();

        // Then
        // The job should be running: this test checks that state restored to NoopP, which is
        // created by the meta supplier for processor with no partitions, is ignored.
        sleepMillis(3000);
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()), 10);
    }

    private void fillJournal(int countPerPartition) {
        for (int i = 0; i < countPerPartition; i++) {
            map.put(key0, i * 2);
            map.put(key1, i * 2 + 1);
        }
    }

    private void assertRestore(List<Entry> snapshotItems) throws Exception {
        Processor p = supplier.get();
        TestOutbox newOutbox = new TestOutbox(new int[]{16}, 16);
        List<Object> output = new ArrayList<>();
        p.init(newOutbox, new TestProcessorContext());
        TestInbox inbox = new TestInbox();

        inbox.addAll(snapshotItems);
        p.restoreFromSnapshot(inbox);
        p.finishSnapshotRestore();

        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            newOutbox.drainQueueAndReset(0, output, true);
            assertEquals("consumed different number of items than expected", JOURNAL_CAPACITY, output.size());
        }, 3);
    }
}
