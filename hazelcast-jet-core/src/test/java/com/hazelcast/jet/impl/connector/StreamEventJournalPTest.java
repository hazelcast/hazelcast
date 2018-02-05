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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.WatermarkEmissionPolicy;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.core.test.TestSupport.SAME_ITEMS_ANY_ORDER;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class StreamEventJournalPTest extends JetTestSupport {

    private static final int NUM_PARTITIONS = 2;
    private static final int JOURNAL_CAPACITY = 10;

    private MapProxyImpl<Integer, Integer> map;
    private Supplier<Processor> supplier;

    @Before
    public void setUp() {
        JetConfig config = new JetConfig();

        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName("*")
                .setCapacity(JOURNAL_CAPACITY)
                .setEnabled(true);

        config.getHazelcastConfig().setProperty(PARTITION_COUNT.getName(), String.valueOf(NUM_PARTITIONS));
        config.getHazelcastConfig().addEventJournalConfig(journalConfig);
        JetInstance instance = this.createJetMember(config);

        map = (MapProxyImpl<Integer, Integer>) instance.getHazelcastInstance().<Integer, Integer>getMap("test");
        List<Integer> allPartitions = IntStream.range(0, NUM_PARTITIONS).boxed().collect(toList());

        supplier = () -> new StreamEventJournalP<>(map, allPartitions, e -> true,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, false,
                wmGenParams(Integer::intValue, withFixedLag(0), suppressAll(), -1));
    }


    private WatermarkEmissionPolicy suppressAll() {
        return (wm1, wm2) -> false;
    }

    @Test
    public void smokeTest() {
        for (int i = 0; i < 4; i++) {
            map.put(i, i);
        }

        TestSupport.verifyProcessor(supplier)
                   .disableProgressAssertion() // no progress assertion because of async calls
                   .disableRunUntilCompleted(1000) // processor would never complete otherwise
                   .outputChecker(SAME_ITEMS_ANY_ORDER) // ordering is only per partition
                   .expectOutput(Arrays.asList(0, 1, 2, 3));
    }

    @Test
    public void when_newData() {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        List<Object> actual = new ArrayList<>();
        Processor p = supplier.get();

        p.init(outbox, new TestProcessorContext());

        // putting JOURNAL_CAPACITY can overflow as capacity is per map and partitions
        // can be unbalanced.
        int batchSize = JOURNAL_CAPACITY / 2 + 1;
        int i;
        for (i = 0; i < batchSize; i++) {
            map.put(i, i);
        }
        // consume
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertEquals("consumed more items than expected", batchSize, actual.size());
            assertEquals(IntStream.range(0, batchSize).boxed().collect(Collectors.toSet()), new HashSet<>(actual));
        }, 3);

        for (; i < batchSize * 2; i++) {
            map.put(i, i);
        }

        // consume again
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertEquals("consumed more items than expected", JOURNAL_CAPACITY + 2, actual.size());
            assertEquals(IntStream.range(0, batchSize * 2).boxed().collect(Collectors.toSet()), new HashSet<>(actual));
        }, 3);
    }

    @Test
    public void when_staleSequence() {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        Processor p = supplier.get();
        p.init(outbox, new TestProcessorContext());

        // overflow journal
        for (int i = 0; i < JOURNAL_CAPACITY * 2; i++) {
            map.put(i, i);
        }

        // fill and consume
        List<Object> actual = new ArrayList<>();
        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, actual, true);
            assertTrue("consumed less items than expected", actual.size() >= JOURNAL_CAPACITY);
        }, 3);

        for (int i = 0; i < JOURNAL_CAPACITY; i++) {
            map.put(i, i);
        }
    }

    @Test
    public void when_staleSequence_afterRestore() {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        final Processor p = supplier.get();
        p.init(outbox, new TestProcessorContext());
        List<Object> output = new ArrayList<>();

        assertTrueEventually(() -> {
            assertFalse("Processor should never complete", p.complete());
            outbox.drainQueueAndReset(0, output, true);
            assertTrue("consumed more items than expected", output.size() == 0);
        }, 3);

        assertTrueEventually(() -> {
            assertTrue("Processor did not finish snapshot", p.saveToSnapshot());
        }, 3);

        // overflow journal
        for (int i = 0; i < JOURNAL_CAPACITY * 2; i++) {
            map.put(i, i);
        }

        List<Entry> snapshotItems = outbox.snapshotQueue().stream()
                  .map(e -> entry(e.getKey().getObject(), e.getValue().getObject()))
                  .collect(Collectors.toList());

        System.out.println("Restoring journal");
        // restore from snapshot
        assertRestore(snapshotItems);
    }

    private void assertRestore(List<Entry> snapshotItems) {
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
