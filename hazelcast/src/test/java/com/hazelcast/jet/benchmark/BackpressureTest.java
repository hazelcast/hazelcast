/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.benchmark;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.partition.Partition;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class BackpressureTest extends JetTestSupport {

    private static final int CLUSTER_SIZE = 2;
    private static final int TOTAL_PARALLELISM = min(8, Runtime.getRuntime().availableProcessors());
    private static final int PARALLELISM_PER_MEMBER = TOTAL_PARALLELISM / CLUSTER_SIZE;
    private static final int KEY_COUNT = 1000;
    private static final int COUNT_PER_KEY_AND_SLICE = 10_000;

    private HazelcastInstance hz1;
    private HazelcastInstance hz2;

    @Before
    public void setUp() {
        Config config = defaultInstanceConfigWithJetEnabled();
        config.getJetConfig().setCooperativeThreadCount(PARALLELISM_PER_MEMBER);
        hz1 = createHazelcastInstance(config);
        hz2 = createHazelcastInstance(config);
    }

    @Test
    public void testBackpressure() {
        DAG dag = new DAG();

        final int member1Port = hz1.getCluster().getLocalMember().getAddress().getPort();
        final Member member2 = hz2.getCluster().getLocalMember();
        final int ptionOwnedByMember2 =
                hz1.getPartitionService()
                    .getPartitions().stream()
                    .filter(p -> p.getOwner().equals(member2))
                    .map(Partition::getPartitionId)
                    .findAny()
                    .orElseThrow(() -> new RuntimeException("Can't find a partition owned by member " + hz2));
        Vertex source = dag.newVertex("source", ProcessorMetaSupplier.of((Address address) ->
                ProcessorSupplier.of(address.getPort() == member1Port ? GenerateP::new : noopP())
        ));
        Vertex hiccup = dag.newVertex("hiccup", HiccupP::new);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP("counts"));

        dag.edge(between(source, hiccup)
                .distributed().partitioned(wholeItem(), (x, y) -> ptionOwnedByMember2))
           .edge(between(hiccup, sink));

        hz1.getJet().newJob(dag).join();
        assertCounts(hz1.getMap("counts"));
    }

    private static void assertCounts(Map<String, Long> wordCounts) {
        for (int i = 0; i < KEY_COUNT; i++) {
            Long count = wordCounts.get(Integer.toString(i));
            assertNotNull("Missing count for " + i, count);
            assertEquals("The count for key " + i + " is not correct",
                    COUNT_PER_KEY_AND_SLICE * PARALLELISM_PER_MEMBER, (long) count);
        }
    }

    private static class GenerateP extends AbstractProcessor {

        private int key;
        private int itemsEmittedPerKey;
        private final Traverser<Entry<String, Long>> trav = () -> {
            if (itemsEmittedPerKey == COUNT_PER_KEY_AND_SLICE) {
                return null;
            }
            try {
                return entry(Integer.toString(key), 1L);
            } finally {
                if (++key == KEY_COUNT) {
                    itemsEmittedPerKey++;
                    key = 0;
                }
            }
        };

        @Override
        public boolean complete() {
            return emitFromTraverser(trav);
        }

        @Override
        public String toString() {
            return "GenerateP";
        }
    }

    private static class CombineP extends AbstractProcessor {
        private Map<String, Long> counts = new HashMap<>();
        private Traverser<Entry<String, Long>> resultTraverser = lazy(() -> traverseIterable(counts.entrySet()));

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            @SuppressWarnings("unchecked")
            Entry<String, Long> entry = (Entry<String, Long>) item;
            counts.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : v + entry.getValue());
            return true;
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(resultTraverser);
        }

        @Override
        public String toString() {
            return "CombineP";
        }
    }

    private static class HiccupP extends CombineP {

        private long startTime;
        private boolean isHiccuping;
        private long nextDeadline;
        private int itemsProcessed;

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            super.init(context);
            long now = System.nanoTime();
            this.startTime = now;
            // Start in hiccup state, but with deadline set to immediately change to
            // running state
            isHiccuping = true;
            nextDeadline = now;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            updateHiccupStatus();
            if (isHiccuping) {
                return false;
            }
            itemsProcessed++;
            return super.tryProcess(ordinal, item);
        }

        private void updateHiccupStatus() {
            long now = System.nanoTime();
            if (now < nextDeadline) {
                return;
            }
            isHiccuping = !isHiccuping;
            long minTime = isHiccuping ? 301 : 700;
            long randomRange = isHiccuping ? 570 : 2_000;
            long prevDeadline = nextDeadline;
            nextDeadline = now + MILLISECONDS.toNanos(minTime)
                    + MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(randomRange));

            System.out.printf("==== %,7d %s for %,5d ms, late by %,d ms. Processed %,d items%n",
                    NANOSECONDS.toMillis(now - startTime),
                    isHiccuping ? "Hiccup" : "Resume",
                    NANOSECONDS.toMillis(nextDeadline - now),
                    NANOSECONDS.toMillis(now - prevDeadline),
                    itemsProcessed);
        }

        @Override
        public String toString() {
            return "HiccupP";
        }
    }
}
