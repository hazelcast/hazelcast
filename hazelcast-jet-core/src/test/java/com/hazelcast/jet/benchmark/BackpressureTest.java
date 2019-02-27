/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.nio.Address;
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

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(NightlyTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class BackpressureTest extends JetTestSupport {

    private static final int CLUSTER_SIZE = 2;
    private static final int TOTAL_PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static final int PARALLELISM_PER_MEMBER = TOTAL_PARALLELISM / CLUSTER_SIZE;
    private static final int DISTINCT = 1000;
    private static final int COUNT_PER_DISTINCT_AND_SLICE = 10_000;

    private JetInstance jet1;
    private JetInstance jet2;

    @Before
    public void setUp() {
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(PARALLELISM_PER_MEMBER);
        jet1 = createJetMember(config);
        jet2 = createJetMember(config);
    }

    @Test
    public void testBackpressure() {
        DAG dag = new DAG();

        final int member1Port = jet1.getCluster().getLocalMember().getAddress().getPort();
        final Member member2 = jet2.getCluster().getLocalMember();
        final int ptionOwnedByMember2 =
                jet1.getHazelcastInstance().getPartitionService()
                    .getPartitions().stream()
                    .filter(p -> p.getOwner().equals(member2))
                    .map(Partition::getPartitionId)
                    .findAny()
                    .orElseThrow(() -> new RuntimeException("Can't find a partition owned by member " + jet2));
        Vertex source = dag.newVertex("source", ProcessorMetaSupplier.of((Address address) ->
                ProcessorSupplier.of(address.getPort() == member1Port ? GenerateP::new : noopP())
        ));
        Vertex hiccup = dag.newVertex("hiccup", HiccupP::new);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP("counts"));

        dag.edge(between(source, hiccup)
                .distributed().partitioned(wholeItem(), (x, y) -> ptionOwnedByMember2))
           .edge(between(hiccup, sink));

        jet1.newJob(dag).join();
        assertCounts(jet1.getMap("counts"));
    }

    private static void assertCounts(Map<String, Long> wordCounts) {
        for (int i = 0; i < DISTINCT; i++) {
            Long count = wordCounts.get(Integer.toString(i));
            assertNotNull("Missing count for " + i, count);
            assertEquals("The count for " + i + " is not correct",
                    COUNT_PER_DISTINCT_AND_SLICE * PARALLELISM_PER_MEMBER, (long) count);
        }
    }

    private static class GenerateP extends AbstractProcessor {

        private int item;
        private int count;
        private final Traverser<Entry<String, Long>> trav = () -> {
            if (count == COUNT_PER_DISTINCT_AND_SLICE) {
                return null;
            }
            try {
                return entry(Integer.toString(item), 1L);
            } finally {
                if (++item == DISTINCT) {
                    count++;
                    item = 0;
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
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
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

        private long hiccupDeadline;
        private long nextHiccupTime;

        HiccupP() {
            updateNextHiccupTime();
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            if (isHiccuping()) {
                return false;
            }
            return super.tryProcess(ordinal, item);
        }

        private boolean isHiccuping() {
            if (hiccupDeadline != 0) {
                if (System.nanoTime() < hiccupDeadline) {
                    return true;
                }
                System.out.println("==== Resume");
                hiccupDeadline = 0;
                updateNextHiccupTime();
            }
            if (System.nanoTime() >= nextHiccupTime) {
                final long hiccupDuration = MILLISECONDS.toNanos(301)
                        + ThreadLocalRandom.current().nextLong(MILLISECONDS.toNanos(570));
                hiccupDeadline = System.nanoTime() + hiccupDuration;
                System.out.println("==== Hiccup " + NANOSECONDS.toMillis(hiccupDuration) + " ms");
            }
            return false;
        }

        private void updateNextHiccupTime() {
            nextHiccupTime = System.nanoTime() + MILLISECONDS.toNanos(700)
                    + MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(2_000));
        }

        @Override
        public String toString() {
            return "HiccupP";
        }
    }
}
