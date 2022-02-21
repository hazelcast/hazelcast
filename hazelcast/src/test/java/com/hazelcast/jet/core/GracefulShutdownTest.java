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

package com.hazelcast.jet.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.map.MapStore;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({SlowTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class GracefulShutdownTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;

    private HazelcastInstance[] instances;
    private HazelcastInstance client;

    @Before
    public void setup() {
        TestProcessors.reset(0);
        instances = createHazelcastInstances(NODE_COUNT);
        client = createHazelcastClient();
        EmitIntegersP.savedCounters.clear();
    }

    @Test
    public void when_snapshottedJob_coordinatorShutDown_then_gracefully() {
        when_shutDown(true, true);
    }

    @Test
    public void when_snapshottedJob_nonCoordinatorShutDown_then_gracefully() {
        when_shutDown(false, true);
    }

    @Test
    public void when_nonSnapshottedJob_coordinatorShutDown_then_restarts() {
        when_shutDown(true, false);
    }

    @Test
    public void when_nonSnapshottedJob_nonCoordinatorShutDown_then_restarts() {
        when_shutDown(false, false);
    }

    private void when_shutDown(boolean shutdownCoordinator, boolean snapshotted) {
        DAG dag = new DAG();
        final int numItems = 50_000;
        Vertex source = dag.newVertex("source", throttle(() -> new EmitIntegersP(numItems), 10_000)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(between(source, sink));

        Job job = client.getJet().newJob(dag, new JobConfig()
                .setProcessingGuarantee(snapshotted ? EXACTLY_ONCE : NONE)
                .setSnapshotIntervalMillis(HOURS.toMillis(1)));
        assertJobStatusEventually(job, JobStatus.RUNNING);
        logger.info("sleeping 1 sec");
        sleepSeconds(1);

        int shutDownInstance = shutdownCoordinator ? 0 : 1;
        int liveInstance = shutdownCoordinator ? 1 : 0;

        // When
        logger.info("Shutting down instance...");
        instances[shutDownInstance].shutdown();
        logger.info("Joining job...");
        job.join();
        logger.info("Joined");

        // Then
        // If the shutdown was graceful, output items must not be duplicated
        Map<Integer, Integer> expected;
        Map<Integer, Integer> actual = new ArrayList<>(instances[liveInstance].<Integer>getList("sink")).stream()
                .collect(Collectors.toMap(Function.identity(), item -> 1, Integer::sum));
        if (snapshotted) {
            logger.info("savedCounters=" + EmitIntegersP.savedCounters);
            assertEquals(EmitIntegersP.savedCounters.toString(), 2, EmitIntegersP.savedCounters.size());
            int minCounter = EmitIntegersP.savedCounters.values().stream().mapToInt(Integer::intValue).min().getAsInt();
            expected = IntStream.range(0, numItems).boxed()
                    .collect(Collectors.toMap(Function.identity(), item -> item < minCounter ? 2 : 1));
            assertEquals(expected, actual);
        } else {
            // Items 0, 1, ... up to the point when the member was shut down should be 3 times
            // in the output: twice from before shutdown and one from after it, because it will start
            // from the beginning when the job is not snapshotted.
            assertEquals(3, actual.get(0).intValue());
            assertEquals(3, actual.get(1).intValue());
            assertEquals(1, actual.get(numItems - 1).intValue());
        }
    }

    @Test
    public void when_liteMemberShutDown_then_jobKeepsRunning() throws Exception {
        Config liteMemberConfig = smallInstanceConfig();
        liteMemberConfig.setLiteMember(true);
        HazelcastInstance liteMember = createHazelcastInstance(liteMemberConfig);
        DAG dag = new DAG();
        dag.newVertex("v", (SupplierEx<Processor>) NoOutputSourceP::new);
        Job job = instances[0].getJet().newJob(dag);
        assertJobStatusEventually(job, JobStatus.RUNNING, 10);
        Future future = spawn(() -> liteMember.shutdown());
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 5);
        future.get();
    }

    @Test
    public void when_nonParticipatingMemberShutDown_then_jobKeepsRunning() throws Exception {
        DAG dag = new DAG();
        dag.newVertex("v", (SupplierEx<Processor>) NoOutputSourceP::new);
        Job job = instances[0].getJet().newJob(dag);
        assertJobStatusEventually(job, JobStatus.RUNNING, 10);
        Future future = spawn(() -> {
            HazelcastInstance nonParticipatingMember = createHazelcastInstance();
            sleepSeconds(1);
            nonParticipatingMember.shutdown();
        });
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 5);
        future.get();
    }

    @Test
    public void when_shutdownGracefulWhileRestartGraceful_then_restartsFromTerminalSnapshot() throws Exception {
        MapConfig mapConfig = new MapConfig(JobRepository.SNAPSHOT_DATA_MAP_PREFIX + "*");
        mapConfig.getMapStoreConfig()
                .setClassName(BlockingMapStore.class.getName())
                .setEnabled(true);
        Config config = instances[0].getConfig();
        ((DynamicConfigurationAwareConfig) config).getStaticConfig().addMapConfig(mapConfig);
        BlockingMapStore.shouldBlock = false;
        BlockingMapStore.wasBlocked = false;

        DAG dag = new DAG();
        int numItems = 5000;
        Vertex source = dag.newVertex("source", throttle(() -> new EmitIntegersP(numItems), 500));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(between(source, sink));
        source.localParallelism(1);
        Job job = instances[0].getJet().newJob(dag, new JobConfig()
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(2000));

        // wait for the first snapshot
        JetServiceBackend jetServiceBackend = getNode(instances[0]).nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
        JobRepository jobRepository = jetServiceBackend.getJobCoordinationService().jobRepository();
        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertTrue(
                jobRepository.getJobExecutionRecord(job.getId()).dataMapIndex() >= 0));

        // When
        BlockingMapStore.shouldBlock = true;
        job.restart();
        assertTrueEventually(() -> assertTrue("blocking did not happen", BlockingMapStore.wasBlocked), 5);

        Future shutdownFuture = spawn(() -> instances[1].shutdown());
        logger.info("savedCounters=" + EmitIntegersP.savedCounters);
        int minCounter = EmitIntegersP.savedCounters.values().stream().mapToInt(Integer::intValue).min().getAsInt();
        BlockingMapStore.shouldBlock = false;
        shutdownFuture.get();

        // Then
        job.join();

        Map<Integer, Integer> actual = new ArrayList<>(instances[0].<Integer>getList("sink")).stream()
                .collect(Collectors.toMap(Function.identity(), item -> 1, Integer::sum));
        Map<Integer, Integer> expected = IntStream.range(0, numItems).boxed()
                .collect(Collectors.toMap(Function.identity(), item -> item < minCounter ? 2 : 1));
        assertEquals(expected, actual);
    }

    private static final class EmitIntegersP extends AbstractProcessor {
        static final ConcurrentMap<Integer, Integer> savedCounters = new ConcurrentHashMap<>();

        private int counter;
        private int globalIndex;
        private int numItems;

        EmitIntegersP(int numItems) {
            this.numItems = numItems;
        }

        @Override
        protected void init(@Nonnull Context context) {
            globalIndex = context.globalProcessorIndex();
        }

        @Override
        public boolean complete() {
            if (tryEmit(counter)) {
                counter++;
            }
            return counter == numItems;
        }

        @Override
        public boolean saveToSnapshot() {
            savedCounters.put(globalIndex, counter);
            return tryEmitToSnapshot(broadcastKey(globalIndex), counter);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            counter = Math.max(counter, (Integer) value);
        }
    }

    /**
     * A MapStore that will block map operations until unblocked.
     */
    private static class BlockingMapStore implements MapStore {
        private static volatile boolean shouldBlock;
        private static volatile boolean wasBlocked;

        @Override
        public void store(Object key, Object value) {
            block();
        }

        @Override
        public void storeAll(Map map) {
            block();
        }

        @Override
        public void delete(Object key) {
            block();
        }

        @Override
        public void deleteAll(Collection keys) {
            block();
        }

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            return null;
        }

        @Override
        public Iterable loadAllKeys() {
            return null;
        }

        private void block() {
            while (shouldBlock) {
                wasBlocked = true;
                sleepMillis(100);
            }
        }
    }
}
