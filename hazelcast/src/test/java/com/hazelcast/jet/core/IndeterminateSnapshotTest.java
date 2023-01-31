/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.FirewallingServer;
import com.hazelcast.internal.server.OperationPacketFilter;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.JobProxy;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JetTestSupport.getNode;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.impl.JobRepository.JOB_EXECUTION_RECORDS_MAP_NAME;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.answersWithDelay;
import static org.mockito.AdditionalAnswers.returnsSecondArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

//TODO: manual snapshot tests in EE
@RunWith(Enclosed.class)
public class IndeterminateSnapshotTest {

    private static final int NODE_COUNT = 5;
    private static final int BASE_PORT = NetworkConfig.DEFAULT_PORT;
    // increase number of chunks in snapshot by setting greater parallelism
    private static final int LOCAL_PARALLELISM = 10;

    public abstract static class IndeterminateSnapshotTestBase extends JetTestSupport {

        protected HazelcastInstance[] instances;

        protected CountDownLatch snapshotDone;

        protected Config getConfig() {
            return smallInstanceConfig();
        }

        @Before
        public void setup() {
            initSnapshotDoneCounter();
            SnapshotInstrumentationP.reset();

            instances = createHazelcastInstances(getConfig(), NODE_COUNT);
        }

        protected void setupJetTests(int allowedSnapshotsCount) {
            SnapshotInstrumentationP.allowedSnapshotsCount = allowedSnapshotsCount;
            SnapshotInstrumentationP.snapshotCommitFinishConsumer = success -> {
                // allow normal snapshot after job restart
                SnapshotInstrumentationP.saveSnapshotConsumer = null;
                SnapshotInstrumentationP.snapshotCommitPrepareConsumer = null;
                snapshotDone.countDown();
            };
        }

        private int getActiveNodesCount() {
            int terminatedCount = (instances != null && instances[0].getLifecycleService().isRunning()) ? 0 : 1;
            return NODE_COUNT - terminatedCount;
        }

        protected void initSnapshotDoneCounter() {
            snapshotDone = new CountDownLatch(getActiveNodesCount() * LOCAL_PARALLELISM);
        }

        /**
         * Executes given action once when invoked multiple times from many instances of processor.
         */
        protected <T> Consumer<T> singleExecutionConsumer(Consumer<T> snapshotAction) {
            AtomicBoolean executed = new AtomicBoolean(false);
            return (idx) -> {
                // synchronize so all snapshot chunk operations have broken replication
                synchronized (executed) {
                    if (executed.compareAndSet(false, true)) {
                        snapshotAction.accept(idx);
                    }
                }
            };
        }

        protected <T> Consumer<T> lastExecutionConsumer(Consumer<T> snapshotAction) {
            AtomicInteger executed = new AtomicInteger();
            return (idx) -> {
                int activeNodesCount = getActiveNodesCount();
                if (executed.incrementAndGet() >= activeNodesCount * LOCAL_PARALLELISM) {
                    snapshotAction.accept(idx);
                }
            };
        }

        protected <T> Consumer<T> breakSnapshotConsumer(String message, Runnable snapshotAction) {
            return singleExecutionConsumer((idx) -> {
                logger.info("Breaking replication in " + message + " for " + idx);
                snapshotAction.run();
                logger.finest("Proceeding with " + message + " " + idx + " after breaking replication");
            });
        }

        protected void waitForCorruptedSnapshot() throws InterruptedException {
            logger.info("Waiting for corrupted snapshot");
            assertTrue(snapshotDone.await(30, TimeUnit.SECONDS));
            logger.info("Got corrupted snapshot");
        }

        /**
         * @param allowedSnapshotsCount snapshot id that should be used to restore
         *                              or -1 if the job should not be restored
         */
        protected static void assertRestoredFromSnapshot(int allowedSnapshotsCount) {
            if (allowedSnapshotsCount >= 0) {
                assertTrue("Should be restored from snapshot", SnapshotInstrumentationP.restoredFromSnapshot);
                assertThat(SnapshotInstrumentationP.restoredCounters.values())
                        .as("Should restore from last known good snapshot")
                        .containsOnly(allowedSnapshotsCount);
            } else {
                assertFalse("Should not be restored from snapshot", SnapshotInstrumentationP.restoredFromSnapshot);
            }
        }

        /**
         * @param snapshotId snapshot id for last snapshot (including in progress)
         *                   or -1 if no snapshot is expected
         */
        protected static void assumePresentLastSnapshot(int snapshotId) {
            // TODO: assume instead of assert
            if (snapshotId >= 0) {
                assertThat(SnapshotInstrumentationP.savedCounters.values())
                        .as("Current snapshot is different than expected")
                        .containsOnly(snapshotId);
            } else {
                assertThat(SnapshotInstrumentationP.savedCounters)
                        .as("Unexpected snapshot")
                        .isEmpty();
            }
        }

        @NotNull
        protected Job createJob(int numItems) {
            DAG dag = new DAG();
            Vertex source = dag.newVertex("source", throttle(() -> new SnapshotInstrumentationP(numItems), 1)).localParallelism(LOCAL_PARALLELISM);
            Vertex sink = dag.newVertex("sink", Processors.noopP()).localParallelism(1);
            dag.edge(between(source, sink));

            return instances[0].getJet().newJob(dag, customizeJobConfig(new JobConfig()
                    .setProcessingGuarantee(EXACTLY_ONCE)
                    // trigger snapshot often to speed up test
                    .setSnapshotIntervalMillis(1000)));
        }

        protected JobConfig customizeJobConfig(JobConfig config) {
            return config;
        }
    }

    /**
     * Tests that break replication in the cluster. More realistic, but slower,
     * and sometimes it is hard to get correct partitions assignment.
     */
    // TODO: make this SlowTest? This test needs mock network
    @Category({QuickTest.class, ParallelJVMTest.class})
    @RunWith(HazelcastSerialClassRunner.class)
    public static class ReplicationBreakingTests extends IndeterminateSnapshotTestBase {
        // SnapshotVerificationKey
        private int masterKeyPartitionInstanceIdx;
        private int backupKeyPartitionInstanceIdx;
        // Job
        private Integer masterJobPartitionInstanceIdx;
        private Integer backupJobPartitionInstanceIdx;

        private CompletableFuture<Integer> failingInstanceFuture;

        @Override
        public Config getConfig() {
            Config config = super.getConfig();
            // slow down anti-entropy process, so it does not kick in during test
            config.setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "120");
            return config;
        }

        @Before
        @Override
        public void setup() {
            // @Repeat does not recreate test class object, so all initialization must be in the setup method
            super.setup();
            failingInstanceFuture = new CompletableFuture<>();
        }

        @Override
        protected void setupJetTests(int allowedSnapshotsCount) {
            super.setupJetTests(allowedSnapshotsCount);

            setupPartitions();
        }

        // This test checks if our way to simulate map replication failure is reliable
        // which is critical for the forceful shutdown test.
        // Should detect if new anti-entropy mechanisms are introduced.
        @Test
        public void mapShouldBeCorrupted_whenReplicationNetworkFailure() throws ExecutionException, InterruptedException, TimeoutException {
            // Jet does not start when there is no data in partitions (?)
            IMap<String, String> otherMap = instances[0].getMap("dummy");
            otherMap.put("1", "2");

            IMap<String, String> map = instances[0].getMap("testMapPutAndGet");

            InternalPartition partitionForKey = getPartitionForKey("Hello");
            logger.fine("KEY partition id:" + partitionForKey.getPartitionId() + " owner: " + partitionForKey.getOwnerOrNull());
            for (int i = 0; i < 3; ++i) {
                logger.fine("KEY partition addr:" + partitionForKey.getReplica(i));
            }

            int masterPartitionInstanceIdx = partitionForKey.getReplica(0).address().getPort() - BASE_PORT;
            int backupPartitionInstanceIdx = partitionForKey.getReplica(1).address().getPort() - BASE_PORT;
            int noPartitionInstanceIdx = partitionForKey.getReplica(2).address().getPort() - BASE_PORT;

            setBackupPacketDropFilter(instances[masterPartitionInstanceIdx], 1f);
            setBackupPacketDropFilter(instances[backupPartitionInstanceIdx], 0f, instances[masterPartitionInstanceIdx]);
            setBackupPacketDropFilter(instances[noPartitionInstanceIdx], 0f);

            // String value = map.put("Hello", "World");
            String value = map.putAsync("Hello", "World").toCompletableFuture().get();
            assertEquals("World", map.get("Hello"));
            assertEquals(1, map.size());
            assertNull(value);

            final int numExtraPuts = 60;

            CompletableFuture<?>[] futures = IntStream.range(0, numExtraPuts)
                    .mapToObj(i -> map.putAsync("Hello" + i, "World"))
                    .map(CompletionStage::toCompletableFuture)
                    .toArray(CompletableFuture[]::new);

            // wait for put operations completion
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);

            // kill master
            logger.info("Killing master: " + masterPartitionInstanceIdx);
            instances[masterPartitionInstanceIdx].getLifecycleService().terminate();

            assertTrueEventually(() -> {
                boolean state =
                        instances[backupPartitionInstanceIdx].getPartitionService().isLocalMemberSafe()
                                && instances[noPartitionInstanceIdx].getPartitionService().isLocalMemberSafe();
                if (!state) {
                    logger.fine("Partition replication is not in safe state");
                }
                assertTrue(state);
            });

            IMap<String, String> mapNoInstance = instances[noPartitionInstanceIdx].getMap("testMapPutAndGet");
            assertNull("Should lose write to master", mapNoInstance.get("Hello"));
            assertGreaterOrEquals("Should keep some writes", mapNoInstance.size(), 2);
            assertNotEquals("Should lose some writes", 1 + numExtraPuts, mapNoInstance.size());

            IMap<String, String> mapBackup = instances[backupPartitionInstanceIdx].getMap("testMapPutAndGet");
            assertNull("Should lose write to master", mapBackup.get("Hello"));
            assertGreaterOrEquals("Should keep some writes", mapBackup.size(), 2);
            assertNotEquals("Should lose some writes", 1 + numExtraPuts, mapBackup.size());
        }

        // demonstrates that IMap.clear() can silently fail to send backups
        // and some elements may remain in the IMap
        @Test
        public void mapShouldNotBeFullyCleared_whenReplicationNetworkFailure() {
            // Jet does not start when there is no data in partitions (?)
            IMap<String, String> otherMap = instances[0].getMap("dummy");
            otherMap.put("1", "2");

            IMap<String, String> map = instances[0].getMap("testMapPutAndGet");

            InternalPartition partitionForKey = getPartitionForKey("Hello");
            logger.fine("KEY partition id:" + partitionForKey.getPartitionId() + " owner: " + partitionForKey.getOwnerOrNull());
            for (int i = 0; i < 3; ++i) {
                logger.fine("KEY partition addr:" + partitionForKey.getReplica(i));
            }

            int masterPartitionInstanceIdx = partitionForKey.getReplica(0).address().getPort() - BASE_PORT;
            int backupPartitionInstanceIdx = partitionForKey.getReplica(1).address().getPort() - BASE_PORT;
            int noPartitionInstanceIdx = partitionForKey.getReplica(2).address().getPort() - BASE_PORT;

            String value = map.put("Hello", "World");
            assertEquals("World", map.get("Hello"));
            assertEquals(1, map.size());
            assertNull(value);

            final int numExtraPuts = 60;
            IntStream.range(0, numExtraPuts)
                    .forEach(i -> map.put("Hello" + i, "World"));
            assertEquals(1 + numExtraPuts, map.size());

            // break network
            setBackupPacketDropFilter(instances[masterPartitionInstanceIdx], 1f);
            setBackupPacketDropFilter(instances[backupPartitionInstanceIdx], 0f, instances[masterPartitionInstanceIdx]);
            setBackupPacketDropFilter(instances[noPartitionInstanceIdx], 0f);

            map.clear();

            // kill master
            logger.info("Killing master: " + masterPartitionInstanceIdx);
            instances[masterPartitionInstanceIdx].getLifecycleService().terminate();

            assertTrueEventually(() -> {
                boolean state =
                        instances[backupPartitionInstanceIdx].getPartitionService().isLocalMemberSafe()
                                && instances[noPartitionInstanceIdx].getPartitionService().isLocalMemberSafe();
                if (!state) {
                    logger.fine("Partition replication is not in safe state");
                }
                assertTrue(state);
            });

            IMap<String, String> mapNoInstance = instances[noPartitionInstanceIdx].getMap("testMapPutAndGet");

            logger.fine("Size after clear: " + mapNoInstance.size());
            assertGreaterOrEquals("Should lose some clear executions", mapNoInstance.size(), 1);

            assertEquals("Should lose clear on master", "World", mapNoInstance.get("Hello"));
            assertNotEquals("Should clear some entries", 1 + numExtraPuts, mapNoInstance.size());

            IMap<String, String> mapBackup = instances[backupPartitionInstanceIdx].getMap("testMapPutAndGet");
            assertEquals("Should lose clear on master", "World", mapBackup.get("Hello"));
            assertNotEquals("Should clear some entries", 1 + numExtraPuts, mapBackup.size());
        }

        @Test
        public void whenFirstSnapshotPossiblyCorrupted_thenRestartWithoutSnapshot() throws InterruptedException {
            when_shutDown(0);
        }

        @Test
        public void whenNextSnapshotPossiblyCorrupted_thenRestartFromLastGoodSnapshot() throws InterruptedException {
            when_shutDown(3);
        }

        private void setupPartitions() {
            InternalPartition partitionForKey = getPartitionForKey(SnapshotValidationRecord.KEY);
            logger.info("SVR KEY partition id:" + partitionForKey.getPartitionId());
            for (int i = 0; i < 3; ++i) {
                logger.info("SVR KEY partition addr:" + partitionForKey.getReplica(i));
            }
            masterKeyPartitionInstanceIdx = partitionForKey.getReplica(0).address().getPort() - BASE_PORT;
            backupKeyPartitionInstanceIdx = partitionForKey.getReplica(1).address().getPort() - BASE_PORT;
        }

        private <K> InternalPartition getPartitionForKey(K key) {
            // to determine partition we can use any instance (let's pick the first one)
            HazelcastInstance instance = instances[0];
            InternalPartitionService partitionService = getNode(instance).getPartitionService();
            int partitionId = partitionService.getPartitionId(key);
            return partitionService.getPartition(partitionId);
        }

        private void when_shutDown(int allowedSnapshotsCount) throws InterruptedException {

            // We need to keep replicated SVR KEY and JobExecutionRecord for jobId.
            // Each can be in different partition that can be on different instance
            // (primary and backup), so we need to have at least 5 nodes, so one
            // of them can be broken to corrupt snapshot data but not affect job metadata
            // (JobExecutionRecord and SnapshotValidationRecord.KEY).
            assertTrue(NODE_COUNT >= 5);

            setupJetTests(allowedSnapshotsCount);

            // Scenario:
            // - start job normally
            // - when first job snapshot is started:
            //   - break synchronous IMap backup on one node (failingInstance)
            //   - break anti-entropy executed on replica versions mismatch
            //     (REPLICA_SYNC_REQUEST_OFFLOADABLE) related to failingInstance
            // - wait for snapshot completion
            // - disconnect and terminate victim node (should not matter if it is Jet master or not)
            // - job should be restarted, but should not use broken snapshot
            final int numItems = allowedSnapshotsCount + 5;

            SnapshotInstrumentationP.saveSnapshotConsumer =
                    breakSnapshotConsumer("snapshot", this::breakFailingInstance);

            // dummy job
            // start job after SnapshotInstrumentationP setup to avoid race
            Job job = createJob(numItems);

            // choose failing node
            int failingInstance = chooseFailingInstance(masterKeyPartitionInstanceIdx, backupKeyPartitionInstanceIdx,
                    masterJobPartitionInstanceIdx, backupJobPartitionInstanceIdx);
            int liveInstance = masterKeyPartitionInstanceIdx;

            // ensure that job started
            assertJobStatusEventually(job, JobStatus.RUNNING);

            waitForCorruptedSnapshot();
            sleepMillis(500);

            logger.info("Shutting down instance... " + failingInstance);
            instances[failingInstance].getLifecycleService().terminate();
            logger.info("Removed instance " + failingInstance + " from cluster");
            // restore network between remaining nodes
            restoreNetwork();

            logger.info("Joining job...");
            // TODO: in reproducer job.join() fails, after fix should not throw but complete normally
            instances[liveInstance].getJet().getJob(job.getId()).join();
//        assertThatThrownBy(() -> instances[liveInstance].getJet().getJob(job.getId()).join())
//                .hasMessageContainingAll("State for job", "is corrupted: it should have");
            logger.info("Joined");

            assertRestoredFromSnapshot(allowedSnapshotsCount - 1);
        }

        // repeat test to first success. @Repeat does not recreate test class instance.
        private boolean test1Succeeded;

        @Test
        @Repeat(5)
        public void whenFirstSnapshotPossiblyCorruptedAfter1stPhase_thenRestartWithoutSnapshot() throws InterruptedException {
            assumeThat(test1Succeeded).as("Test already succeeded").isFalse();
            when_shutDownAfter1stPhase(0);
            test1Succeeded = true;
        }

        private boolean test2Succeeded;

        @Test
        @Repeat(5)
        public void whenNextSnapshotPossiblyCorruptedAfter1stPhase_thenRestartFromLastGoodSnapshot() throws InterruptedException {
            assumeThat(test2Succeeded).as("Test already succeeded").isFalse();
            when_shutDownAfter1stPhase(3);
            test2Succeeded = true;
        }

        private void when_shutDownAfter1stPhase(int allowedSnapshotsCount) throws InterruptedException {

            setupJetTests(allowedSnapshotsCount);

            // Scenario:
            // - start job normally
            // - when first job snapshot is started:
            //   - wait for completion of 1st phase of snapshot
            //   - break synchronous IMap backup on one node (failingInstance)
            //   - break anti-entropy executed on replica versions mismatch
            //     (REPLICA_SYNC_REQUEST_OFFLOADABLE) related to failingInstance
            // - disconnect and terminate victim node (should not matter if it is Jet master or not)
            // - job should be restarted, but should not use broken snapshot
            final int numItems = allowedSnapshotsCount + 5;

            SnapshotInstrumentationP.snapshotCommitPrepareConsumer =
                    breakSnapshotConsumer("commit prepare", this::breakFailingInstance);

            // dummy job
            // start job after SnapshotInstrumentationP setup to avoid race
            Job job = createJob(numItems);

            // choose failing node

            // Due to these constraints this test cannot be executed 2 times out of NODE_COUNT times.
            // They are necessary for the test to be more deterministic but jobId is random.
            // The test is repeated a few times so the chance that it actually runs are increased.
            assumeThat(masterKeyPartitionInstanceIdx)
                    // Keep job data, in particular JobExecutionRecord safe
                    .as("Should not damage job data").isNotEqualTo(masterJobPartitionInstanceIdx)
                    // and not restart Jet master
                    .as("Should not restart Jet master").isNotEqualTo(0);

            int failingInstance = chooseConstantFailingNode(masterKeyPartitionInstanceIdx);
            int liveInstance = backupKeyPartitionInstanceIdx;

            // ensure that job started
            assertJobStatusEventually(job, JobStatus.RUNNING);

            waitForCorruptedSnapshot();
            sleepMillis(500);

            logger.info("Shutting down instance... " + failingInstance);
            instances[failingInstance].getLifecycleService().terminate();
            logger.info("Removed instance " + failingInstance + " from cluster");
            // restore network between remaining nodes
            restoreNetwork();

            logger.info("Joining job...");
            // TODO: in reproducer job.join() fails, after fix should not throw but complete normally
            instances[liveInstance].getJet().getJob(job.getId()).join();
//        assertThatThrownBy(() -> instances[liveInstance].getJet().getJob(job.getId()).join())
//                .hasMessageContainingAll("snapshot with ID", "is damaged. Unable to restore the state for job");
            logger.info("Joined");

            assertRestoredFromSnapshot(allowedSnapshotsCount - 1);
        }

        @NotNull
        @Override
        protected Job createJob(int numItems) {
            Job job = super.createJob(numItems);

            Long jobId = job.getId();
            InternalPartition partitionForJob = getPartitionForKey(jobId);
            logger.info("Job partition id:" + partitionForJob.getPartitionId());
            for (int i = 0; i < 3; ++i) {
                logger.info("Job partition addr:" + partitionForJob.getReplica(i));
            }
            masterJobPartitionInstanceIdx = partitionForJob.getReplica(0).address().getPort() - BASE_PORT;
            backupJobPartitionInstanceIdx = partitionForJob.getReplica(1).address().getPort() - BASE_PORT;

            return job;
        }

        /**
         * Choose one of the instances except given ones and mark it as destined to fail.
         * @param safeNodes Nodes that should not be terminated
         * @return chosen failing instance
         */
        private int chooseFailingInstance(Integer... safeNodes) {
            Set<Integer> nodes = IntStream.range(0, NODE_COUNT).boxed().collect(Collectors.toCollection(HashSet::new));
            // note that `safeNodes` must be `Integer` not `int` so asList does not return List<int[]>
            Arrays.asList(safeNodes).forEach(nodes::remove);
            logger.info("Replicas that can be broken: " + nodes);
            assertThat(nodes).isNotEmpty();
            int failingInstance = nodes.stream().findAny().get();
            return chooseConstantFailingNode(failingInstance);
        }

        private int chooseConstantFailingNode(int failingInstance) {
            logger.info("Failing instance selected: " + failingInstance);
            failingInstanceFuture.complete(failingInstance);
            return failingInstance;
        }

        /**
         * Breaks backups that should be stored on failing instance.
         */
        private void breakFailingInstance() {
            try {
                Integer failingInstanceIdx = failingInstanceFuture.get();
                for (int i = 0; i < NODE_COUNT; ++i) {
                    setBackupPacketDropFilter(instances[i], i != failingInstanceIdx ? 0f : 1f, instances[failingInstanceIdx]);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        private void restoreNetwork() {
            int failingInstanceIdx;
            try {
                failingInstanceIdx = failingInstanceFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < NODE_COUNT; ++i) {
                if (i != failingInstanceIdx) {
                    resetPacketFiltersFrom(instances[i]);
                }
            }
        }
    }

    /**
     * These tests use simplified approach to simulating IMap failure. Instead
     * of trying to terminate appropriate node on given time they inject
     * erroneous IMap operation responses. This is much simpler and allows to
     * test more scenarios. However, tests terminating nodes are also useful as
     * more resembling reality.
     */
    @Category({QuickTest.class, ParallelJVMTest.class})
    @RunWith(HazelcastParametrizedRunner.class)
    @UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
    public static class SnapshotFailureTests extends IndeterminateSnapshotTestBase {

        @Parameter
        public boolean suspendOnFailure;

        @Parameterized.Parameters(name = "suspendOnFailure:{0}")
        public static Object[] parameters() {
            return new Object[] { false, true };
        }

        @Override
        protected JobConfig customizeJobConfig(JobConfig config) {
            return config.setSuspendOnFailure(suspendOnFailure);
        }

        // fixes problem with mock deserialization on cluster side.
        // probably registers generated mockito classes in classloader that is shared with the HZ instances.
        private MapInterceptor registerMockInClassloader = mock(MapInterceptor.class, withSettings().serializable());

        @Test
        public void whenNextSnapshotUpdateLostSameCoordinator_thenRestartFromFirstSnapshot() {
            whenSnapshotUpdateLostSameCoordinator(0);
        }

        @Test
        public void whenNextSnapshotUpdateLostSameCoordinator_thenRestartFromLastGoodSnapshot() {
            whenSnapshotUpdateLostSameCoordinator(3);
        }

        private void whenSnapshotUpdateLostSameCoordinator(int initialSnapshotsCount) {
            // JobExecutionRecord update during snapshot commit is indeterminate and lost,
            // but succeeds after job restart on the same coordinator.
            // Indeterminate snapshot turns out to be correct.

            new SuccessfulSnapshots(Math.max(initialSnapshotsCount - 1, 0))
                    .then(new IndeterminateLostPut())
                    .then(new SuccessfulSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);

            logger.info("Joining job...");
            instances[0].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            assertRestoredFromSnapshot(initialSnapshotsCount);
        }

        @Test
        public void whenSnapshotUpdateLostChangedCoordinatorNoOtherSnapshot_thenRestartWithoutSnapshot() throws InterruptedException {
            whenSnapshotUpdateLostChangedCoordinator(0);
        }

        @Test
        public void whenSnapshotUpdateLostChangedCoordinator_thenRestartFromLastGoodSnapshot() throws InterruptedException {
            whenSnapshotUpdateLostChangedCoordinator(3);
        }

        private void whenSnapshotUpdateLostChangedCoordinator(int initialSnapshotsCount) throws InterruptedException {
            // JobExecutionRecord update during snapshot commit is indeterminate and lost,
            // and is indeterminate until original coordinator is terminated.
            // Job is restart on different coordinator and indeterminate snapshot turns out to be lost.

            CompletableFuture<Void> coordinatorTerminated = new CompletableFuture<>();

            new SuccessfulSnapshots(Math.max(initialSnapshotsCount - 1, 0))
                    // snapshot during suspend will be indeterminate until coordinator is terminated
                    .then(new IndeterminateLostPutsUntil(coordinatorTerminated))
                    // will be restored from previous snapshot, reuse counter
                    .then(new SuccessfulSnapshots(1).reusedCounter())
                    .then(new SuccessfulIgnoredSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);
            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                logger.info("Waiting for snapshot...");
                snapshotDone.await();
            }

            // wait for job restart
            logger.info("Waiting for job restart...");
            assertJobStatusEventually(job, JobStatus.STARTING);
            assertThat(snapshotDone.getCount())
                    .as("Snapshot must not be committed when indeterminate")
                    .isEqualTo(NODE_COUNT * LOCAL_PARALLELISM);

            // terminate coordinator
            logger.info("Terminating coordinator...");
            instances[0].getLifecycleService().terminate();
            coordinatorTerminated.complete(null);

            logger.info("Joining job...");
            instances[1].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            assertThat(snapshotDone.getCount())
                    .as("Snapshot should be ultimately committed")
                    .isZero();
            assertRestoredFromSnapshot(initialSnapshotsCount - 1);
        }

        @Test
        public void whenSuspendSnapshotUpdateLostSameCoordinatorAndNoOtherSnapshots_thenRestartFromSuspendSnapshot() throws InterruptedException {
            whenSuspendSnapshotUpdateLostSameCoordinator(0);
        }

        @Test
        public void whenSuspendSnapshotUpdateLostSameCoordinator_thenRestartFromSuspendSnapshot() throws InterruptedException {
            whenSuspendSnapshotUpdateLostSameCoordinator(3);
        }

        private abstract class AbstractScenarioStep {
            @Nullable
            private AbstractScenarioStep next;
            @Nullable
            protected AbstractScenarioStep prev;
            /**
             * Null means step that adds some action, but does not increase number of completed snapshots
             */
            protected Integer repetitions = 1;

            public AbstractScenarioStep repeat(Integer repetitions) {
                this.repetitions = repetitions;
                return this;
            }

            public <T extends AbstractScenarioStep> T then(T next) {
                this.next = next;
                next.prev = this;
                return next;
            }

            /**
             * Configure {@link SnapshotInstrumentationP} according to this step
             */
            public final void apply() {
                if (repetitions == null || repetitions > 0) {
                    logger.info("Applying scenario step " + this);
                    if (repetitions != null) {
                        SnapshotInstrumentationP.allowedSnapshotsCount += repetitions;
                    }

                    // init consumers to default values
                    SnapshotInstrumentationP.saveSnapshotConsumer = null;
                    SnapshotInstrumentationP.snapshotCommitPrepareConsumer = null;
                    // If step breaks snapshot, snapshotCommitFinishConsumer may be not invoked.
                    // In such case step is responsible for advancing the scenario.
                    SnapshotInstrumentationP.snapshotCommitFinishConsumer =
                            ((Consumer<Boolean>) (b) -> snapshotDone.countDown())
                            .andThen(lastExecutionConsumer((b) -> goToNextStep()));

                    doApply();
                } else {
                    logger.info("Skipping scenario step " + this);
                    goToNextStep();
                }
            }

            protected abstract void doApply();

            protected void goToNextStep() {
                if (next != null) {
                    next.apply();
                }
            }

            /**
             * Start executing the scenario
             */
            public final void start() {
                getFirst().apply();
            }

            @Nonnull
            private AbstractScenarioStep getFirst() {
                AbstractScenarioStep first = this;
                while (first.prev != null) {
                    first = first.prev;
                }
                return first;
            }
        }

        private class SuccessfulSnapshots extends AbstractScenarioStep {
            SuccessfulSnapshots() {
                super();
            }

            SuccessfulSnapshots(int repetitions) {
                this();
                repeat(repetitions);
            }

            /**
             * Snapshot counter will be reused. Most often this is the case after restore.
             */
            SuccessfulSnapshots reusedCounter() {
                repeat(repetitions > 1 ? repetitions - 1 : null);
                return this;
            }

            @Override
            protected void doApply() {
                // will count commits of last repetition
                initSnapshotDoneCounter();
                SnapshotInstrumentationP.snapshotCommitFinishConsumer =
                        ((Consumer<Boolean>) (b) -> snapshotDone.countDown())
                                .andThen(lastExecutionConsumer((b) -> goToNextStep()));
            }

            @Override
            public String toString() {
                return String.format("Successful %d Snapshots", repetitions);
            }
        }

        private class SuccessfulIgnoredSnapshots extends AbstractScenarioStep {
            SuccessfulIgnoredSnapshots() {
                super();
            }

            SuccessfulIgnoredSnapshots(int repetitions) {
                this();
                repeat(repetitions);
            }
            @Override
            protected void doApply() {
                // nothing to break
            }

            @Override
            public String toString() {
                return String.format("Successful %d Snapshots - do not count", repetitions);
            }
        }

        /**
         * 1 lost indeterminate IMap update, then successes (configured number of repetitions)
         */
        private class IndeterminateLostPut extends AbstractScenarioStep {
            IndeterminateLostPut() {
                super();
                repeat(null);
            }

            @Override
            protected void doApply() {
                initSnapshotDoneCounter();
                SnapshotInstrumentationP.snapshotCommitPrepareConsumer =
                        SnapshotFailureTests.this.<Integer>
                            breakSnapshotConsumer("snapshot commit prepare",
                                        SnapshotFailureTests.this::singleIndeterminatePutLost)
                            .andThen(lastExecutionConsumer((Integer b) -> {
                                // there should be no commit complete
                                goToNextStep();
                            }));
            }

            @Override
            public String toString() {
                return "IndeterminateLostPut";
            }
        }

        /**
         * Lost indeterminate IMap updates until condition is satisfied
         */
        private class IndeterminateLostPutsUntil extends AbstractScenarioStep {

            private final CompletableFuture<String> registration = new CompletableFuture<>();

            <T> IndeterminateLostPutsUntil(CompletableFuture<T> condition) {
                super();
                repeat(null);

                // fix IMap when condition is met
                condition.thenCombineAsync(registration, (v, reg) -> {
                    logger.info("Fixing IMap replication");
                    getJobExecutionRecordIMap().removeInterceptor(reg);
                    goToNextStep();
                    return null;
                });
            }

            @Override
            protected void doApply() {
                initSnapshotDoneCounter();
                SnapshotInstrumentationP.snapshotCommitPrepareConsumer =
                        breakSnapshotConsumer("snapshot commit prepare",
                                () -> registration.complete(allIndeterminatePutsLost()));
            }

            @Override
            public String toString() {
                return "IndeterminateLostPutsUntil";
            }
        }

        /**
         * @param initialSnapshotsCount number of snapshots before suspend
         */
        private void whenSuspendSnapshotUpdateLostSameCoordinator(int initialSnapshotsCount) throws InterruptedException {
            // Suspend operation is initiated.
            // JobExecutionRecord update during snapshot commit is indeterminate and lost,
            // but succeeds after job restart on the same coordinator.
            // Indeterminate snapshot turns out to be correct.

            new SuccessfulSnapshots(Math.max(initialSnapshotsCount - 1, 0))
                    .then(new IndeterminateLostPut())
                    .then(new SuccessfulSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);

            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                // wait for initial successful snapshots
                snapshotDone.await();
            }

            // there is a race between suspend and regular snapshots,
            // but we want the test to be executed when there is no snapshot in progress
            // try to detect slow execution and skip test if there is unexpected snapshot
            assumePresentLastSnapshot(initialSnapshotsCount - 1);

            job.suspend();
            assertJobStatusEventually(job, JobStatus.SUSPENDED);
            logger.info("Suspended");

            job.resume();
            assertJobStatusEventually(job, JobStatus.RUNNING);
            logger.info("Resumed");

            logger.info("Joining job...");
            instances[0].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            // indeterminate suspend snapshot turns out to be fine
            assertRestoredFromSnapshot(initialSnapshotsCount);
        }

        @Test
        public void whenSuspendSnapshotUpdateLostChangedCoordinatorAndNoOtherSnapshots_thenRestartWithoutSnapshot() throws InterruptedException {
            whenSuspendSnapshotUpdateLostChangedCoordinator(0);
        }

        @Test
        public void whenSuspendSnapshotUpdateLostChangedCoordinator_thenRestartFromRegularSnapshot() throws InterruptedException {
            whenSuspendSnapshotUpdateLostChangedCoordinator(3);
        }

        private void whenSuspendSnapshotUpdateLostChangedCoordinator(int initialSnapshotsCount) throws InterruptedException {
            // Suspend operation is initiated.
            // JobExecutionRecord update during snapshot commit is indeterminate and lost.
            // Job restarted on different coordinator.
            // Indeterminate snapshot is lost.

            CompletableFuture<Void> coordinatorTerminated = new CompletableFuture<>();

            new SuccessfulSnapshots(Math.max(initialSnapshotsCount - 1, 0))
                    // snapshot during suspend will be indeterminate until coordinator is terminated
                    .then(new IndeterminateLostPutsUntil(coordinatorTerminated))
                    .then(new SuccessfulSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);

            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                // wait for initial successful snapshots
                snapshotDone.await();
            }

            job.suspend();
            assertJobStatusEventually(job, JobStatus.SUSPENDED);
            logger.info("Suspended");
            assertThat(snapshotDone.getCount())
                    .as("Snapshot must not be committed when indeterminate")
                    .isEqualTo(NODE_COUNT * LOCAL_PARALLELISM);

            instances[0].getLifecycleService().terminate();
            coordinatorTerminated.complete(null);

            job = instances[1].getJet().getJob(job.getId());
            job.resume();
            assertJobStatusEventually(job, JobStatus.RUNNING);

            logger.info("Joining job...");
            job.join();
            logger.info("Joined");

            // indeterminate suspend snapshot turns out to disappear
            assertRestoredFromSnapshot(initialSnapshotsCount - 1);
        }

        @Test
        public void whenRestartGracefulSnapshotUpdateLostSameCoordinatorAndNoOtherSnapshots_thenRestartFromRestartSnapshot() throws InterruptedException {
            whenRestartGracefulSnapshotUpdateLostSameCoordinator(0);
        }

        @Test
        public void whenRestartGracefulSnapshotUpdateLostSameCoordinator_thenRestartFromRestartSnapshot() throws InterruptedException {
            whenRestartGracefulSnapshotUpdateLostSameCoordinator(3);
        }

        private void whenRestartGracefulSnapshotUpdateLostSameCoordinator(int initialSnapshotsCount) throws InterruptedException {
            // Restart_graceful operation is initiated (private API).
            // JobExecutionRecord update during snapshot commit is indeterminate and lost,
            // but succeeds after job restart on the same coordinator.
            // Indeterminate snapshot turns out to be correct.

            new SuccessfulSnapshots(Math.max(initialSnapshotsCount - 1, 0))
                    .then(new IndeterminateLostPut())
                    .then(new SuccessfulSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);
            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                snapshotDone.await();
            }

            // there is a race between suspend and regular snapshots,
            // but we want the test to be executed when there is no snapshot in progress
            // try to detect slow execution and skip test if there is unexpected snapshot
            assumePresentLastSnapshot(initialSnapshotsCount - 1);
            ((JobProxy) job).restart(true);

            logger.info("Joining job...");
            instances[0].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            // indeterminate restart graceful snapshot turns out to be fine
            assertRestoredFromSnapshot(initialSnapshotsCount);
        }

        @Nonnull
        private IMap<Object, Object> getJobExecutionRecordIMap() {
            // use last instance, 0 is master, 1 non-master - may be terminated during the test
            return instances[NODE_COUNT - 1].getMap(JOB_EXECUTION_RECORDS_MAP_NAME);
        }

        private void singleIndeterminatePutLost() {
            // affects put and also executeOnKey
            MapInterceptor mockInt = mock(MapInterceptor.class, withSettings().serializable());
            when(mockInt.interceptPut(any(), any()))
                    .thenThrow(new IndeterminateOperationStateException("Simulated lost IMap update"))
                    .thenAnswer(returnsSecondArg());
            getJobExecutionRecordIMap().addInterceptor(mockInt);
        }

        private String allIndeterminatePutsLost() {
            // affects put and also executeOnKey
            MapInterceptor mockInt = mock(MapInterceptor.class, withSettings().serializable());
            when(mockInt.interceptPut(any(), any()))
                    // delay to prevent to many retries
                    .thenAnswer(answersWithDelay(1000,
                            new ThrowsException(new IndeterminateOperationStateException("Simulated lost IMap update"))));
            return getJobExecutionRecordIMap().addInterceptor(mockInt);
        }

        private void singleIndeterminatePutNotLost() {
            // affects put and also executeOnKey
            MapInterceptor mockInt = mock(MapInterceptor.class, withSettings().serializable());
            Mockito.doThrow(new IndeterminateOperationStateException("Simulated not lost IMap update"))
                    .doReturn(null)
                    .when(mockInt).afterPut(any());
            getJobExecutionRecordIMap().addInterceptor(mockInt);
        }
    }

    public static void setBackupPacketDropFilter(HazelcastInstance instance, float blockRatio, HazelcastInstance... blockSync) {
        setBackupPacketDropFilter(instance, blockRatio, Arrays.stream(blockSync).map(Accessors::getAddress).collect(Collectors.toSet()));
    }

    public static void setBackupPacketDropFilter(HazelcastInstance instance, float blockRatio, Set<Address> blockSync) {
        Node node = getNode(instance);
        FirewallingServer.FirewallingServerConnectionManager cm = (FirewallingServer.FirewallingServerConnectionManager)
                node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
        cm.setPacketFilter(new MapBackupPacketDropFilter(node.getThisAddress(), node.getSerializationService(), blockRatio, blockSync));
    }

    private static class MapBackupPacketDropFilter extends OperationPacketFilter {
        private final ILogger logger = Logger.getLogger(getClass());

        private final Address thisAddress;
        private final float blockRatio;
        private final Set<Address> blockSyncTo;

        MapBackupPacketDropFilter(Address thisAddress, InternalSerializationService serializationService, float blockRatio, Set<Address> blockSyncTo) {
            super(serializationService);
            this.thisAddress = thisAddress;
            this.blockRatio = blockRatio;
            this.blockSyncTo = blockSyncTo;
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;

            boolean isSync = factory == PartitionDataSerializerHook.F_ID
                    // REPLICA_SYNC_REQUEST_OFFLOADABLE is sent when backup replica detects
                    // that is it out-of-date because there is never version in partition table
                    && (type == PartitionDataSerializerHook.REPLICA_SYNC_REQUEST_OFFLOADABLE
            );

            Action action;
            if (isBackup) {
                action = (Math.random() >= blockRatio ? Action.ALLOW : Action.DROP);
                logger.info(thisAddress + " sending backup packet to " + endpoint + " action: " + action);
            } else if (isSync) {
                action = blockSyncTo.contains(endpoint) ? Action.DROP : Action.ALLOW;
                logger.info(thisAddress + " sending sync packet (type=" + type + ") to " + endpoint + " action: " + action);
            } else {
                action = Action.ALLOW;
            }

            return action;
        }
    }

    private static final class SnapshotInstrumentationP extends AbstractProcessor {
        static final ConcurrentMap<Integer, Integer> savedCounters = new ConcurrentHashMap<>();
        static final ConcurrentMap<Integer, Integer> restoredCounters = new ConcurrentHashMap<>();

        private int globalIndex;
        private final int numItems;
        /**
         * Number of completed snapshots.
         */
        private int snapshotCounter;
        /**
         * Number of normal snapshots before {@link #saveSnapshotConsumer} and other hooks are called.
         */
        static volatile int allowedSnapshotsCount = 0;
        @Nullable
        static volatile Consumer<Integer> saveSnapshotConsumer;
        @Nullable
        static volatile Consumer<Integer> snapshotCommitPrepareConsumer;
        @Nonnull
        static volatile Consumer<Boolean> snapshotCommitFinishConsumer = b -> { };
        static volatile boolean restoredFromSnapshot = false;

        SnapshotInstrumentationP(int numItems) {
            this.numItems = numItems;
        }

        /**
         * Reset global processor settings and state to default values
         */
        public static void reset() {
            savedCounters.clear();
            restoredCounters.clear();

            allowedSnapshotsCount = 0;
            saveSnapshotConsumer = null;
            snapshotCommitPrepareConsumer = null;
            snapshotCommitFinishConsumer = b -> { };
            restoredFromSnapshot = false;
        }

        @Override
        protected void init(@Nonnull Context context) {
            globalIndex = context.globalProcessorIndex();
        }

        @Override
        public boolean complete() {
            // emit current snapshot counter
            // finish when requested number of items was emitted
            return tryEmit(10000 * (globalIndex + 1) + snapshotCounter)
                    && snapshotCounter == numItems;
        }

        @Override
        public boolean saveToSnapshot() {
            if (saveSnapshotConsumer != null && snapshotCounter >= allowedSnapshotsCount) {
                saveSnapshotConsumer.accept(globalIndex);
            }
            savedCounters.put(globalIndex, snapshotCounter);
            return tryEmitToSnapshot(broadcastKey(globalIndex), snapshotCounter);
        }

        @Override
        public boolean snapshotCommitPrepare() {
            if (snapshotCommitPrepareConsumer != null && snapshotCounter >= allowedSnapshotsCount) {
                snapshotCommitPrepareConsumer.accept(globalIndex);
            }
            return true;
        }

        @Override
        public boolean snapshotCommitFinish(boolean success) {
            if (snapshotCounter++ >= allowedSnapshotsCount) {
                snapshotCommitFinishConsumer.accept(success);
            }
            return true;
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            if (key instanceof BroadcastKey && ((BroadcastKey<?>) key).key().equals(globalIndex)) {
                restoredCounters.put(globalIndex, (Integer) value);
                // Note that counter will not be incremented before first snapshot after restore.
                // snapshotCounter can be less than snapshot id if some snapshots are lost.
                snapshotCounter = (int) value;
            }
            restoredFromSnapshot = true;
        }
    }

}
