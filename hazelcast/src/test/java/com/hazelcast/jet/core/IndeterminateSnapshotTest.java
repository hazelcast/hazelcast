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
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequestOffloadable;
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
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.PacketFiltersUtil;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.internal.stubbing.answers.ThrowsException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.jet.impl.JobRepository.JOB_EXECUTION_RECORDS_MAP_NAME;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
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

@RunWith(Enclosed.class)
public class IndeterminateSnapshotTest {

    public static final int NODE_COUNT = 5;
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

        private int getActiveNodesCount() {
            int terminatedCount = (instances != null && instances[0].getLifecycleService().isRunning()) ? 0 : 1;
            return NODE_COUNT - terminatedCount;
        }

        protected void initSnapshotDoneCounter() {
            snapshotDone = new CountDownLatch(getActiveNodesCount() * LOCAL_PARALLELISM);
        }

        /**
         * Returns a consumer that will pass on to the `delegate` on the first
         * call. Subsequent calls do nothing, except that they block until the
         * first call is complete, if they are concurrent.
         *
         * @param delegate consumer that should be invoked once
         * @return wrapping consumer
         */
        protected static <T> Consumer<T> firstExecutionConsumer(Consumer<T> delegate) {
            boolean[] executed = {false};
            return (arg) -> {
                synchronized (executed) {
                    if (!executed[0]) {
                        executed[0] = true;
                        delegate.accept(arg);
                    }
                }
            };
        }

        /**
         * Returns a consumer that will pass on to the `delegate` consumer on the last
         * execution. It assumes that there will be {@link #LOCAL_PARALLELISM} invocations
         * on each active node.
         *
         * @param delegate consumer that should be invoked once
         * @return wrapping consumer
         */
        protected <T> Consumer<T> lastExecutionConsumer(Consumer<T> delegate) {
            AtomicInteger executed = new AtomicInteger();
            return (idx) -> {
                int currentExecutionCount = executed.incrementAndGet();
                int totalExecutionCount = getActiveNodesCount() * LOCAL_PARALLELISM;
                assert currentExecutionCount <= totalExecutionCount;
                if (currentExecutionCount == totalExecutionCount) {
                    // this is the last execution
                    delegate.accept(idx);
                }
            };
        }

        protected <T> Consumer<T> breakSnapshotConsumer(String message, Runnable snapshotAction) {
            return firstExecutionConsumer((idx) -> {
                logger.info("Breaking replication in " + message + " for " + idx);
                snapshotAction.run();
                logger.finest("Proceeding with " + message + " " + idx + " after breaking replication");
            });
        }

        protected void waitForSnapshot() {
            logger.info("Waiting for selected snapshot");
            try {
                assertTrue(snapshotDone.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                fail("Interrupted", e);
            }
            logger.info("Got selected snapshot");
        }

        protected void assertSnapshotNotCommitted() {
            assertThat(snapshotDone.getCount())
                    .as("Snapshot must not be committed when indeterminate")
                    .isEqualTo(getActiveNodesCount() * LOCAL_PARALLELISM);
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
         * @param snapshotId snapshot id for the last snapshot (including in progress),
         *                   or -1 if no snapshot is expected
         */
        protected static void assumeLastSnapshotPresent(int snapshotId) {
            if (snapshotId >= 0) {
                assumeThat(SnapshotInstrumentationP.savedCounters.values())
                        .as("Current snapshot is different than expected")
                        .containsOnly(snapshotId);
            } else {
                assumeThat(SnapshotInstrumentationP.savedCounters)
                        .as("Unexpected snapshot")
                        .isEmpty();
            }
        }

        protected static void assertJobNotRestarted() {
            assertThat(SnapshotInstrumentationP.executions)
                    .as("Job should not be restarted during test")
                    .hasSize(1);
        }

        protected static void assertJobRestarted() {
            assertThat(SnapshotInstrumentationP.executions)
                    .as("Job should be restarted during test")
                    .hasSizeGreaterThan(1);
        }

        @Nonnull
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
     * and sometimes it is hard to get correct partition assignment.
     */
    // this test needs mock network
    @Category({SlowTest.class, ParallelJVMTest.class})
    @RunWith(HazelcastSerialClassRunner.class)
    public static class ReplicationBreakingTests extends IndeterminateSnapshotTestBase {
        // the instance indexes which store the primary and backup copy of the SnapshotValidationRecord
        private int masterKeyPartitionInstanceIdx;
        private int backupKeyPartitionInstanceIdx;
        // the instance indexes which store the primary and backup copy of JobRecord/JobExecutionRecord/JobResult
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

        protected void setupJetTests(int allowedSnapshotsCount) {
            SnapshotInstrumentationP.allowedSnapshotsCount = allowedSnapshotsCount;
            SnapshotInstrumentationP.snapshotCommitFinishConsumer = success -> {
                // allow normal snapshot after job restart
                SnapshotInstrumentationP.saveSnapshotConsumer = null;
                SnapshotInstrumentationP.snapshotCommitPrepareConsumer = null;
                snapshotDone.countDown();
            };

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

            disableBackupsFrom(masterPartitionInstanceIdx);

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
            disableBackupsFrom(masterPartitionInstanceIdx);

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
        public void whenFirstSnapshotPossiblyCorrupted_thenRestartWithoutSnapshot() {
            when_shutDown(0);
        }

        @Test
        public void whenNextSnapshotPossiblyCorrupted_thenRestartFromLastGoodSnapshot() {
            when_shutDown(3);
        }

        private void setupPartitions() {
            InternalPartition partitionForKey = getPartitionForKey(SnapshotValidationRecord.KEY);
            logger.info("SVR KEY partition id: " + partitionForKey.getPartitionId());
            for (int i = 0; i < 3; ++i) {
                logger.info("SVR KEY partition addr: " + partitionForKey.getReplica(i));
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

        private void when_shutDown(int allowedSnapshotsCount) {
            // We need to keep replicated SVR KEY and JobExecutionRecord for jobId.
            // Each can be in a different partition that can be on a different instance
            // (primary and backup), so we need to have at least 5 nodes, so one
            // of them can be broken to corrupt the snapshot data, but not affect job metadata
            // (JobExecutionRecord and SnapshotValidationRecord).
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

            waitForSnapshot();
            sleepMillis(500);

            logger.info("Shutting down instance... " + failingInstance);
            instances[failingInstance].getLifecycleService().terminate();
            logger.info("Removed instance " + failingInstance + " from cluster");
            // restore network between remaining nodes
            restoreNetwork();

            logger.info("Joining job...");
            instances[liveInstance].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            assertRestoredFromSnapshot(allowedSnapshotsCount - 1);
        }

        // repeat test to first success. @Repeat does not recreate test class instance.
        private boolean test1Succeeded;

        @Test
        @Repeat(5)
        public void whenFirstSnapshotPossiblyCorruptedAfter1stPhase_thenRestartWithoutSnapshot() {
            assumeThat(test1Succeeded).as("Test already succeeded").isFalse();
            when_shutDownAfter1stPhase(0);
            test1Succeeded = true;
        }

        private boolean test2Succeeded;

        @Test
        @Repeat(5)
        public void whenNextSnapshotPossiblyCorruptedAfter1stPhase_thenRestartFromLastGoodSnapshot() {
            assumeThat(test2Succeeded).as("Test already succeeded").isFalse();
            when_shutDownAfter1stPhase(3);
            test2Succeeded = true;
        }

        private void when_shutDownAfter1stPhase(int allowedSnapshotsCount) {
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

            int failingInstance = chooseConstantFailingInstance(masterKeyPartitionInstanceIdx);
            int liveInstance = backupKeyPartitionInstanceIdx;

            // ensure that job started
            assertJobStatusEventually(job, JobStatus.RUNNING);

            waitForSnapshot();
            sleepMillis(500);

            logger.info("Shutting down instance... " + failingInstance);
            instances[failingInstance].getLifecycleService().terminate();
            logger.info("Removed instance " + failingInstance + " from cluster");
            // restore network between remaining nodes
            restoreNetwork();

            logger.info("Joining job...");
            instances[liveInstance].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            assertRestoredFromSnapshot(allowedSnapshotsCount - 1);
        }

        @Nonnull
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
            Set<Integer> nodes = IntStream.range(0, NODE_COUNT).boxed().collect(Collectors.toSet());
            // note that `safeNodes` must be `Integer` not `int` so asList does not return List<int[]>
            Arrays.asList(safeNodes).forEach(nodes::remove);
            logger.info("Replicas that can be broken: " + nodes);
            assertThat(nodes).isNotEmpty();
            int failingInstance = nodes.stream().findAny().get();
            return chooseConstantFailingInstance(failingInstance);
        }

        private int chooseConstantFailingInstance(int failingInstance) {
            logger.info("Failing instance selected: " + failingInstance);
            failingInstanceFuture.complete(failingInstance);
            return failingInstance;
        }

        /**
         * Sets a packet filter that drops all backups from the instance in
         * {@link #failingInstanceFuture}.
         */
        private void breakFailingInstance() {
            Integer failingInstanceIdx;
            try {
                failingInstanceIdx = failingInstanceFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            disableBackupsFrom(failingInstanceIdx);
        }

        private void restoreNetwork() {
            for (int i = 0; i < NODE_COUNT; ++i) {
                if (instances[i].getLifecycleService().isRunning()) {
                    resetPacketFiltersFrom(instances[i]);
                }
            }
        }

        //region MapBackupPacketDropFilter

        /**
         * Installs a packet filter to all instances. The filter will drop
         * {@link PartitionReplicaSyncRequestOffloadable} operation on all
         * members, and will drop the {@link Backup} operation only on the
         * member with `noBackupInstanceIndex`.
         */
        private void disableBackupsFrom(int noBackupInstanceIndex) {
            for (int i = 0; i < instances.length; i++) {
                PacketFiltersUtil.setCustomFilter(instances[i],
                        new MapBackupPacketDropFilter(instances[i], noBackupInstanceIndex == i ? 1 : 0));
            }
        }

        private static class MapBackupPacketDropFilter extends OperationPacketFilter {
            private final ILogger logger = Logger.getLogger(getClass());

            private final Address sourceAddress;
            private final float blockRatio;

            /**
             * @param blockRatio 0 - block none, 1 - block all, 0.5 - block random 50%
             */
            MapBackupPacketDropFilter(HazelcastInstance instance, float blockRatio) {
                super(getNode(instance).getSerializationService());
                this.sourceAddress = instance.getCluster().getLocalMember().getAddress();
                this.blockRatio = blockRatio;
            }

            @Override
            protected Action filterOperation(Address targetAddress, int factory, int type) {
                // REPLICA_SYNC_REQUEST_OFFLOADABLE is sent when backup replica detects
                // that is it out-of-date because there is a newer version in partition table
                if (factory == PartitionDataSerializerHook.F_ID
                        && type == PartitionDataSerializerHook.REPLICA_SYNC_REQUEST_OFFLOADABLE) {
                    return Action.DROP;
                }

                if (factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP) {
                    Action action = (Math.random() >= blockRatio ? Action.ALLOW : Action.DROP);
                    logger.info(sourceAddress + " sending backup packet to " + targetAddress + ", action: " + action);
                    return action;
                }

                // allow the rest
                return Action.ALLOW;
            }
        }

        //endregion
    }

    /**
     * These tests use simplified approach to simulating IMap failure. Instead
     * of trying to terminate appropriate node on given time they inject
     * erroneous IMap operation responses. This is much simpler and allows to
     * test more scenarios. However, tests terminating nodes are also useful as
     * they resemble reality more.
     */
    @Category({SlowTest.class, ParallelJVMTest.class})
    @RunWith(HazelcastParametrizedRunner.class)
    @UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
    public static class SnapshotFailureTests extends IndeterminateSnapshotTestBase {

        @Parameter
        public boolean suspendOnFailure;

        @Parameters(name = "suspendOnFailure:{0}")
        public static Object[] parameters() {
            return new Object[] { false, true };
        }

        @Override
        protected JobConfig customizeJobConfig(JobConfig config) {
            return config.setSuspendOnFailure(suspendOnFailure);
        }

        // fixes problem with mock deserialization on cluster side.
        // probably registers generated mockito classes in classloader that is shared with the HZ instances.
        @SuppressWarnings("unused")
        private final MapInterceptor registerMockInClassloader = mock(MapInterceptor.class, withSettings().serializable());

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

            new SuccessfulSnapshots(initialSnapshotsCount)
                    .then(new IndeterminateLostPut())
                    .then(new SuccessfulIgnoredSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);

            logger.info("Joining job...");
            instances[0].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            assertRestoredFromSnapshot(initialSnapshotsCount);
        }

        @Test
        public void whenSnapshotUpdateLostChangedCoordinatorNoOtherSnapshot_thenRestartWithoutSnapshot() {
            whenSnapshotUpdateLostChangedCoordinator(0);
        }

        @Test
        public void whenSnapshotUpdateLostChangedCoordinator_thenRestartFromLastGoodSnapshot() {
            whenSnapshotUpdateLostChangedCoordinator(3);
        }

        private void whenSnapshotUpdateLostChangedCoordinator(int initialSnapshotsCount) {
            // JobExecutionRecord update during snapshot commit is indeterminate and lost,
            // and is indeterminate until original coordinator is terminated.
            // Job is restarted on a different coordinator and indeterminate snapshot turns out to be lost.

            CompletableFuture<Void> coordinatorTerminated = new CompletableFuture<>();

            new SuccessfulSnapshots(initialSnapshotsCount)
                    // snapshot during suspend will be indeterminate until coordinator is terminated
                    .then(new IndeterminateLostPutsUntil(coordinatorTerminated))
                    // will be restored from previous snapshot, reuse counter
                    .then(new SuccessfulSnapshots(1).reusedCounter())
                    .then(new SuccessfulIgnoredSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);
            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                waitForSnapshot();
            }

            // wait for job restart
            logger.info("Waiting for job restart...");
            assertJobStatusEventually(job, JobStatus.STARTING);
            assertSnapshotNotCommitted();

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
        public void whenSuspendSnapshotUpdateLostSameCoordinatorAndNoOtherSnapshots_thenRestartFromSuspendSnapshot() {
            whenSuspendSnapshotUpdateLostSameCoordinator(0);
        }

        @Test
        public void whenSuspendSnapshotUpdateLostSameCoordinator_thenRestartFromSuspendSnapshot() {
            whenSuspendSnapshotUpdateLostSameCoordinator(3);
        }

        /**
         * @param initialSnapshotsCount number of snapshots before suspend
         */
        private void whenSuspendSnapshotUpdateLostSameCoordinator(int initialSnapshotsCount) {
            // Suspend operation is initiated.
            // JobExecutionRecord update during snapshot commit is indeterminate and lost,
            // but succeeds after job restart on the same coordinator.
            // Indeterminate snapshot turns out to be correct.

            new SuccessfulSnapshots(initialSnapshotsCount)
                    .then(new IndeterminateLostPut())
                    .then(new SuccessfulIgnoredSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);

            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                // wait for initial successful snapshots
                waitForSnapshot();
            }

            // There is a race between suspend and regular snapshots,
            // but we want the test to be executed when there is no snapshot in progress.
            // Try to detect slow execution and skip test if there is unexpected snapshot.
            assumeLastSnapshotPresent(initialSnapshotsCount - 1);

            // suspend should fail and job should be restarted
            job.suspend();

            logger.info("Joining job...");
            instances[0].getJet().getJob(job.getId()).join();
            logger.info("Joined");

            // indeterminate suspend snapshot turns out to be fine
            assertRestoredFromSnapshot(initialSnapshotsCount);
            assertJobRestarted();
        }

        @Test
        public void whenSuspendSnapshotUpdateLostChangedCoordinatorAndNoOtherSnapshots_thenRestartWithoutSnapshot() {
            whenSuspendSnapshotUpdateLostChangedCoordinator(0);
        }

        @Test
        public void whenSuspendSnapshotUpdateLostChangedCoordinator_thenRestartFromRegularSnapshot() {
            whenSuspendSnapshotUpdateLostChangedCoordinator(3);
        }

        private void whenSuspendSnapshotUpdateLostChangedCoordinator(int initialSnapshotsCount) {
            // Suspend operation is initiated.
            // JobExecutionRecord update during snapshot commit is indeterminate and lost.
            // Job restarted on different coordinator.
            // Indeterminate snapshot is lost.

            CompletableFuture<Void> coordinatorTerminated = new CompletableFuture<>();

            new SuccessfulSnapshots(initialSnapshotsCount)
                    // snapshot during suspend will be indeterminate until coordinator is terminated
                    .then(new IndeterminateLostPutsUntil(coordinatorTerminated))
                    .then(new SuccessfulIgnoredSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);

            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                // wait for initial successful snapshots
                waitForSnapshot();
            }

            job.suspend();
            // suspend will fail (be indeterminate) and job will be restarted,
            // but after restart it will still be indeterminate
            assertJobStatusEventually(job, JobStatus.STARTING);
            logger.info("Suspend failed and job restarted");
            assertSnapshotNotCommitted();

            instances[0].getLifecycleService().terminate();
            coordinatorTerminated.complete(null);

            job = instances[1].getJet().getJob(job.getId());
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

            new SuccessfulSnapshots(initialSnapshotsCount)
                    .then(new IndeterminateLostPut())
                    .then(new SuccessfulIgnoredSnapshots(100))
                    .start();

            Job job = createJob(initialSnapshotsCount + 3);
            assertJobStatusEventually(job, JobStatus.RUNNING);

            if (initialSnapshotsCount > 0) {
                snapshotDone.await();
            }

            // there is a race between suspend and regular snapshots,
            // but we want the test to be executed when there is no snapshot in progress
            // try to detect slow execution and skip test if there is unexpected snapshot
            assumeLastSnapshotPresent(initialSnapshotsCount - 1);
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

        // region Snapshot scenarios

        /**
         * Base class for defining snapshot failure scenarios. Allows to
         * precisely inject failure at a correct moment. The scenario is defined
         * at the beginning of the test and runs automatically afterwards after
         * it is started using {@link #start()}.
         * <p>
         * Scenario steps use {@link SnapshotInstrumentationP} to inject
         * failures at appropriate stages of processing.
         */
        private abstract class AbstractScenarioStep {
            @Nullable
            private AbstractScenarioStep next;
            @Nullable
            protected AbstractScenarioStep prev;
            /**
             * Null means a step that adds some action, but does not increase the
             * number of completed snapshots
             */
            @Nullable
            private Integer repetitions = 1;

            public AbstractScenarioStep repeat(@Nullable Integer repetitions) {
                if (repetitions != null && repetitions < 0) {
                    throw new IllegalArgumentException("must be >0, but is " + repetitions);
                }
                this.repetitions = repetitions;
                return this;
            }

            public <T extends AbstractScenarioStep> T then(T next) {
                this.next = next;
                next.prev = this;
                return next;
            }

            /**
             * Configure {@link SnapshotInstrumentationP} according to the
             * definition of this step.
             */
            public final void apply() {
                if (repetitions == null || repetitions > 0) {
                    logger.info("Applying scenario step " + this);
                    if (repetitions != null) {
                        boolean first = SnapshotInstrumentationP.allowedSnapshotsCount == 0;
                        SnapshotInstrumentationP.allowedSnapshotsCount += repetitions;
                        if (first) {
                            // snapshot ids start from 0
                            // adjust the counter so repetitions are intuitive
                            SnapshotInstrumentationP.allowedSnapshotsCount--;
                        }
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
                    assert repetitions == 0;
                    logger.info("Skipping scenario step " + this);
                    goToNextStep();
                }
            }

            protected abstract void doApply();

            protected void goToNextStep() {
                if (next != null) {
                    next.apply();
                } else {
                    logger.info("End of scenario");
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

            @Nullable
            public Integer getRepetitions() {
                return repetitions;
            }
        }

        /**
         * Allow given number of successful snapshots before proceeding to next step.
         * Proceeds to next step when last snapshot is committed. Updates {@link #snapshotDone}.
         */
        protected class SuccessfulSnapshots extends AbstractScenarioStep {
            SuccessfulSnapshots() {
                super();
            }

            public SuccessfulSnapshots(int repetitions) {
                this();
                repeat(repetitions);
            }

            /**
             * Snapshot counter will be reused. Most often this is the case after restore.
             */
            public SuccessfulSnapshots reusedCounter() {
                repeat(getRepetitions() > 1 ? getRepetitions() - 1 : null);
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
                return String.format("Successful %d Snapshots", getRepetitions());
            }
        }

        /**
         * Allow given number of successful snapshots before proceeding to the next
         * step. Proceeds to next step when last snapshot is committed. Does
         * not update {@link #snapshotDone}.
         */
        protected class SuccessfulIgnoredSnapshots extends AbstractScenarioStep {
            SuccessfulIgnoredSnapshots() {
                super();
            }

            public SuccessfulIgnoredSnapshots(int repetitions) {
                this();
                repeat(repetitions);
            }

            @Override
            protected void doApply() {
                // nothing to break
            }

            @Override
            public String toString() {
                return String.format("Successful %d Snapshots - do not count", getRepetitions());
            }
        }

        /**
         * Inject 1 lost indeterminate JobExecutionRecord IMap update after
         * last snapshot chunk is saved and go to next step after injecting the
         * error.
         */
        protected class IndeterminateLostPut extends AbstractScenarioStep {
            public IndeterminateLostPut() {
                super();
                repeat(null);
            }

            @Override
            protected void doApply() {
                initSnapshotDoneCounter();
                // break update of JobExecutionRecord after last snapshot chunk is saved
                SnapshotInstrumentationP.saveSnapshotConsumer =
                        SnapshotFailureTests.this.<Integer>
                                        breakSnapshotConsumer("snapshot save",
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
         * Inject indeterminate JobExecutionRecord IMap updates until condition is satisfied.
         * Go to next step after the condition is satisfied and IMap updates are restored.
         */
        protected class IndeterminateLostPutsUntil extends AbstractScenarioStep {

            private final CompletableFuture<String> registration = new CompletableFuture<>();

            /**
             * @param condition Future indicating when IMap puts should be fixed. It will happen
             *                  when the future is completed.
             */
            public <T> IndeterminateLostPutsUntil(CompletableFuture<T> condition) {
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

        //endregion
    }

    /**
     * Processor that provides hooks for snapshot taking stages and tracks data
     * saved to and restored from snapshots.
     */
    @Ignore("test processor used by EE tests")
    public static final class SnapshotInstrumentationP extends AbstractProcessor {
        static final ConcurrentMap<Integer, Integer> savedCounters = new ConcurrentHashMap<>();
        static final ConcurrentMap<Integer, Integer> restoredCounters = new ConcurrentHashMap<>();
        static final Set<Long> executions = ConcurrentHashMap.newKeySet();

        private int globalIndex;
        private final int numItems;
        /**
         * Number of completed snapshots.
         */
        private int snapshotCounter;
        /**
         * ID of the first snapshot for which {@link #saveSnapshotConsumer} and other hooks are called.
         * Snapshot ids are assigned starting with 0.
         */
        public static volatile int allowedSnapshotsCount = 0;
        @Nullable
        public static volatile Consumer<Integer> saveSnapshotConsumer;
        @Nullable
        public static volatile Consumer<Integer> snapshotCommitPrepareConsumer;
        @Nonnull
        public static volatile Consumer<Boolean> snapshotCommitFinishConsumer = b -> { };
        public static volatile boolean restoredFromSnapshot = false;

        SnapshotInstrumentationP(int numItems) {
            this.numItems = numItems;
        }

        /**
         * Reset global processor settings and state to default values
         */
        public static void reset() {
            savedCounters.clear();
            restoredCounters.clear();
            executions.clear();

            allowedSnapshotsCount = 0;
            saveSnapshotConsumer = null;
            snapshotCommitPrepareConsumer = null;
            snapshotCommitFinishConsumer = b -> { };
            restoredFromSnapshot = false;
        }

        @Override
        protected void init(@Nonnull Context context) {
            globalIndex = context.globalProcessorIndex();
            executions.add(context.executionId());
        }

        @Override
        public boolean complete() {
            // emit current snapshot counter
            // finish when the requested number of items was emitted
            return tryEmit(10000 * (globalIndex + 1) + snapshotCounter)
                    && snapshotCounter == numItems;
        }

        @Override
        public boolean saveToSnapshot() {
            if (!tryEmitToSnapshot(broadcastKey(globalIndex), snapshotCounter)) {
                return false;
            }
            if (saveSnapshotConsumer != null && snapshotCounter >= allowedSnapshotsCount) {
                saveSnapshotConsumer.accept(globalIndex);
            }
            savedCounters.put(globalIndex, snapshotCounter);
            return true;
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
