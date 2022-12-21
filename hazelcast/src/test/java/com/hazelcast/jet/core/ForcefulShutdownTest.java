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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
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
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.throttle;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// TODO: make this SlowTest? This test needs mock network
@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class ForcefulShutdownTest extends JetTestSupport {

    private static final int NODE_COUNT = 5;
    private static final int BASE_PORT = 5701;
    // increase number of chunks in snapshot by setting greater parallelism
    private static final int LOCAL_PARALLELISM = 10;

    private HazelcastInstance[] instances;

    // SnapshotVerificationKey
    private int masterKeyPartitionInstanceIdx;
    private int backupKeyPartitionInstanceIdx;
    private final CountDownLatch snapshotDone = new CountDownLatch(NODE_COUNT * LOCAL_PARALLELISM);
    // Job
    private Integer masterJobPartitionInstanceIdx;
    private Integer backupJobPartitionInstanceIdx;

    private final CompletableFuture<Integer> failingInstanceFuture = new CompletableFuture<>();

    @Override
    public Config getConfig() {
        Config config = smallInstanceConfig();
        // slow down anti-entropy process, so it does not kick in during test
        config.setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "120");
        // TODO: uncomment to test workaround
//        config.setProperty(ClusterProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), "true");
        return config;
    }

    @Before
    public void setup() {
        instances = createHazelcastInstances(getConfig(), NODE_COUNT);
        SnapshotInstrumentationP.reset();
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

    @Test
    public void whenFirstSnapshotPossiblyCorrupted_thenRestartWithoutSnapshot() throws InterruptedException {
        when_shutDown(true, 0);
    }

    @Test
    public void whenNextSnapshotPossiblyCorrupted_thenRestartFromLastGoodSnapshot() throws InterruptedException {
        when_shutDown(true, 3);
    }

    private void setupJetTests(int allowedSnapshotsCount) {
        InternalPartition partitionForKey = getPartitionForKey(SnapshotValidationRecord.KEY);
        logger.info("SVR KEY partition id:" + partitionForKey.getPartitionId());
        for (int i = 0; i < 3; ++i) {
            logger.info("SVR KEY partition addr:" + partitionForKey.getReplica(i));
        }
        masterKeyPartitionInstanceIdx = partitionForKey.getReplica(0).address().getPort() - BASE_PORT;
        backupKeyPartitionInstanceIdx = partitionForKey.getReplica(1).address().getPort() - BASE_PORT;

        SnapshotInstrumentationP.allowedSnapshotsCount = allowedSnapshotsCount;
        SnapshotInstrumentationP.snapshotCommitFinishConsumer = success -> {
            // allow normal snapshot after job restart
            SnapshotInstrumentationP.saveSnapshotConsumer = null;
            SnapshotInstrumentationP.snapshotCommitPrepareConsumer = null;
            snapshotDone.countDown();
        };
    }

    private <K> InternalPartition getPartitionForKey(K key) {
        // to determine partition we can use any instance (let's pick the first one)
        HazelcastInstance instance = instances[0];
        InternalPartitionService partitionService = getNode(instance).getPartitionService();
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartition(partitionId);
    }

    private void when_shutDown(boolean snapshotted, int allowedSnapshotsCount) throws InterruptedException {

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

        AtomicBoolean networkBroken = new AtomicBoolean(false);
        SnapshotInstrumentationP.saveSnapshotConsumer = (idx) -> {
            // synchronize so all snapshot chunk operations have broken replication
            synchronized (networkBroken) {
                if (networkBroken.compareAndSet(false, true)) {
                    logger.info("Breaking replication in snapshot for " + idx);
                    breakFailingInstance();
                    logger.finest("Proceeding with snapshot idx " + idx + " after breaking replication");
                } else {
                    logger.finest("Proceeding with snapshot idx " + idx);
                }
            }
        };

        // dummy job
        // start job after SnapshotInstrumentationP setup to avoid race
        Job job = createJob(snapshotted, numItems);

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
//        instances[liveInstance].getJet().getJob(job.getId()).join();
        assertThatThrownBy(() -> instances[liveInstance].getJet().getJob(job.getId()).join())
                .hasMessageContainingAll("State for job", "is corrupted: it should have");
        logger.info("Joined");

        // TODO: this check will make sense after fix
        if (allowedSnapshotsCount > 0) {
            assertTrue("Should be restored from snapshot", SnapshotInstrumentationP.restoredFromSnapshot);
            assertThat(SnapshotInstrumentationP.restoredCounters.values())
                    .as("Should restore from last known good snapshot")
                    .containsOnly(allowedSnapshotsCount - 1);
        } else {
            assertFalse("Should not be restored from snapshot", SnapshotInstrumentationP.restoredFromSnapshot);
        }
    }

    @Test
    public void whenFirstSnapshotPossiblyCorruptedAfter1stPhase_thenRestartWithoutSnapshot() throws InterruptedException {
        when_shutDownAfter1stPhase(true, 0);
    }

    @Test
    public void whenNextSnapshotPossiblyCorruptedAfter1stPhase_thenRestartFromLastGoodSnapshot() throws InterruptedException {
        when_shutDownAfter1stPhase(true, 3);
    }

    private void when_shutDownAfter1stPhase(boolean snapshotted, int allowedSnapshotsCount) throws InterruptedException {

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

        AtomicBoolean networkBroken = new AtomicBoolean(false);
        SnapshotInstrumentationP.snapshotCommitPrepareConsumer = (idx) -> {
            if (networkBroken.compareAndSet(false, true)) {
                logger.info("Breaking replication in snapshot commit prepare for " + idx);
                breakFailingInstance();
                logger.finest("Proceeding with snapshot commit prepare idx " + idx + " after breaking replication");
            } else {
                logger.finest("Proceeding with snapshot commit prepare idx " + idx);
            }
        };

        // dummy job
        // start job after SnapshotInstrumentationP setup to avoid race
        Job job = createJob(snapshotted, numItems);

        // choose failing node
        // TODO: due to these constraints this test cannot be executed 2 times out of NODE_COUNT times.
        // they are necessary for the test to be more deterministic but jobId is random.
        assertThat(masterKeyPartitionInstanceIdx)
                // Keep job data, in particular JobExecutionRecord safe
                .as("Should not damage job data").isNotEqualTo(masterJobPartitionInstanceIdx)
                // To not restart Jet master (TODO: this could be another test)
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
//         instances[liveInstance].getJet().getJob(job.getId()).join();
        assertThatThrownBy(() -> instances[liveInstance].getJet().getJob(job.getId()).join())
                .hasMessageContainingAll("snapshot with ID", "is damaged. Unable to restore the state for job");
        logger.info("Joined");

        // TODO: this check will make sense after fix
        if (allowedSnapshotsCount > 0) {
            assertTrue("Should be restored from snapshot", SnapshotInstrumentationP.restoredFromSnapshot);
            assertThat(SnapshotInstrumentationP.restoredCounters.values())
                    .as("Should restore from last known good snapshot")
                    .containsOnly(allowedSnapshotsCount - 1);
        } else {
            assertFalse("Should not be restored from snapshot", SnapshotInstrumentationP.restoredFromSnapshot);
        }
    }

    @NotNull
    private Job createJob(boolean snapshotted, int numItems) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", throttle(() -> new SnapshotInstrumentationP(numItems), 1)).localParallelism(LOCAL_PARALLELISM);
        Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLoggerP()).localParallelism(1);
        dag.edge(between(source, sink));

        Job job = instances[0].getJet().newJob(dag, new JobConfig()
                .setProcessingGuarantee(snapshotted ? EXACTLY_ONCE : NONE)
                // trigger snapshot often to speed up test
                .setSnapshotIntervalMillis(1000));

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

    private void waitForCorruptedSnapshot() throws InterruptedException {
        logger.info("Waiting for corrupted snapshot");
        assertTrue(snapshotDone.await(30, TimeUnit.SECONDS));
        logger.info("Got corrupted snapshot");
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
        private int snapshotCounter;

        /**
         * Number of normal snapshots before {@link #saveSnapshotConsumer} is called.
         * Resets on job reset/restore.
         */
        static volatile int allowedSnapshotsCount = 0;
        @Nullable
        static volatile Consumer<Integer> saveSnapshotConsumer;
        @Nullable
        static volatile Consumer<Integer> snapshotCommitPrepareConsumer;
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
            tryEmit(10000 * (globalIndex + 1) + snapshotCounter);
            return snapshotCounter == numItems;
        }

        @Override
        public boolean saveToSnapshot() {
            if (saveSnapshotConsumer != null) {
                if (snapshotCounter >= allowedSnapshotsCount) {
                    saveSnapshotConsumer.accept(globalIndex);
                }
            }
            savedCounters.put(globalIndex, snapshotCounter);
            return tryEmitToSnapshot(broadcastKey(globalIndex), snapshotCounter);
        }

        @Override
        public boolean snapshotCommitPrepare() {
            if (snapshotCommitPrepareConsumer != null) {
                if (snapshotCounter >= allowedSnapshotsCount) {
                    snapshotCommitPrepareConsumer.accept(globalIndex);
                }
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
                snapshotCounter = (int) value;
            }
            getLogger().info("EmitIntegersP got from snapshot: " + value + " for " + key);
            restoredFromSnapshot = true;
        }
    }

}
