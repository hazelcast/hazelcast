/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.impl.MigrationManagerImpl;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.partition.PartitionMigrationListenerTest;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.ASSIGN_PARTITIONS;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.FETCH_PARTITION_STATE;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.F_ID;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PARTITION_STATE_OP;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PROMOTION_COMMIT;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationEventsConsistentWithResult;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationProcessCompleted;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationProcessEventsConsistent;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_MAX_PARALLEL_PROMOTION_BATCHES;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MigrationInvocationsSafetyTest extends PartitionCorrectnessTestSupport {

    @Before
    public void setupParams() {
        nodeCount = 4;
        backupCount = nodeCount - 1;
    }

    @Test
    public void members_shouldAgree_onPartitionTable_whenMasterChanges() {
        HazelcastInstance initialMaster = factory.newHazelcastInstance();
        HazelcastInstance nextMaster = factory.newHazelcastInstance();
        HazelcastInstance slave1 = factory.newHazelcastInstance();
        HazelcastInstance slave2 = factory.newHazelcastInstance();
        HazelcastInstance slave3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(5, nextMaster, slave1, slave2, slave3);

        // nextMaster & slave1 won't receive partition table updates from initialMaster.
        dropOperationsBetween(initialMaster, asList(slave1, nextMaster), F_ID, singletonList(PARTITION_STATE_OP));
        dropOperationsBetween(nextMaster, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));
        dropOperationsBetween(slave1, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));

        ensurePartitionsInitialized(initialMaster, slave2, slave3);

        long initialPartitionStateStamp = getPartitionService(initialMaster).getPartitionStateStamp();
        assertEquals(initialPartitionStateStamp, getPartitionService(slave2).getPartitionStateStamp());
        assertEquals(initialPartitionStateStamp, getPartitionService(slave3).getPartitionStateStamp());
        assertEquals(0, getPartitionService(nextMaster).getPartitionStateStamp());
        assertEquals(0, getPartitionService(slave1).getPartitionStateStamp());

        dropOperationsBetween(nextMaster, slave3, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);

        spawn(() -> {
            assertClusterSizeEventually(4, nextMaster, slave1, slave2, slave3);
            sleepSeconds(10);
            resetPacketFiltersFrom(nextMaster);
        });

        assertTrueEventually(() -> {
            long nextPartitionStateStamp = getPartitionService(nextMaster).getPartitionStateStamp();
            assertNotEquals(nextPartitionStateStamp, initialPartitionStateStamp);

            assertEquals(nextPartitionStateStamp, getPartitionService(slave1).getPartitionStateStamp());
            assertEquals(nextPartitionStateStamp, getPartitionService(slave2).getPartitionStateStamp());
            assertEquals(nextPartitionStateStamp, getPartitionService(slave3).getPartitionStateStamp());
        });

    }

    @Test
    public void members_shouldAgree_onPartitionTable_whenMasterChanges_and_anotherMemberCrashes() {
        HazelcastInstance initialMaster = factory.newHazelcastInstance();
        HazelcastInstance nextMaster = factory.newHazelcastInstance();
        HazelcastInstance slave1 = factory.newHazelcastInstance();
        HazelcastInstance slave2 = factory.newHazelcastInstance();
        HazelcastInstance slave3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(5, nextMaster, slave1, slave2, slave3);

        // nextMaster & slave1 won't receive partition table updates from initialMaster.
        dropOperationsBetween(initialMaster, asList(slave1, nextMaster), F_ID, singletonList(PARTITION_STATE_OP));
        dropOperationsBetween(nextMaster, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));
        dropOperationsBetween(slave1, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));

        ensurePartitionsInitialized(initialMaster, slave2, slave3);

        long initialPartitionStateStamp = getPartitionService(initialMaster).getPartitionStateStamp();
        assertEquals(initialPartitionStateStamp, getPartitionService(slave2).getPartitionStateStamp());
        assertEquals(initialPartitionStateStamp, getPartitionService(slave3).getPartitionStateStamp());
        assertEquals(0, getPartitionService(nextMaster).getPartitionStateStamp());
        assertEquals(0, getPartitionService(slave1).getPartitionStateStamp());

        dropOperationsBetween(nextMaster, slave3, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);

        spawn(() -> {
            assertClusterSizeEventually(4, nextMaster, slave1, slave2, slave3);
            sleepSeconds(10);
            terminateInstance(slave3);
        });

        assertTrueEventually(() -> {
            long nextPartitionStateStamp = getPartitionService(nextMaster).getPartitionStateStamp();
            assertNotEquals(nextPartitionStateStamp, initialPartitionStateStamp);

            assertEquals(nextPartitionStateStamp, getPartitionService(slave1).getPartitionStateStamp());
            assertEquals(nextPartitionStateStamp, getPartitionService(slave2).getPartitionStateStamp());
        });

    }

    @Test
    public void partitionState_shouldNotBeSafe_duringPartitionTableFetch_whenMasterLeaves() {
        partitionState_shouldNotBeSafe_duringPartitionTableFetch_whenMasterChanges(false);
    }

    @Test
    public void partitionState_shouldNotBeSafe_duringPartitionTableFetch_whenLiteMasterLeaves() {
        partitionState_shouldNotBeSafe_duringPartitionTableFetch_whenMasterChanges(true);
    }

    private void partitionState_shouldNotBeSafe_duringPartitionTableFetch_whenMasterChanges(boolean liteMaster) {
        HazelcastInstance initialMaster = factory.newHazelcastInstance(new Config().setLiteMember(liteMaster));
        HazelcastInstance nextMaster = factory.newHazelcastInstance();
        HazelcastInstance slave = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, nextMaster, slave);
        warmUpPartitions(initialMaster, nextMaster, slave);

        dropOperationsBetween(nextMaster, slave, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);
        assertClusterSizeEventually(2, nextMaster, slave);

        assertTrueAllTheTime(() -> {
            assertFalse(getPartitionService(nextMaster).isMemberStateSafe());
            assertFalse(getPartitionService(slave).isMemberStateSafe());
        }, 5);

        resetPacketFiltersFrom(nextMaster);

        assertTrueEventually(() -> {
            assertTrue(getPartitionService(nextMaster).isMemberStateSafe());
            assertTrue(getPartitionService(slave).isMemberStateSafe());
        });
    }

    @Test
    public void migrationCommit_shouldBeRetried_whenTargetNotResponds() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "4000")
                // Allow single migration per-member.
                // This is required to be able to block only migration commit invocations,
                // otherwise we can drop responses of usual migration operations and that breaks the test.
                .setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_MIGRATIONS.getName(), "1");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, slave1, slave2);
        warmUpPartitions(master, slave1, slave2);

        setMigrationCommitTimeoutMillis(master, 5000);

        fillAndAssertData(master);

        // prevent migrations before adding migration listeners when slave3 joins the cluster
        changeClusterStateEventually(master, ClusterState.NO_MIGRATION);

        HazelcastInstance slave3 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(4, slave1, slave2);

        // set migration listener to drop migration commit response after a migration to slave3 is completed
        setMigrationListenerToDropCommitResponse(master, slave3);

        // enable migrations
        changeClusterStateEventually(master, ClusterState.ACTIVE);

        // wait for retry of migration commit
        sleepSeconds(10);

        // reset filters to allow sending migration commit response
        resetPacketFiltersFrom(slave3);

        waitAllForSafeState(master, slave1, slave2, slave3);

        PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(() -> {
            assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
            assertEquals(masterPartitionTable, getPartitionService(slave2).createPartitionTableView());
            assertEquals(masterPartitionTable, getPartitionService(slave3).createPartitionTableView());
        });

        assertSizeAndData();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
        assertNoDuplicateMigrations(slave3);
    }

    private void setMigrationListenerToDropCommitResponse(HazelcastInstance master, HazelcastInstance destination) {
        // intercept migration complete on destination and drop commit response
        getPartitionServiceImpl(destination).setMigrationInterceptor(new MigrationInterceptor() {
            final AtomicReference<MigrationInfo> committedMigrationInfoRef = new AtomicReference<>();

            @Override
            public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
               assertClusterStateEventually(ClusterState.ACTIVE, destination);
            }

            @Override
            public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
                if (getAddress(master).equals(migrationInfo.getSourceAddress())
                        || migrationInfo.getDestinationNewReplicaIndex() != 0) {
                    // Skip the migrations which master is the source or can be the source...
                    // Because we want to block migration commit invocation not the usual migration operations.
                    return;
                }
                if (participant == MigrationParticipant.DESTINATION && committedMigrationInfoRef.compareAndSet(null, migrationInfo)) {
                    dropOperationsBetween(destination, master, SpiDataSerializerHook.F_ID, singletonList(NORMAL_RESPONSE));
                }
            }
        });
    }

    @Test
    public void migrationCommit_shouldRollback_whenTargetCrashes() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "4000");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, slave1, slave2);
        warmUpPartitions(master, slave1, slave2);

        setMigrationCommitTimeoutMillis(master, 5000);

        fillAndAssertData(master);

        // prevent migrations before adding migration listeners when slave3 joins the cluster
        changeClusterStateEventually(master, ClusterState.NO_MIGRATION);

        HazelcastInstance slave3 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(4, slave1, slave2);

        // set migration listener to drop migration commit response after a migration to slave3 is completed
        setMigrationListenerToDropCommitResponse(master, slave3);

        // enable migrations
        changeClusterStateEventually(master, ClusterState.ACTIVE);

        // wait for retry of migration commit
        sleepSeconds(10);

        terminateInstance(slave3);

        waitAllForSafeState(master, slave1, slave2);

        PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(() -> {
            assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
            assertEquals(masterPartitionTable, getPartitionService(slave2).createPartitionTableView());
        });

        assertSizeAndDataEventually();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
    }

    @Test
    public void promotionCommits_shouldRespectSingleBatchLimit() {
        assertPromotionBatchLimit(1, true);
    }

    @Test
    public void promotionCommits_shouldRespectConcurrentBatchLimit() {
        assertPromotionBatchLimit(2, true);
    }

    @Test
    public void promotionCommits_shouldRespectDefaultBatchLimit() {
        assertPromotionBatchLimit(4, false);
    }

    private void assertPromotionBatchLimit(int limit, boolean configureLimit) {
        nodeCount = 6;
        backupCount = nodeCount - 1;
        partitionCount = 271;
        Config config = getConfig(true, true);
        if (configureLimit) {
            config.setProperty(PARTITION_MAX_PARALLEL_PROMOTION_BATCHES.getName(), Integer.toString(limit));
        }
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
        HazelcastInstance master = instances[0];
        warmUpPartitions(instances);
        fillAndAssertData(master);
        waitAllForSafeState(instances);

        PartitionTableView initialTable = getPartitionService(master).createPartitionTableView();
        HazelcastInstance departing = null;
        Set<PartitionReplica> destinations = new HashSet<>();
        for (int i = 1; i < instances.length; i++) {
            destinations.clear();
            PartitionReplica candidate = PartitionReplica.from(getNode(instances[i]).getLocalMember());
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                if (candidate.equals(initialTable.getReplica(partitionId, 0))) {
                    destinations.add(initialTable.getReplica(partitionId, 1));
                }
            }
            if (destinations.size() > limit && destinations.contains(PartitionReplica.from(getNode(master).getLocalMember()))) {
                departing = instances[i];
                break;
            }
        }
        assertTrue("No departing member supplies both a local batch and pending work beyond the cap", departing != null);
        int expectedBatches = destinations.size();

        AtomicInteger started = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Map<HazelcastInstance, CountDownLatch> releases = new HashMap<>();
        for (HazelcastInstance instance : instances) {
            if (instance == departing) {
                continue;
            }
            CountDownLatch release = new CountDownLatch(1);
            releases.put(instance, release);
            getPartitionServiceImpl(instance).setMigrationInterceptor(new MigrationInterceptor() {
                @Override
                public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                    if (participant == MigrationParticipant.DESTINATION) {
                        peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                        started.incrementAndGet();
                        assertOpenEventually(release);
                    }
                }

                @Override
                public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                                boolean success) {
                    if (participant == MigrationParticipant.DESTINATION) {
                        active.decrementAndGet();
                    }
                }
            });
        }
        PartitionMigrationListenerTest.EventCollectingMigrationListener listener =
                new PartitionMigrationListenerTest.EventCollectingMigrationListener(false);
        master.getPartitionService().addMigrationListener(listener);

        try {
            terminateInstance(departing);
            assertTrueEventually(() -> assertTrue(started.get() >= limit));
            assertTrueAllTheTime(() -> assertEquals(limit, started.get()), 2);

            // The local batch occupies a slot and is consumed first. Releasing it must admit one more destination.
            releases.get(master).countDown();
            assertTrueEventually(() -> assertEquals(limit + 1, started.get()));
            assertTrueAllTheTime(() -> assertEquals(limit + 1, started.get()), 2);
        } finally {
            releases.values().forEach(CountDownLatch::countDown);
        }
        HazelcastInstance[] survivors = releases.keySet().toArray(new HazelcastInstance[0]);
        waitAllForSafeState(survivors);
        assertEquals(expectedBatches, started.get());
        assertEquals(0, active.get());
        assertEquals(limit, peak.get());
        PartitionTableView table = getPartitionService(master).createPartitionTableView();
        for (HazelcastInstance survivor : survivors) {
            assertEquals(table, getPartitionService(survivor).createPartitionTableView());
            assertNoDuplicateMigrations(survivor);
        }
        assertSizeAndData();
        PartitionMigrationListenerTest.MigrationEventsPack events = listener.ensureAndGetSingleEventPack();
        assertMigrationProcessCompleted(events);
        assertMigrationProcessEventsConsistent(events);
        assertMigrationEventsConsistentWithResult(events);
        assertTrueAllTheTime(() -> {
            List<PartitionMigrationListenerTest.MigrationEventsPack> packs =
                    listener.ensureAndGetEventPacks(listener.getEventPackCount());
            long promotionProcesses = packs.stream()
                    .filter(pack -> pack.migrationsCompleted.stream()
                            .anyMatch(event -> event.getSource() == null && event.getReplicaIndex() == 0))
                    .count();
            assertEquals(1, promotionProcesses);
        }, 2);
    }

    @Test
    public void promotionProcess_shouldFinishWithPartialProgress_whenCallerThrows() {
        Config config = getConfig(true, true);
        config.setProperty(PARTITION_MAX_PARALLEL_PROMOTION_BATCHES.getName(), "2");
        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        HazelcastInstance departing = factory.newHazelcastInstance(config);
        warmUpPartitions(master, slave1, slave2, departing);
        fillAndAssertData(master);
        waitAllForSafeState(master, slave1, slave2, departing);

        AtomicReference<Address> blockedDestination = new AtomicReference<>();
        AtomicInteger localPromotionCount = new AtomicInteger();
        AtomicInteger completedBatches = new AtomicInteger();
        CountDownLatch callerFailed = new CountDownLatch(1);
        CountDownLatch destinationBlocked = new CountDownLatch(1);
        CountDownLatch releaseDestination = new CountDownLatch(1);
        CountDownLatch destinationCompleted = new CountDownLatch(1);
        getPartitionServiceImpl(master).setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                if (participant == MigrationParticipant.MASTER) {
                    Address destination = migrations.iterator().next().getDestinationAddress();
                    if (destination.equals(getAddress(master))) {
                        localPromotionCount.set(migrations.size());
                    } else {
                        // Leave the last remote batch outstanding when the first remote result is processed.
                        blockedDestination.set(destination);
                    }
                }
            }

            @Override
            public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                            boolean success) {
                if (participant == MigrationParticipant.MASTER && completedBatches.incrementAndGet() == 2) {
                    assertOpenEventually(destinationBlocked);
                    callerFailed.countDown();
                    throw new IllegalStateException("Injected caller-side promotion failure");
                }
            }
        });
        for (HazelcastInstance destination : asList(slave1, slave2)) {
            getPartitionServiceImpl(destination).setMigrationInterceptor(new MigrationInterceptor() {
                @Override
                public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                    if (participant == MigrationParticipant.DESTINATION
                            && getAddress(destination).equals(blockedDestination.get())) {
                        destinationBlocked.countDown();
                        assertOpenEventually(releaseDestination);
                    }
                }

                @Override
                public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                                boolean success) {
                    if (participant == MigrationParticipant.DESTINATION
                            && getAddress(destination).equals(blockedDestination.get())) {
                        destinationCompleted.countDown();
                    }
                }
            });
        }
        PartitionMigrationListenerTest.EventCollectingMigrationListener listener =
                new PartitionMigrationListenerTest.EventCollectingMigrationListener(true);
        master.getPartitionService().addMigrationListener(listener);
        try {
            terminateInstance(departing);
            assertOpenEventually(callerFailed);
            PartitionMigrationListenerTest.MigrationEventsPack events = listener.ensureAndGetSingleEventPack();
            assertTrue(localPromotionCount.get() > 0);
            assertEquals(localPromotionCount.get(), events.migrationProcessCompleted.getCompletedMigrations());
            assertTrue(events.migrationProcessCompleted.getRemainingMigrations() > 0);
            assertEquals(events.migrationProcessStarted.getStartTime(), events.migrationProcessCompleted.getStartTime());
            assertEquals(events.migrationProcessStarted.getPlannedMigrations(),
                    events.migrationProcessCompleted.getPlannedMigrations());
            assertMigrationEventsConsistentWithResult(events);
            for (ReplicaMigrationEvent event : events.migrationsCompleted) {
                assertEquals(getAddress(master), event.getDestination().getAddress());
            }
            assertTrueAllTheTime(() -> assertEquals(1, listener.getEventPackCount()), 2);
        } finally {
            getPartitionServiceImpl(master).resetMigrationInterceptor();
            releaseDestination.countDown();
        }
        assertOpenEventually(destinationCompleted);
        // Keep the existing repair scheduling semantics: explicitly request a new control round after the injected error.
        getPartitionServiceImpl(master).getMigrationManager().triggerControlTask();
        waitAllForSafeState(master, slave1, slave2);
        listener.ensureAndGetEventPacks(2);
        PartitionTableView table = getPartitionService(master).createPartitionTableView();
        assertEquals(table, getPartitionService(slave1).createPartitionTableView());
        assertEquals(table, getPartitionService(slave2).createPartitionTableView());
        assertSizeAndData();
        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
    }

    @Test
    public void promotionCommits_shouldRunConcurrently() throws Exception {
        Config config = getConfig(true, true);
        config.setProperty(PARTITION_MAX_PARALLEL_PROMOTION_BATCHES.getName(), "3");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        HazelcastInstance slave3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave1, slave2, slave3);
        warmUpPartitions(master, slave1, slave2, slave3);
        setMigrationCommitTimeoutMillis(master, 5000);
        fillAndAssertData(master);

        PartitionMigrationListenerTest.EventCollectingMigrationListener listener =
                new PartitionMigrationListenerTest.EventCollectingMigrationListener(false);
        master.getPartitionService().addMigrationListener(listener);

        AtomicReference<HazelcastInstance> blockedDestination = new AtomicReference<>();
        CountDownLatch concurrentPromotionCompleted = new CountDownLatch(1);
        setMigrationListenerToBlockFirstRemotePromotion(master, blockedDestination, slave1, slave2);
        setMigrationListenerToSignalUnblockedPromotion(slave1, blockedDestination, concurrentPromotionCompleted);
        setMigrationListenerToSignalUnblockedPromotion(slave2, blockedDestination, concurrentPromotionCompleted);

        terminateInstance(slave3);
        assertClusterSizeEventually(3, master, slave1, slave2);

        assertTrueEventually(() -> assertEquals(0, concurrentPromotionCompleted.getCount()));

        resetPacketFiltersFrom(master);
        waitAllForSafeState(master, slave1, slave2);

        PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(() -> {
            assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
            assertEquals(masterPartitionTable, getPartitionService(slave2).createPartitionTableView());
        });

        PartitionMigrationListenerTest.MigrationEventsPack eventsPack = listener.ensureAndGetSingleEventPack();
        assertMigrationProcessCompleted(eventsPack);
        assertMigrationProcessEventsConsistent(eventsPack);
        assertMigrationEventsConsistentWithResult(eventsPack);

        assertSizeAndData();
        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
    }

    @Test
    public void promotionCommits_shouldRecoverWhenMasterLeaves() {
        nodeCount = 5;
        backupCount = nodeCount - 1;
        Config config = getConfig(true, true);
        config.setProperty(PARTITION_MAX_PARALLEL_PROMOTION_BATCHES.getName(), "4");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        HazelcastInstance slave3 = factory.newHazelcastInstance(config);
        HazelcastInstance slave4 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(5, slave1, slave2, slave3, slave4);
        warmUpPartitions(master, slave1, slave2, slave3, slave4);
        fillAndAssertData(master);
        waitAllForSafeState(master, slave1, slave2, slave3, slave4);
        PartitionTableView initialPartitionTable = getPartitionService(master).createPartitionTableView();
        PartitionReplica departingReplica = PartitionReplica.from(getNode(slave4).getLocalMember());

        AtomicReference<HazelcastInstance> blockedDestination = new AtomicReference<>();
        CountDownLatch concurrentPromotionCompleted = new CountDownLatch(2);
        setMigrationListenerToBlockFirstRemotePromotion(master, blockedDestination, slave1, slave2, slave3);
        setMigrationListenerToSignalUnblockedPromotion(slave1, blockedDestination, concurrentPromotionCompleted);
        setMigrationListenerToSignalUnblockedPromotion(slave2, blockedDestination, concurrentPromotionCompleted);
        setMigrationListenerToSignalUnblockedPromotion(slave3, blockedDestination, concurrentPromotionCompleted);

        terminateInstance(slave4);
        assertClusterSizeEventually(4, master, slave1, slave2, slave3);
        assertOpenEventually(concurrentPromotionCompleted);

        // The first remote result is blocked. The other two destinations must each hold completed promotions
        // that neither the master nor the other completed destination has applied.
        List<HazelcastInstance> completedDestinations = asList(slave1, slave2, slave3).stream()
                .filter(instance -> instance != blockedDestination.get()).toList();
        assertEquals(2, completedDestinations.size());
        PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        for (HazelcastInstance destination : completedDestinations) {
            HazelcastInstance otherDestination = completedDestinations.get(destination == completedDestinations.get(0) ? 1 : 0);
            PartitionTableView destinationTable = getPartitionService(destination).createPartitionTableView();
            PartitionTableView otherTable = getPartitionService(otherDestination).createPartitionTableView();
            int promotedPartitions = 0;
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                if (departingReplica.equals(initialPartitionTable.getReplica(partitionId, 0))
                        && initialPartitionTable.getReplica(partitionId, 1).isIdentical(getNode(destination).getLocalMember())) {
                    assertEquals(initialPartitionTable.getReplica(partitionId, 1), destinationTable.getReplica(partitionId, 0));
                    assertTrue(destinationTable.getPartition(partitionId).version()
                            > masterPartitionTable.getPartition(partitionId).version());
                    assertNull(masterPartitionTable.getReplica(partitionId, 0));
                    assertNull(otherTable.getReplica(partitionId, 0));
                    promotedPartitions++;
                }
            }
            assertTrue("No independent promotions on " + getAddress(destination), promotedPartitions > 0);
        }

        terminateInstance(master);
        assertClusterSizeEventually(3, slave1, slave2, slave3);
        waitAllForSafeState(slave1, slave2, slave3);

        PartitionTableView partitionTable = getPartitionService(slave1).createPartitionTableView();
        assertTrueEventually(() -> {
            assertEquals(partitionTable, getPartitionService(slave2).createPartitionTableView());
            assertEquals(partitionTable, getPartitionService(slave3).createPartitionTableView());
        });

        assertSizeAndData();
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
        assertNoDuplicateMigrations(slave3);
    }

    @Test
    public void queuedPromotionResult_shouldNotInstallDepartedDestination() throws Exception {
        Config config = getConfig(true, true);
        config.setProperty(PARTITION_MAX_PARALLEL_PROMOTION_BATCHES.getName(), "3");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        HazelcastInstance slave3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave1, slave2, slave3);
        warmUpPartitions(master, slave1, slave2, slave3);
        setMigrationCommitTimeoutMillis(master, 5000);
        fillAndAssertData(master);

        CountDownLatch localPromotionStarted = new CountDownLatch(1);
        CountDownLatch releaseLocalPromotion = new CountDownLatch(1);
        CountDownLatch remoteDestinationCompleted = new CountDownLatch(1);
        CountDownLatch remoteMasterCompleted = new CountDownLatch(1);
        AtomicReference<Address> completedDestination = new AtomicReference<>();
        AtomicReference<Boolean> completedResult = new AtomicReference<>();
        AtomicReference<Integer> failedPromotionCount = new AtomicReference<>();
        PartitionMigrationListenerTest.EventCollectingMigrationListener listener =
                new PartitionMigrationListenerTest.EventCollectingMigrationListener(true);
        master.getPartitionService().addMigrationListener(listener);
        setMigrationListenerToBlockLocalPromotion(master, localPromotionStarted, releaseLocalPromotion,
                completedDestination, completedResult, failedPromotionCount, remoteMasterCompleted);
        setMigrationListenerToSignalPromotionComplete(
                slave1, completedDestination, remoteDestinationCompleted);
        setMigrationListenerToSignalPromotionComplete(
                slave2, completedDestination, remoteDestinationCompleted);

        terminateInstance(slave3);
        assertClusterSizeEventually(3, master, slave1, slave2);

        HazelcastInstance departedDestination;
        HazelcastInstance survivingSlave;
        try {
            assertOpenEventually(localPromotionStarted);
            assertOpenEventually(remoteDestinationCompleted);
            assertEquals("Remote promotion was installed before the local promotion completed",
                    1, remoteMasterCompleted.getCount());

            departedDestination = findInstance(completedDestination.get(), slave1, slave2);
            survivingSlave = departedDestination == slave1 ? slave2 : slave1;
            terminateInstance(departedDestination);
            assertClusterSizeEventually(2, master, survivingSlave);
        } finally {
            releaseLocalPromotion.countDown();
        }

        assertOpenEventually(remoteMasterCompleted);
        assertFalse("Departed destination was installed as partition owner", completedResult.get());

        PartitionMigrationListenerTest.MigrationEventsPack eventsPack = listener.ensureAndGetSingleEventPack();
        assertMigrationProcessCompleted(eventsPack);
        assertMigrationProcessEventsConsistent(eventsPack);
        assertMigrationEventsConsistentWithResult(eventsPack, failedPromotionCount.get());

        waitAllForSafeState(master, survivingSlave);

        PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(() ->
                assertEquals(masterPartitionTable, getPartitionService(survivingSlave).createPartitionTableView()));

        assertSizeAndData();
        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(survivingSlave);
    }

    @Test
    public void promotionCommit_shouldBeRetried_whenTargetNotResponds() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "4000");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        HazelcastInstance slave3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave1, slave2, slave3);
        warmUpPartitions(master, slave1, slave2, slave3);

        setMigrationCommitTimeoutMillis(master, 5000);

        fillAndAssertData(master);

        PartitionMigrationListenerTest.EventCollectingMigrationListener listener =
                new PartitionMigrationListenerTest.EventCollectingMigrationListener(false);
        master.getPartitionService().addMigrationListener(listener);

        // reject promotion commits from master to prevent promotions complete when slave3 leaves the cluster
        rejectOperationsFrom(master, F_ID, singletonList(PROMOTION_COMMIT));

        terminateInstance(slave3);
        assertClusterSizeEventually(3, slave1, slave2);

        // set migration listener to drop promotion commit response after a promotion is completed
        setMigrationListenerToPromotionResponse(master, slave2);

        // allow promotion commits
        resetPacketFiltersFrom(master);

        sleepSeconds(10);
        resetPacketFiltersFrom(slave2);

        waitAllForSafeState(master, slave1, slave2);

        PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(() -> {
            assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
            assertEquals(masterPartitionTable, getPartitionService(slave2).createPartitionTableView());
        });

        List<PartitionMigrationListenerTest.MigrationEventsPack> eventsPacks = listener.ensureAndGetEventPacks(2);
        for (PartitionMigrationListenerTest.MigrationEventsPack eventsPack : eventsPacks) {
            assertMigrationProcessCompleted(eventsPack);
            assertMigrationProcessEventsConsistent(eventsPack);
            assertMigrationEventsConsistentWithResult(eventsPack);
        }
        Set<Integer> promotedPartitions = new HashSet<>();
        for (ReplicaMigrationEvent event : eventsPacks.get(0).migrationsCompleted) {
            assertTrue("Duplicate promotion event for partition " + event.getPartitionId(),
                    promotedPartitions.add(event.getPartitionId()));
        }
        assertTrueAllTheTime(() -> assertEquals(2, listener.getEventPackCount()), 3);

        assertSizeAndData();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
    }

    @Test
    public void promotionCommit_shouldRollback_whenTargetCrashes() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "4000");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        HazelcastInstance slave3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave1, slave2, slave3);
        warmUpPartitions(master, slave1, slave2, slave3);

        setMigrationCommitTimeoutMillis(master, 5000);

        fillAndAssertData(master);

        // reject promotion commits from master to prevent promotions complete when slave3 leaves the cluster
        rejectOperationsFrom(master, F_ID, singletonList(PROMOTION_COMMIT));

        terminateInstance(slave3);
        assertClusterSizeEventually(3, slave1, slave2);

        // set migration listener to drop promotion commit response after a promotion is completed
        setMigrationListenerToPromotionResponse(master, slave2);

        // allow promotion commits
        resetPacketFiltersFrom(master);

        sleepSeconds(10);
        terminateInstance(slave2);

        waitAllForSafeState(master, slave1);

        PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(() -> assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView()));

        assertSizeAndData();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
    }

    private void setMigrationListenerToBlockLocalPromotion(HazelcastInstance master,
                                                           CountDownLatch localPromotionStarted,
                                                           CountDownLatch releaseLocalPromotion,
                                                           AtomicReference<Address> completedDestination,
                                                           AtomicReference<Boolean> completedResult,
                                                           AtomicReference<Integer> failedPromotionCount,
                                                           CountDownLatch remoteMasterCompleted) {
        getPartitionServiceImpl(master).setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                if (participant == MigrationParticipant.DESTINATION) {
                    localPromotionStarted.countDown();
                    assertOpenEventually(releaseLocalPromotion);
                }
            }

            @Override
            public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                            boolean success) {
                Address destination = completedDestination.get();
                if (participant == MigrationParticipant.MASTER
                        && destination != null
                        && migrations.iterator().next().getDestinationAddress().equals(destination)) {
                    completedResult.set(success);
                    failedPromotionCount.set(migrations.size());
                    remoteMasterCompleted.countDown();
                }
            }
        });
    }

    private void setMigrationListenerToSignalPromotionComplete(HazelcastInstance destination,
                                                               AtomicReference<Address> completedDestination,
                                                               CountDownLatch completed) {
        getPartitionServiceImpl(destination).setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                            boolean success) {
                if (participant == MigrationParticipant.DESTINATION && success
                        && completedDestination.compareAndSet(null, getAddress(destination))) {
                    completed.countDown();
                }
            }
        });
    }

    private void setMigrationListenerToBlockFirstRemotePromotion(HazelcastInstance master,
                                                                AtomicReference<HazelcastInstance> blockedDestination,
                                                                HazelcastInstance... destinations) {
        getPartitionServiceImpl(master).setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                if (participant != MigrationParticipant.MASTER) {
                    return;
                }

                Address destinationAddress = migrations.iterator().next().getDestinationAddress();
                if (destinationAddress.equals(getAddress(master))) {
                    return;
                }

                HazelcastInstance destination = findInstance(destinationAddress, destinations);
                if (blockedDestination.compareAndSet(null, destination)) {
                    rejectOperationsBetween(master, destination, F_ID, singletonList(PROMOTION_COMMIT));
                }
            }
        });
    }

    private void setMigrationListenerToSignalUnblockedPromotion(HazelcastInstance destination,
                                                                AtomicReference<HazelcastInstance> blockedDestination,
                                                                CountDownLatch completed) {
        getPartitionServiceImpl(destination).setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                            boolean success) {
                if (participant == MigrationParticipant.DESTINATION && destination != blockedDestination.get()) {
                    assertTrue(success);
                    completed.countDown();
                }
            }
        });
    }

    private static HazelcastInstance findInstance(Address address, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            if (address.equals(getAddress(instance))) {
                return instance;
            }
        }
        throw new AssertionError("No Hazelcast instance at " + address);
    }

    private void setMigrationCommitTimeoutMillis(HazelcastInstance master, long timeout) throws Exception {
        MigrationManager migrationManager = getPartitionServiceImpl(master).getMigrationManager();
        Field field = MigrationManagerImpl.class.getDeclaredField("memberHeartbeatTimeoutMillis");
        field.setAccessible(true);
        field.setLong(migrationManager, timeout);
    }

    private void fillAndAssertData(HazelcastInstance hz) {
        assertTrueEventually(() -> {
            fillData(hz);
            assertSizeAndData();
        });
    }

    private void setMigrationListenerToPromotionResponse(HazelcastInstance master, HazelcastInstance destination) {
        getPartitionServiceImpl(destination).setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrationInfos, boolean success) {
                if (participant == MigrationParticipant.DESTINATION) {
                    dropOperationsBetween(destination, master, SpiDataSerializerHook.F_ID, singletonList(NORMAL_RESPONSE));
                }
            }
        });
    }

    private static void assertNoDuplicateMigrations(HazelcastInstance hz) {
        TestMigrationAwareService service = getNodeEngineImpl(hz).getService(TestMigrationAwareService.SERVICE_NAME);
        List<PartitionMigrationEvent> events = service.getBeforeEvents();
        Set<PartitionMigrationEvent> uniqueEvents = new HashSet<>(events);
        assertEquals("Node: " + getAddress(hz) + ", Events: " + events, uniqueEvents.size(), events.size());
    }

    private static InternalPartitionServiceImpl getPartitionServiceImpl(HazelcastInstance hz) {
        return getNode(hz).partitionService;
    }

    private static void ensurePartitionsInitialized(HazelcastInstance... instances) {
        warmUpPartitions(instances);
        for (HazelcastInstance instance : instances) {
            assertPartitionStateInitialized(instance);
        }
    }

    private static void assertPartitionStateInitialized(HazelcastInstance instance) {
        assertTrueEventually(() -> assertTrue(getPartitionServiceImpl(instance).getPartitionStateManager().isInitialized()));
    }
}
