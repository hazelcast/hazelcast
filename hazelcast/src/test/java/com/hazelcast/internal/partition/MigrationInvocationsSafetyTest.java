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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.ASSIGN_PARTITIONS;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.FETCH_PARTITION_STATE;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.F_ID;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PARTITION_STATE_OP;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PROMOTION_COMMIT;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    @Ignore("https://github.com/hazelcast/hazelcast/issues/12788")
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

    private void setMigrationCommitTimeoutMillis(HazelcastInstance master, long timeout) throws Exception {
        MigrationManager migrationManager = getPartitionServiceImpl(master).getMigrationManager();
        Field field = MigrationManager.class.getDeclaredField("memberHeartbeatTimeoutMillis");
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
