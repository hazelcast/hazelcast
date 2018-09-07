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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.InternalMigrationListener;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.ASSIGN_PARTITIONS;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.FETCH_PARTITION_STATE;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.F_ID;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PARTITION_STATE_OP;
import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PROMOTION_COMMIT;
import static com.hazelcast.spi.impl.SpiDataSerializerHook.NORMAL_RESPONSE;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationInvocationsSafetyTest extends PartitionCorrectnessTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug.xml");

    @Before
    public void setupParams() {
        nodeCount = 4;
        backupCount = nodeCount - 1;
    }

    @Test
    public void members_shouldAgree_onPartitionTable_whenMasterChanges() {
        final HazelcastInstance initialMaster = factory.newHazelcastInstance();
        final HazelcastInstance nextMaster = factory.newHazelcastInstance();
        final HazelcastInstance slave1 = factory.newHazelcastInstance();
        final HazelcastInstance slave2 = factory.newHazelcastInstance();
        final HazelcastInstance slave3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(5, nextMaster, slave1, slave2, slave3);

        // nextMaster & slave1 won't receive partition table updates from initialMaster.
        dropOperationsBetween(initialMaster, asList(slave1, nextMaster), F_ID, singletonList(PARTITION_STATE_OP));
        dropOperationsBetween(nextMaster, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));
        dropOperationsBetween(slave1, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));

        warmUpPartitions(initialMaster, slave2, slave3);

        final int initialPartitionStateVersion = getPartitionService(initialMaster).getPartitionStateVersion();
        assertEquals(initialPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
        assertEquals(initialPartitionStateVersion, getPartitionService(slave3).getPartitionStateVersion());
        assertEquals(0, getPartitionService(nextMaster).getPartitionStateVersion());
        assertEquals(0, getPartitionService(slave1).getPartitionStateVersion());

        dropOperationsBetween(nextMaster, slave3, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);

        spawn(new Runnable() {
            @Override
            public void run() {
                assertClusterSizeEventually(4, nextMaster, slave1, slave2, slave3);
                sleepSeconds(10);
                resetPacketFiltersFrom(nextMaster);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int nextPartitionStateVersion = getPartitionService(nextMaster).getPartitionStateVersion();
                assertThat(nextPartitionStateVersion, greaterThan(initialPartitionStateVersion));

                assertEquals(nextPartitionStateVersion, getPartitionService(slave1).getPartitionStateVersion());
                assertEquals(nextPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
                assertEquals(nextPartitionStateVersion, getPartitionService(slave3).getPartitionStateVersion());
            }
        });

    }

    @Test
    public void members_shouldAgree_onPartitionTable_whenMasterChanges_and_anotherMemberCrashes() {
        final HazelcastInstance initialMaster = factory.newHazelcastInstance();
        final HazelcastInstance nextMaster = factory.newHazelcastInstance();
        final HazelcastInstance slave1 = factory.newHazelcastInstance();
        final HazelcastInstance slave2 = factory.newHazelcastInstance();
        final HazelcastInstance slave3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(5, nextMaster, slave1, slave2, slave3);

        // nextMaster & slave1 won't receive partition table updates from initialMaster.
        dropOperationsBetween(initialMaster, asList(slave1, nextMaster), F_ID, singletonList(PARTITION_STATE_OP));
        dropOperationsBetween(nextMaster, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));
        dropOperationsBetween(slave1, singletonList(initialMaster), F_ID, singletonList(ASSIGN_PARTITIONS));

        warmUpPartitions(initialMaster, slave2, slave3);

        final int initialPartitionStateVersion = getPartitionService(initialMaster).getPartitionStateVersion();
        assertEquals(initialPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
        assertEquals(initialPartitionStateVersion, getPartitionService(slave3).getPartitionStateVersion());
        assertEquals(0, getPartitionService(nextMaster).getPartitionStateVersion());
        assertEquals(0, getPartitionService(slave1).getPartitionStateVersion());

        dropOperationsBetween(nextMaster, slave3, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);

        spawn(new Runnable() {
            @Override
            public void run() {
                assertClusterSizeEventually(4, nextMaster, slave1, slave2, slave3);
                sleepSeconds(10);
                terminateInstance(slave3);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int nextPartitionStateVersion = getPartitionService(nextMaster).getPartitionStateVersion();
                assertThat(nextPartitionStateVersion, greaterThan(initialPartitionStateVersion));

                assertEquals(nextPartitionStateVersion, getPartitionService(slave1).getPartitionStateVersion());
                assertEquals(nextPartitionStateVersion, getPartitionService(slave2).getPartitionStateVersion());
            }
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
        final HazelcastInstance initialMaster = factory.newHazelcastInstance(new Config().setLiteMember(liteMaster));
        final HazelcastInstance nextMaster = factory.newHazelcastInstance();
        final HazelcastInstance slave = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, nextMaster, slave);
        warmUpPartitions(initialMaster, nextMaster, slave);

        dropOperationsBetween(nextMaster, slave, F_ID, singletonList(FETCH_PARTITION_STATE));

        terminateInstance(initialMaster);
        assertClusterSizeEventually(2, nextMaster, slave);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse(getPartitionService(nextMaster).isMemberStateSafe());
                assertFalse(getPartitionService(slave).isMemberStateSafe());
            }
        }, 5);

        resetPacketFiltersFrom(nextMaster);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getPartitionService(nextMaster).isMemberStateSafe());
                assertTrue(getPartitionService(slave).isMemberStateSafe());
            }
        });
    }

    @Test
    public void migrationCommit_shouldBeRetried_whenTargetNotResponds() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "5")
                .setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                .setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");

        final HazelcastInstance master = factory.newHazelcastInstance(config);
        final HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        final HazelcastInstance slave2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, slave1, slave2);
        warmUpPartitions(master, slave1, slave2);

        fillData(master);
        assertSizeAndDataEventually();

        // prevent migrations before adding migration listeners when slave3 joins the cluster
        changeClusterStateEventually(master, ClusterState.NO_MIGRATION);

        final HazelcastInstance slave3 = factory.newHazelcastInstance(config);
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

        final PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
                assertEquals(masterPartitionTable, getPartitionService(slave2).createPartitionTableView());
                assertEquals(masterPartitionTable, getPartitionService(slave3).createPartitionTableView());
            }
        });

        assertSizeAndData();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
        assertNoDuplicateMigrations(slave3);
    }

    private void setMigrationListenerToDropCommitResponse(final HazelcastInstance master, final HazelcastInstance destination) {
        // intercept migration complete on destination and drop commit response
        getPartitionServiceImpl(destination).setInternalMigrationListener(new InternalMigrationListener() {
            final AtomicReference<MigrationInfo> committedMigrationInfoRef = new AtomicReference<MigrationInfo>();

            @Override
            public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
               assertClusterStateEventually(ClusterState.ACTIVE, destination);
            }

            @Override
            public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
                if (participant == MigrationParticipant.DESTINATION && committedMigrationInfoRef.compareAndSet(null, migrationInfo)) {
                    dropOperationsBetween(destination, master, SpiDataSerializerHook.F_ID, singletonList(NORMAL_RESPONSE));
                }
            }
        });
    }

    @Test
    public void migrationCommit_shouldRollback_whenTargetCrashes() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "5")
                .setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                .setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");

        final HazelcastInstance master = factory.newHazelcastInstance(config);
        final HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        final HazelcastInstance slave2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, slave1, slave2);
        warmUpPartitions(master, slave1, slave2);

        fillData(master);
        assertSizeAndDataEventually();

        // prevent migrations before adding migration listeners when slave3 joins the cluster
        changeClusterStateEventually(master, ClusterState.NO_MIGRATION);

        final HazelcastInstance slave3 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(4, slave1, slave2);

        // set migration listener to drop migration commit response after a migration to slave3 is completed
        setMigrationListenerToDropCommitResponse(master, slave3);

        // enable migrations
        changeClusterStateEventually(master, ClusterState.ACTIVE);

        // wait for retry of migration commit
        sleepSeconds(10);

        terminateInstance(slave3);

        waitAllForSafeState(master, slave1, slave2);

        final PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
                assertEquals(masterPartitionTable, getPartitionService(slave2).createPartitionTableView());
            }
        });

        assertSizeAndData();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
    }

    @Test
    public void promotionCommit_shouldBeRetried_whenTargetNotResponds() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "5")
                .setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                .setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");

        final HazelcastInstance master = factory.newHazelcastInstance(config);
        final HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        final HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        final HazelcastInstance slave3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave1, slave2, slave3);
        warmUpPartitions(master, slave1, slave2, slave3);

        fillData(master);
        assertSizeAndDataEventually();

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

        final PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
                assertEquals(masterPartitionTable, getPartitionService(slave2).createPartitionTableView());
            }
        });

        assertSizeAndData();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
        assertNoDuplicateMigrations(slave2);
    }

    @Test
    public void promotionCommit_shouldRollback_whenTargetCrashes() throws Exception {
        Config config = getConfig(true, true)
                .setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "5")
                .setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                .setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");

        final HazelcastInstance master = factory.newHazelcastInstance(config);
        final HazelcastInstance slave1 = factory.newHazelcastInstance(config);
        final HazelcastInstance slave2 = factory.newHazelcastInstance(config);
        final HazelcastInstance slave3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave1, slave2, slave3);
        warmUpPartitions(master, slave1, slave2, slave3);

        fillData(master);
        assertSizeAndDataEventually();

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

        final PartitionTableView masterPartitionTable = getPartitionService(master).createPartitionTableView();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(masterPartitionTable, getPartitionService(slave1).createPartitionTableView());
            }
        });

        assertSizeAndData();

        assertNoDuplicateMigrations(master);
        assertNoDuplicateMigrations(slave1);
    }

    private void setMigrationListenerToPromotionResponse(final HazelcastInstance master, final HazelcastInstance destination) {
        getPartitionServiceImpl(destination).setInternalMigrationListener(new InternalMigrationListener() {
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
        Set<PartitionMigrationEvent> uniqueEvents = new HashSet<PartitionMigrationEvent>(events);
        assertEquals(uniqueEvents.size(), events.size());
    }

    private static InternalPartitionServiceImpl getPartitionServiceImpl(HazelcastInstance hz) {
        return getNode(hz).partitionService;
    }
}
