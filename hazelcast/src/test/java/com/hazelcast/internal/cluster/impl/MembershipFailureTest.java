/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.internal.cluster.fd.ClusterFailureDetectorType;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.EXPLICIT_SUSPICION;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.FETCH_MEMBER_LIST_STATE;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT_COMPLAINT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.SPLIT_BRAIN_MERGE_VALIDATION;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static com.hazelcast.spi.properties.ClusterProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class MembershipFailureTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "fd:{0}")
    public static Collection<ClusterFailureDetectorType> parameters() {
        return Arrays.asList(ClusterFailureDetectorType.values());
    }

    @Parameterized.Parameter
    public ClusterFailureDetectorType failureDetectorType;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void slave_shutdown() {
        slave_goesDown(false);
    }

    @Test
    public void slave_crash() {
        slave_goesDown(true);
    }

    private void slave_goesDown(boolean terminate) {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSize(3, master, slave2);
        assertClusterSizeEventually(3, slave1);

        if (terminate) {
            terminateInstance(slave1);
        } else {
            slave1.shutdown();
        }

        assertClusterSizeEventually(2, master, slave2);

        assertMasterAddress(Accessors.getAddress(master), master, slave2);
        assertMemberViewsAreSame(getMemberMap(master), getMemberMap(slave2));
    }

    @Test
    public void master_shutdown() {
        master_goesDown(false);
    }

    @Test
    public void master_crash() {
        master_goesDown(true);
    }

    private void master_goesDown(boolean terminate) {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSize(3, master, slave2);
        assertClusterSizeEventually(3, slave1);

        if (terminate) {
            terminateInstance(master);
        } else {
            master.shutdown();
        }

        assertClusterSizeEventually(2, slave1, slave2);

        assertMasterAddress(Accessors.getAddress(slave1), slave1, slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void masterAndMasterCandidate_crashSequentially() {
        masterAndMasterCandidate_crash(false);
    }

    @Test
    public void masterAndMasterCandidate_crashSimultaneously() {
        masterAndMasterCandidate_crash(true);
    }

    private void masterAndMasterCandidate_crash(boolean simultaneousCrash) {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance masterCandidate = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSize(4, master, slave2);
        assertClusterSizeEventually(4, masterCandidate, slave1);

        if (simultaneousCrash) {
            terminateInstanceAsync(master);
            terminateInstanceAsync(masterCandidate);
        } else {
            terminateInstance(master);
            terminateInstance(masterCandidate);
        }

        assertClusterSizeEventually(2, slave1, slave2);

        assertMasterAddress(Accessors.getAddress(slave1), slave1, slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    private static void terminateInstanceAsync(final HazelcastInstance master) {
        spawn(() -> terminateInstance(master));
    }

    @Test
    public void slaveCrash_duringMastershipClaim() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance masterCandidate = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSize(4, master, slave2);
        assertClusterSizeEventually(4, masterCandidate, slave1);

        // drop FETCH_MEMBER_LIST_STATE packets to block mastership claim process
        dropOperationsBetween(masterCandidate, slave1, F_ID, singletonList(FETCH_MEMBER_LIST_STATE));

        terminateInstance(master);

        final ClusterServiceImpl clusterService = Accessors.getNode(masterCandidate).getClusterService();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(clusterService.getClusterJoinManager().isMastershipClaimInProgress());
            }
        });

        sleepSeconds(3);
        terminateInstance(slave1);

        assertClusterSizeEventually(2, masterCandidate);
        assertClusterSizeEventually(2, slave2);

        assertMasterAddress(Accessors.getAddress(masterCandidate), masterCandidate, slave2);
        assertMemberViewsAreSame(getMemberMap(masterCandidate), getMemberMap(slave2));
    }

    @Test
    public void masterCandidateCrash_duringMastershipClaim() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance masterCandidate = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSize(4, master, slave2);
        assertClusterSizeEventually(4, masterCandidate, slave1);

        // drop FETCH_MEMBER_LIST_STATE packets to block mastership claim process
        dropOperationsBetween(masterCandidate, slave1, F_ID, singletonList(FETCH_MEMBER_LIST_STATE));

        terminateInstance(master);

        final ClusterServiceImpl clusterService = Accessors.getNode(masterCandidate).getClusterService();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(clusterService.getClusterJoinManager().isMastershipClaimInProgress());
            }
        });

        sleepSeconds(3);
        terminateInstance(masterCandidate);

        assertClusterSizeEventually(2, slave1, slave2);

        assertMasterAddress(Accessors.getAddress(slave1), slave1, slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void slave_heartbeat_timeout() {
        Config config = smallInstanceConfig().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSize(3, master, slave2);
        assertClusterSizeEventually(3, slave1);

        dropOperationsFrom(slave2, F_ID, singletonList(HEARTBEAT));

        assertClusterSizeEventually(2, master, slave1);
        assertClusterSizeEventually(1, slave2);
    }

    @Test
    public void master_heartbeat_timeout() {
        Config config = smallInstanceConfig().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "3");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSize(3, master, slave2);
        assertClusterSizeEventually(3, slave1);

        dropOperationsFrom(master, F_ID, singletonList(HEARTBEAT));
        dropOperationsFrom(slave1, F_ID, singletonList(HEARTBEAT_COMPLAINT));
        dropOperationsFrom(slave2, F_ID, singletonList(HEARTBEAT_COMPLAINT));

        assertClusterSizeEventually(1, master);
        assertClusterSizeEventually(2, slave1, slave2);
    }

    @Test
    public void heartbeat_not_sent_to_suspected_member() {
        Config config = smallInstanceConfig().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSize(3, master, slave2);
        assertClusterSizeEventually(3, slave1);

        // prevent heartbeat from master to slave to prevent suspect to be removed
        dropOperationsBetween(master, slave1, F_ID, singletonList(HEARTBEAT));
        suspectMember(slave1, master);

        assertClusterSizeEventually(2, master, slave2);
        assertClusterSizeEventually(1, slave1);
    }

    @Test
    public void slave_heartbeat_removes_suspicion() {
        Config config = smallInstanceConfig().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        final HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSize(3, master, slave2);
        assertClusterSizeEventually(3, slave1);

        dropOperationsBetween(slave2, slave1, F_ID, singletonList(HEARTBEAT));

        final MembershipManager membershipManager = Accessors.getNode(slave1).getClusterService().getMembershipManager();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(membershipManager.isMemberSuspected(Accessors.getAddress(slave2)));
            }
        });

        resetPacketFiltersFrom(slave2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(membershipManager.isMemberSuspected(Accessors.getAddress(slave2)));
            }
        });
    }

    @Test
    public void slave_receives_member_list_from_non_master() {
        String infiniteTimeout = Integer.toString(Integer.MAX_VALUE);
        Config config = smallInstanceConfig().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), infiniteTimeout)
                .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");

        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);
        HazelcastInstance slave3 = newHazelcastInstance(config);

        assertClusterSize(4, master, slave3);
        assertClusterSizeEventually(4, slave1, slave2);

        dropOperationsFrom(master, F_ID, singletonList(HEARTBEAT));
        dropOperationsFrom(slave1, F_ID, singletonList(HEARTBEAT));
        dropOperationsFrom(slave2, F_ID, singletonList(HEARTBEAT));
        dropOperationsFrom(slave3, F_ID, singletonList(HEARTBEAT));

        suspectMember(slave2, master);
        suspectMember(slave2, slave1);
        suspectMember(slave3, master);
        suspectMember(slave3, slave1);

        assertClusterSizeEventually(2, slave2, slave3);

        assertMemberViewsAreSame(getMemberMap(slave2), getMemberMap(slave3));

        assertClusterSizeEventually(2, master, slave1);

        assertMemberViewsAreSame(getMemberMap(master), getMemberMap(slave1));
    }

    @Test
    public void master_candidate_has_stale_member_list() {
        Config config = smallInstanceConfig().setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);

        assertClusterSize(2, master);
        assertClusterSize(2, slave1);

        HazelcastInstance slave2 = newHazelcastInstance(config);
        assertClusterSize(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSize(3, slave2);

        rejectOperationsBetween(master, slave1, F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance slave3 = newHazelcastInstance(config);

        assertClusterSize(4, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(3, slave1, slave2, slave3);

        Address newMasterAddress = Accessors.getAddress(slave1);
        assertMasterAddress(newMasterAddress, slave1, slave2, slave3);
    }

    @Test
    public void master_candidate_discovers_member_list_recursively() {
        Config config = smallInstanceConfig().setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSize(3, master, slave2);
        assertClusterSizeEventually(3, slave1);
        // master, slave1, slave2

        rejectOperationsBetween(master, slave1, F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance slave3 = newHazelcastInstance(config);
        // master, slave1, slave2, slave3

        assertClusterSizeEventually(4, slave3, slave2);
        assertClusterSize(3, slave1);

        rejectOperationsBetween(master, asList(slave1, slave2), F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance slave4 = newHazelcastInstance(config);
        // master, slave1, slave2, slave3, slave4

        assertClusterSizeEventually(5, slave4, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        rejectOperationsBetween(master, asList(slave1, slave2, slave3), F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance slave5 = newHazelcastInstance(config);
        // master, slave1, slave2, slave3, slave4, slave5

        assertClusterSize(6, slave5);
        assertClusterSizeEventually(6, slave4);
        assertClusterSizeEventually(5, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(5, slave1, slave2, slave3);

        Address newMasterAddress = Accessors.getAddress(slave1);
        assertMasterAddress(newMasterAddress, slave2, slave3, slave4, slave5);
    }

    @Test
    public void master_candidate_and_new_member_splits_on_master_failure() {
        Config config = smallInstanceConfig().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);

        assertClusterSize(2, master, slave1);

        rejectOperationsBetween(master, slave1, F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance slave2 = newHazelcastInstance(config);
        assertClusterSize(3, master);
        assertClusterSize(3, slave2);
        assertClusterSize(2, slave1);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(1, slave1);
        assertClusterSizeEventually(1, slave2);

        assertMasterAddress(Accessors.getAddress(slave1), slave1);
        assertMasterAddress(Accessors.getAddress(slave2), slave2);
    }

    @Test
    public void slave_splits_and_eventually_merges_back() {
        Config config = smallInstanceConfig();
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        final HazelcastInstance member1 = newHazelcastInstance(config);
        final HazelcastInstance member2 = newHazelcastInstance(config);
        final HazelcastInstance member3 = newHazelcastInstance(config);

        assertClusterSize(3, member1, member3);
        assertClusterSizeEventually(3, member2);

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        member3.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleState.MERGED) {
                mergeLatch.countDown();
            }
        });

        // prevent heartbeats to member3 to prevent suspicion to be removed
        dropOperationsBetween(member1, member3, F_ID, singletonList(HEARTBEAT));
        dropOperationsBetween(member2, member3, F_ID, singletonList(HEARTBEAT));
        dropOperationsFrom(member3, F_ID, singletonList(SPLIT_BRAIN_MERGE_VALIDATION));
        suspectMember(member3, member1);
        suspectMember(member3, member2);
        assertClusterSizeEventually(1, member3);

        resetPacketFiltersFrom(member1);
        resetPacketFiltersFrom(member2);
        resetPacketFiltersFrom(member3);
        assertOpenEventually(mergeLatch);

        assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member3));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member2));
            }
        });
    }

    @Test
    public void masterCandidate_canGracefullyShutdown_whenMasterShutdown() throws Exception {
        masterCandidate_canGracefullyShutdown_whenMasterGoesDown(false);
    }

    @Test
    public void masterCandidate_canGracefullyShutdown_whenMasterCrashes() throws Exception {
        masterCandidate_canGracefullyShutdown_whenMasterGoesDown(true);
    }

    private void masterCandidate_canGracefullyShutdown_whenMasterGoesDown(boolean terminate) throws Exception {
        Config config = smallInstanceConfig();
        // slow down the migrations
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "12");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        final HazelcastInstance masterCandidate = factory.newHazelcastInstance(config);
        HazelcastInstance slave = factory.newHazelcastInstance(config);

        warmUpPartitions(master, masterCandidate, slave);

        Future shutdownF = spawn(masterCandidate::shutdown);

        sleepSeconds(2);
        if (terminate) {
            terminateInstance(master);
        } else {
            master.shutdown();
        }

        shutdownF.get();
        assertClusterSizeEventually(1, slave);
    }

    @Test
    public void secondMastershipClaimByYounger_shouldRetry_when_firstMastershipClaimByElder_accepted() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");

        HazelcastInstance member1 = newHazelcastInstance(config);
        final HazelcastInstance member2 = newHazelcastInstance(config);
        HazelcastInstance member3 = newHazelcastInstance(smallInstanceConfig()
                .setProperty(ClusterProperty.MASTERSHIP_CLAIM_TIMEOUT_SECONDS.getName(), "10"));
        final HazelcastInstance member4 = newHazelcastInstance(config);

        assertClusterSize(4, member1, member4);
        assertClusterSizeEventually(4, member2, member3);

        dropOperationsFrom(member1, F_ID, asList(MEMBER_INFO_UPDATE, HEARTBEAT));
        dropOperationsFrom(member2, F_ID, singletonList(FETCH_MEMBER_LIST_STATE));

        // If we allow explicit suspicions from member3, when member4 sends a heartbeat to member3
        // after member3 splits from the cluster, member3 will send an explicit suspicion to member4
        // and member4 will start its own mastership claim.
        dropOperationsFrom(member3, F_ID, asList(FETCH_MEMBER_LIST_STATE, EXPLICIT_SUSPICION));

        suspectMember(member2, member3);
        suspectMember(member3, member2);
        suspectMember(member3, member1);
        suspectMember(member2, member1);

        suspectMember(member4, member1);
        suspectMember(member4, member2);

        // member2 will complete mastership claim, but member4 won't learn new member list
        dropOperationsFrom(member2, F_ID, singletonList(MEMBER_INFO_UPDATE));
        // member4 should accept member2 as master during mastership claim
        assertMasterAddressEventually(Accessors.getAddress(member2), member4);
        resetPacketFiltersFrom(member3);
        // member3 will be split when master claim timeouts
        assertClusterSizeEventually(1, member3);

        // member4 will learn member list
        resetPacketFiltersFrom(member2);
        assertClusterSizeEventually(2, member2, member4);
        assertMemberViewsAreSame(getMemberMap(member2), getMemberMap(member4));

        resetPacketFiltersFrom(member1);
        assertClusterSizeEventually(1, member1);
    }

    @Test
    public void secondMastershipClaimByElder_shouldFail_when_firstMastershipClaimByYounger_accepted() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");

        HazelcastInstance member1 = newHazelcastInstance(config);
        final HazelcastInstance member2 = newHazelcastInstance(config);
        final HazelcastInstance member3 = newHazelcastInstance(config);
        final HazelcastInstance member4 = newHazelcastInstance(config);

        assertClusterSize(4, member1, member4);
        assertClusterSizeEventually(4, member2, member3);

        dropOperationsFrom(member1, F_ID, asList(MEMBER_INFO_UPDATE, HEARTBEAT));
        dropOperationsFrom(member2, F_ID, asList(FETCH_MEMBER_LIST_STATE, HEARTBEAT));
        dropOperationsFrom(member3, F_ID, singletonList(FETCH_MEMBER_LIST_STATE));

        suspectMember(member2, member3);
        suspectMember(member3, member2);
        suspectMember(member3, member1);
        suspectMember(member2, member1);

        suspectMember(member4, member1);
        suspectMember(member4, member2);

        // member3 will complete mastership claim, but member4 won't learn new member list
        dropOperationsFrom(member3, F_ID, singletonList(MEMBER_INFO_UPDATE));
        // member4 should accept member3 as master during mastership claim
        assertMasterAddressEventually(Accessors.getAddress(member3), member4);
        resetPacketFiltersFrom(member2);
        // member2 will be split when master claim timeouts
        assertClusterSizeEventually(1, member2);

        // member4 will learn member list
        resetPacketFiltersFrom(member3);
        assertClusterSizeEventually(2, member3, member4);
        assertMemberViewsAreSame(getMemberMap(member3), getMemberMap(member4));

        resetPacketFiltersFrom(member1);
        assertClusterSizeEventually(1, member1);
    }

    @Test
    public void test_whenNodesStartedTerminatedConcurrently() {
        newHazelcastInstance();

        for (int i = 0; i < 3; i++) {
            startInstancesConcurrently(4);
            assertClusterSizeEventually(i + 5, getAllHazelcastInstances());
            terminateRandomInstancesConcurrently(3);

            HazelcastInstance[] instances = getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
            assertEquals(i + 2, instances.length);

            for (HazelcastInstance instance : instances) {
                assertClusterSizeEventually(instances.length, instance);
                assertMemberViewsAreSame(getMemberMap(instances[0]), getMemberMap(instance));
            }
        }
    }

    private void startInstancesConcurrently(int count) {
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                newHazelcastInstance();
                latch.countDown();
            }).start();
        }
        assertOpenEventually(latch);
    }

    private void terminateRandomInstancesConcurrently(int count) {
        List<HazelcastInstance> instances = new ArrayList<>(getAllHazelcastInstances());
        assertThat(instances.size(), greaterThanOrEqualTo(count));

        Collections.shuffle(instances);
        instances = instances.subList(0, count);

        final CountDownLatch latch = new CountDownLatch(count);
        for (final HazelcastInstance hz : instances) {
            new Thread(() -> {
                terminateInstance(hz);
                latch.countDown();
            }).start();
        }
        assertOpenEventually(latch);
    }

    HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(smallInstanceConfig());
    }

    HazelcastInstance newHazelcastInstance(Config config) {
        config.setProperty(ClusterProperty.HEARTBEAT_FAILURE_DETECTOR_TYPE.getName(), failureDetectorType.toString());
        return factory.newHazelcastInstance(config);
    }

    Collection<HazelcastInstance> getAllHazelcastInstances() {
        return factory.getAllHazelcastInstances();
    }
}
