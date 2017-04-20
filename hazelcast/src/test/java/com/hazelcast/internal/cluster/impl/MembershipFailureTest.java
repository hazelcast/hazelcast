/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.FETCH_MEMBER_LIST_STATE;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT_COMPLAINT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MASTER_CONFIRM;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.resetPacketFiltersFrom;
import static com.hazelcast.spi.properties.GroupProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipFailureTest extends HazelcastTestSupport {

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

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        if (terminate) {
            terminateInstance(slave1);
        } else {
            slave1.shutdown();
        }

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(master), master);
        assertMaster(getAddress(master), slave2);
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

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        if (terminate) {
            terminateInstance(master);
        } else {
            master.shutdown();
        }

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
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

        assertClusterSizeEventually(4, master);
        assertClusterSizeEventually(4, masterCandidate);
        assertClusterSizeEventually(4, slave1);

        if (simultaneousCrash) {
            terminateInstanceAsync(master);
            terminateInstanceAsync(masterCandidate);
        } else {
            terminateInstance(master);
            terminateInstance(masterCandidate);
        }

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    private static void terminateInstanceAsync(final HazelcastInstance master) {
        spawn(new Runnable() {
            @Override
            public void run() {
                terminateInstance(master);
            }
        });
    }

    @Test
    public void slaveCrash_duringMastershipClaim() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance masterCandidate = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(4, master);
        assertClusterSizeEventually(4, masterCandidate);
        assertClusterSizeEventually(4, slave1);
        assertClusterSizeEventually(4, slave2);

        // drop FETCH_MEMBER_LIST_STATE packets to block mastership claim process
        dropOperationsBetween(masterCandidate, slave1, FETCH_MEMBER_LIST_STATE);

        terminateInstance(master);

        final ClusterServiceImpl clusterService = getNode(masterCandidate).getClusterService();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(clusterService.getClusterJoinManager().isMastershipClaimInProgress());
            }
        });
        
        sleepSeconds(3);
        terminateInstance(slave1);

        assertClusterSizeEventually(2, masterCandidate);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(masterCandidate), masterCandidate);
        assertMaster(getAddress(masterCandidate), slave2);
        assertMemberViewsAreSame(getMemberMap(masterCandidate), getMemberMap(slave2));
    }

    @Test
    public void masterCandidateCrash_duringMastershipClaim() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance masterCandidate = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(4, master);
        assertClusterSizeEventually(4, masterCandidate);
        assertClusterSizeEventually(4, slave1);
        assertClusterSizeEventually(4, slave2);

        // drop FETCH_MEMBER_LIST_STATE packets to block mastership claim process
        dropOperationsBetween(masterCandidate, slave1, FETCH_MEMBER_LIST_STATE);

        terminateInstance(master);

        final ClusterServiceImpl clusterService = getNode(masterCandidate).getClusterService();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(clusterService.getClusterJoinManager().isMastershipClaimInProgress());
            }
        });

        sleepSeconds(3);
        terminateInstance(masterCandidate);

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void slave_heartbeat_timeout() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                     .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsFrom(slave2, HEARTBEAT);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(1, slave2);
    }

    @Test
    public void master_heartbeat_timeout() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                    .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                                    .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "3");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);


        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsFrom(master, HEARTBEAT);

        dropOperationsFrom(master, HEARTBEAT);
        dropOperationsFrom(slave1, HEARTBEAT_COMPLAINT);
        dropOperationsFrom(slave2, HEARTBEAT_COMPLAINT);

        assertClusterSizeEventually(1, master);
        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);
    }

    @Test
    public void heartbeat_not_sent_to_suspected_member() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "10")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        final HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        // prevent heartbeat from master to slave to prevent suspect to be removed
        dropOperationsBetween(master, slave1, HEARTBEAT);
        suspectMember(slave1, master);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave2);
        assertClusterSizeEventually(1, slave1);
    }

    @Test
    public void master_confirmation_not_sent_to_suspected_master() {
        Config config = new Config().setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), "10")
                .setProperty(MASTER_CONFIRMATION_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        final HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        // prevent heartbeat from master to slave to prevent suspect to be removed
        dropOperationsBetween(master, slave1, HEARTBEAT);
        suspectMember(slave1, master);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave2);
        assertClusterSizeEventually(1, slave1);
    }

    @Test
    public void slave_heartbeat_removes_suspicion() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "10")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        final HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsBetween(slave2, slave1, HEARTBEAT);

        final MembershipManager membershipManager = getNode(slave1).getClusterService().getMembershipManager();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(membershipManager.isMemberSuspected(getAddress(slave2)));
            }
        });

        resetPacketFiltersFrom(slave2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(membershipManager.isMemberSuspected(getAddress(slave2)));
            }
        });
    }

    @Test
    public void slave_master_confirmation_timeout() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                    .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                                    .setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), "15")
                                    .setProperty(MASTER_CONFIRMATION_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsFrom(slave2, MASTER_CONFIRM);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(1, slave2);
    }

    @Test
    public void slave_master_confirmation_timeout_explicit_suspicion() {
        Config config1 = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                    .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                                    .setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), "15")
                                    .setProperty(MASTER_CONFIRMATION_INTERVAL_SECONDS.getName(), "1");

        String infiniteTimeout = Integer.toString(Integer.MAX_VALUE);
        Config config2 = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), infiniteTimeout)
                                     .setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), infiniteTimeout);

        HazelcastInstance master = newHazelcastInstance(config1);
        HazelcastInstance slave1 = newHazelcastInstance(config1);
        HazelcastInstance slave2 = newHazelcastInstance(config2);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsBetween(master, slave2, MEMBER_INFO_UPDATE);
        dropOperationsFrom(slave2, MASTER_CONFIRM, HEARTBEAT);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);

        dropOperationsFrom(slave2, HEARTBEAT);
        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(slave2);
        clusterService.getClusterHeartbeatManager().sendMasterConfirmation();

        assertClusterSizeEventually(1, slave2);
    }

    @Test
    public void slave_receives_member_list_from_non_master() {
        String infiniteTimeout = Integer.toString(Integer.MAX_VALUE);
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), infiniteTimeout)
                                    .setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), infiniteTimeout)
                                    .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");

        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);
        HazelcastInstance slave3 = newHazelcastInstance(config);

        assertClusterSizeEventually(4, master);
        assertClusterSizeEventually(4, slave1);
        assertClusterSizeEventually(4, slave2);
        assertClusterSizeEventually(4, slave3);

        dropOperationsFrom(master, HEARTBEAT, MASTER_CONFIRM);
        dropOperationsFrom(slave1, HEARTBEAT, MASTER_CONFIRM);
        dropOperationsFrom(slave2, HEARTBEAT, MASTER_CONFIRM);
        dropOperationsFrom(slave3, HEARTBEAT, MASTER_CONFIRM);

        suspectMember(slave2, master);
        suspectMember(slave2, slave1);
        suspectMember(slave3, master);
        suspectMember(slave3, slave1);

        assertClusterSizeEventually(2, slave2);
        assertClusterSizeEventually(2, slave3);

        assertMemberViewsAreSame(getMemberMap(slave2), getMemberMap(slave3));

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);

        assertMemberViewsAreSame(getMemberMap(master), getMemberMap(slave1));
    }

    @Test
    public void master_candidate_has_stale_member_list() {
        Config config = new Config().setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);

        HazelcastInstance slave2 = newHazelcastInstance(config);
        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsBetween(master, slave1, MEMBER_INFO_UPDATE);

        HazelcastInstance slave3 = newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);
        assertClusterSizeEventually(3, slave3);

        Address newMasterAddress = getAddress(slave1);
        assertEquals(newMasterAddress, getNode(slave1).getMasterAddress());
        assertEquals(newMasterAddress, getNode(slave2).getMasterAddress());
        assertEquals(newMasterAddress, getNode(slave3).getMasterAddress());
    }

    @Test
    public void master_candidate_discovers_member_list_recursively() {
        Config config = new Config().setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);

        HazelcastInstance slave2 = newHazelcastInstance(config);
        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);
        // master, slave1, slave2

        dropOperationsBetween(master, slave1, MEMBER_INFO_UPDATE);

        HazelcastInstance slave3 = newHazelcastInstance(config);
        // master, slave1, slave2, slave3

        assertClusterSizeEventually(4, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        dropOperationsBetween(master, asList(slave1, slave2), MEMBER_INFO_UPDATE);

        HazelcastInstance slave4 = newHazelcastInstance(config);
        // master, slave1, slave2, slave3, slave4

        assertClusterSizeEventually(5, slave4);
        assertClusterSizeEventually(5, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        dropOperationsBetween(master, asList(slave1, slave2, slave3), MEMBER_INFO_UPDATE);

        HazelcastInstance slave5 = newHazelcastInstance(config);
        // master, slave1, slave2, slave3, slave4, slave5

        assertClusterSizeEventually(6, slave5);
        assertClusterSizeEventually(6, slave4);
        assertClusterSizeEventually(5, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(5, slave1);
        assertClusterSizeEventually(5, slave2);
        assertClusterSizeEventually(5, slave3);

        Address newMasterAddress = getAddress(slave1);
        assertEquals(newMasterAddress, getNode(slave2).getMasterAddress());
        assertEquals(newMasterAddress, getNode(slave3).getMasterAddress());
        assertEquals(newMasterAddress, getNode(slave4).getMasterAddress());
        assertEquals(newMasterAddress, getNode(slave5).getMasterAddress());
    }

    @Test
    public void master_candidate_and_new_member_splits_on_master_failure() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                    .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                                    .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);

        dropOperationsBetween(master, slave1, MEMBER_INFO_UPDATE);

        HazelcastInstance slave2 = newHazelcastInstance(config);
        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave2);
        assertClusterSize(2, slave1);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(1, slave1);
        assertClusterSizeEventually(1, slave2);

        assertEquals(getAddress(slave1), getNode(slave1).getMasterAddress());
        assertEquals(getAddress(slave2), getNode(slave2).getMasterAddress());
    }

    @Test
    public void slave_splits_and_eventually_merges_back() {
        Config config = new Config();
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "15")
              .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        final HazelcastInstance member1 = newHazelcastInstance(config);
        final HazelcastInstance member2 = newHazelcastInstance(config);
        final HazelcastInstance member3 = newHazelcastInstance(config);

        assertClusterSizeEventually(3, member1);
        assertClusterSizeEventually(3, member2);
        assertClusterSizeEventually(3, member3);

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        member3.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
                    mergeLatch.countDown();
                }
            }
        });

        suspectMember(member3, member1);
        suspectMember(member3, member2);
        assertClusterSizeEventually(1, member3);

        assertOpenEventually(mergeLatch);
        assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member2));
        assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member3));
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
        Config config = new Config();
        // slow down the migrations
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "12");

        HazelcastInstance master = factory.newHazelcastInstance(config);
        final HazelcastInstance masterCandidate = factory.newHazelcastInstance(config);
        HazelcastInstance slave = factory.newHazelcastInstance(config);

        warmUpPartitions(master, masterCandidate, slave);

        Future shutdownF = spawn(new Runnable() {
            @Override
            public void run() {
                masterCandidate.shutdown();
            }
        });

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
        Config config = new Config();
        config.setProperty(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");

        HazelcastInstance member1 = newHazelcastInstance(config);
        final HazelcastInstance member2 = newHazelcastInstance(config);
        HazelcastInstance member3 = newHazelcastInstance(new Config()
                .setProperty(GroupProperty.MASTERSHIP_CLAIM_TIMEOUT_SECONDS.getName(), "10"));
        final HazelcastInstance member4 = newHazelcastInstance(config);

        assertClusterSizeEventually(4, member1);
        assertClusterSizeEventually(4, member2);
        assertClusterSizeEventually(4, member3);
        assertClusterSizeEventually(4, member4);

        dropOperationsFrom(member1, MEMBER_INFO_UPDATE, HEARTBEAT);
        dropOperationsFrom(member2, FETCH_MEMBER_LIST_STATE);
        dropOperationsFrom(member3, FETCH_MEMBER_LIST_STATE);

        suspectMember(member2, member3);
        suspectMember(member3, member2);
        suspectMember(member3, member1);
        suspectMember(member2, member1);

        suspectMember(member4, member1);
        suspectMember(member4, member2);

        // member2 will complete mastership claim, but member4 won't learn new member list
        dropOperationsFrom(member2, MEMBER_INFO_UPDATE);
        // member4 should accept member2 as master during mastership claim
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMaster(getAddress(member2), member4);
            }
        });
        resetPacketFiltersFrom(member3);
        // member3 will be split when master claim timeouts
        assertClusterSizeEventually(1, member3);

        // member4 will learn member list
        resetPacketFiltersFrom(member2);
        assertClusterSizeEventually(2, member2);
        assertClusterSizeEventually(2, member4);
        assertMemberViewsAreSame(getMemberMap(member2), getMemberMap(member4));

        resetPacketFiltersFrom(member1);
        assertClusterSizeEventually(1, member1);
    }

    @Test
    public void secondMastershipClaimByElder_shouldFail_when_firstMastershipClaimByYounger_accepted() {
        Config config = new Config();
        config.setProperty(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");

        HazelcastInstance member1 = newHazelcastInstance(config);
        final HazelcastInstance member2 = newHazelcastInstance(config);
        final HazelcastInstance member3 = newHazelcastInstance(config);
        final HazelcastInstance member4 = newHazelcastInstance(config);

        assertClusterSizeEventually(4, member1);
        assertClusterSizeEventually(4, member2);
        assertClusterSizeEventually(4, member3);
        assertClusterSizeEventually(4, member4);

        dropOperationsFrom(member1, MEMBER_INFO_UPDATE, HEARTBEAT);
        dropOperationsFrom(member2, FETCH_MEMBER_LIST_STATE, HEARTBEAT);
        dropOperationsFrom(member3, FETCH_MEMBER_LIST_STATE);

        suspectMember(member2, member3);
        suspectMember(member3, member2);
        suspectMember(member3, member1);
        suspectMember(member2, member1);

        suspectMember(member4, member1);
        suspectMember(member4, member2);

        // member3 will complete mastership claim, but member4 won't learn new member list
        dropOperationsFrom(member3, MEMBER_INFO_UPDATE);
        // member4 should accept member3 as master during mastership claim
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMaster(getAddress(member3), member4);
            }
        });
        resetPacketFiltersFrom(member2);
        // member2 will be split when master claim timeouts
        assertClusterSizeEventually(1, member2);

        // member4 will learn member list
        resetPacketFiltersFrom(member3);
        assertClusterSizeEventually(2, member3);
        assertClusterSizeEventually(2, member4);
        assertMemberViewsAreSame(getMemberMap(member3), getMemberMap(member4));

        resetPacketFiltersFrom(member1);
        assertClusterSizeEventually(1, member1);
    }

    @Test
    public void test_whenNodesStartedTerminatedConcurrently() throws InterruptedException {
        Config config = new Config();

        newHazelcastInstance(config);

        for (int i = 0; i < 3; i++) {
            startInstancesConcurrently(config, 4);
            terminateRandomInstancesConcurrently(3);

            HazelcastInstance[] instances = getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
            assertEquals(i + 2, instances.length);

            for (HazelcastInstance instance : instances) {
                assertClusterSizeEventually(instances.length, instance);
                assertMemberViewsAreSame(getMemberMap(instances[0]), getMemberMap(instance));
            }
        }
    }

    private void startInstancesConcurrently(final Config config, int count) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread() {
                public void run() {
                    newHazelcastInstance(config);
                    latch.countDown();
                }
            }.start();
        }
        assertTrue(latch.await(2, TimeUnit.MINUTES));
    }

    private void terminateRandomInstancesConcurrently(int count) throws InterruptedException {
        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(getAllHazelcastInstances());
        assertThat(instances.size(), greaterThanOrEqualTo(count));

        Collections.shuffle(instances);
        instances = instances.subList(0, count);

        final CountDownLatch latch = new CountDownLatch(count);
        for (final HazelcastInstance hz : instances) {
            new Thread() {
                public void run() {
                    TestUtil.terminateInstance(hz);
                    latch.countDown();
                }
            }.start();
        }
        assertTrue(latch.await(2, TimeUnit.MINUTES));
    }

    HazelcastInstance newHazelcastInstance() {
        return factory.newHazelcastInstance();
    }

    HazelcastInstance newHazelcastInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }

    Collection<HazelcastInstance> getAllHazelcastInstances() {
        return factory.getAllHazelcastInstances();
    }

    static void assertMaster(Address masterAddress, HazelcastInstance instance) {
        assertEquals(masterAddress, getNode(instance).getMasterAddress());
    }

    static void suspectMember(HazelcastInstance suspectingInstance, HazelcastInstance suspectedInstance) {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(suspectingInstance);
        Member suspectedMember = suspectedInstance.getCluster().getLocalMember();
        clusterService.suspectMember(suspectedMember, "test", false);
    }

}
