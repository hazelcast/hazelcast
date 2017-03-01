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
import com.hazelcast.nio.Address;
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

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.FETCH_MEMBER_LIST_STATE;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MASTER_CONFIRM;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.spi.properties.GroupProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipFailureTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    // TODO: add membership failure tests
    // ✔ graceful slave shutdown
    // ✔ graceful master shutdown
    // ✔ master removes slave because of heartbeat timeout
    // ✔ slaves remove master because of heartbeat timeout
    // ✔ master removes slave because of master confirm timeout
    // ✔ slave member failure detected by master
    // - slave member suspected by others and its failure eventually detected by master
    // ✔ master member failure detected by others
    // ✔ master and master-candidate fail simultaneously
    // ✔ master fails when master-candidate doesn't have the most recent member list
    // ✔ partial network failure: multiple master claims
    // ✔ member failures during mastership claim
    // ✔ partial split: 3 members [A, B, C] partially split into [A, B, C] and [C], then eventually merge
    // - so on...

    @Test
    public void slave_shutdown() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        slave1.shutdown();

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(master), master);
        assertMaster(getAddress(master), slave2);
        assertMemberViewsAreSame(getMemberMap(master), getMemberMap(slave2));
    }

    @Test
    public void master_shutdown() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        master.shutdown();

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void slave_crash() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        terminateInstance(slave1);

        assertMaster(getAddress(master), master);
        assertMaster(getAddress(master), slave2);
        assertMemberViewsAreSame(getMemberMap(master), getMemberMap(slave2));
    }

    @Test
    public void master_crash() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        terminateInstance(master);

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void masterAndMasterCandidate_crashSequentially() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance masterCandidate = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(4, master);
        assertClusterSizeEventually(4, masterCandidate);
        assertClusterSizeEventually(4, slave1);

        terminateInstance(master);
        terminateInstance(masterCandidate);

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void masterAndMasterCandidate_crashSimultaneously() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance masterCandidate = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(4, master);
        assertClusterSizeEventually(4, masterCandidate);
        assertClusterSizeEventually(4, slave1);

        terminateInstanceAsync(master);
        terminateInstanceAsync(masterCandidate);

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

        dropOperationsFrom(slave2, HEARTBEAT);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

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

        dropOperationsFrom(master, HEARTBEAT);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        assertClusterSizeEventually(1, master);
        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);
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

        dropOperationsFrom(slave2, MASTER_CONFIRM);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

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
                                     .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), infiniteTimeout)
                                     .setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), infiniteTimeout)
                                     .setProperty(MASTER_CONFIRMATION_INTERVAL_SECONDS.getName(), infiniteTimeout);

        HazelcastInstance master = newHazelcastInstance(config1);
        HazelcastInstance slave1 = newHazelcastInstance(config1);
        HazelcastInstance slave2 = newHazelcastInstance(config2);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsBetween(master, slave2, MEMBER_INFO_UPDATE);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);

        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(slave2);
        clusterService.getClusterHeartbeatManager().sendMasterConfirmation();

        assertClusterSizeEventually(1, slave2);
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
    public void multiple_master_claims() {
        Config config = new Config();
        HazelcastInstance member1 = newHazelcastInstance(config);
        final HazelcastInstance member2 = newHazelcastInstance(config);
        final HazelcastInstance member3 = newHazelcastInstance(config);
        final HazelcastInstance member4 = newHazelcastInstance(config);
        final HazelcastInstance member5 = newHazelcastInstance(config);

        assertClusterSizeEventually(5, member1);
        assertClusterSizeEventually(5, member2);
        assertClusterSizeEventually(5, member3);
        assertClusterSizeEventually(5, member4);
        assertClusterSizeEventually(5, member5);

        suspectMember(member3, member2);
        suspectMember(member5, member2);

        member1.getLifecycleService().terminate();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertClusterSize(2, member2);
                assertClusterSize(2, member4);
                assertMemberViewsAreSame(getMemberMap(member2), getMemberMap(member4));

                assertClusterSize(2, member3);
                assertClusterSize(2, member5);
                assertMemberViewsAreSame(getMemberMap(member3), getMemberMap(member5));
            }
        });
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

    HazelcastInstance newHazelcastInstance() {
        return factory.newHazelcastInstance();
    }

    HazelcastInstance newHazelcastInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }

    private static void assertMaster(Address masterAddress, HazelcastInstance instance) {
        assertEquals(masterAddress, getNode(instance).getMasterAddress());
    }

    private void suspectMember(HazelcastInstance suspectingInstance, HazelcastInstance suspectedInstance) {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(suspectingInstance);
        Member suspectedMember = suspectedInstance.getCluster().getLocalMember();
        clusterService.suspectAddress(suspectedMember.getAddress(), suspectedMember.getUuid(), "test", false);
    }

}
