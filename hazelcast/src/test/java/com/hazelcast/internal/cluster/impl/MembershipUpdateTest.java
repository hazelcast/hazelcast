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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
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

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.instance.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.FINALIZE_JOIN;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.spi.properties.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.test.PacketFiltersUtil.delayOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipUpdateTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void sequential_member_join() {
        HazelcastInstance[] instances = new HazelcastInstance[4];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        MemberMap referenceMemberMap = getMemberMap(instances[0]);
        // version = number of started members
        assertEquals(instances.length, referenceMemberMap.getVersion());

        for (HazelcastInstance instance : instances) {
            MemberMap memberMap = getMemberMap(instance);
            assertMemberViewsAreSame(referenceMemberMap, memberMap);
        }
    }

    @Test
    public void parallel_member_join() {
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(4);
        for (int i = 0; i < instances.length(); i++) {
            final int ix = i;
            spawn(new Runnable() {
                @Override
                public void run() {
                    instances.set(ix, factory.newHazelcastInstance());
                }
            });
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < instances.length(); i++) {
                    HazelcastInstance instance = instances.get(i);
                    assertNotNull(instance);
                    assertClusterSize(instances.length(), instance);
                }
            }
        });

        MemberMap referenceMemberMap = getMemberMap(instances.get(0));
        // version = number of started members
        assertEquals(instances.length(), referenceMemberMap.getVersion());

        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            MemberMap memberMap = getMemberMap(instance);
            assertMemberViewsAreSame(referenceMemberMap, memberMap);
        }
    }

    @Test
    public void parallel_member_join_whenPostJoinOperationPresent() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        final Config config = getConfigWithService(new PostJoinAwareServiceImpl(latch), PostJoinAwareServiceImpl.SERVICE_NAME);

        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(6);
        for (int i = 0; i < instances.length(); i++) {
            final int ix = i;
            spawn(new Runnable() {
                @Override
                public void run() {
                    instances.set(ix, factory.newHazelcastInstance(config));
                }
            });
        }

        // just a random latency
        sleepSeconds(3);
        latch.countDown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < instances.length(); i++) {
                    HazelcastInstance instance = instances.get(i);
                    assertNotNull(instance);
                    assertClusterSize(instances.length(), instance);
                }
            }
        });
    }

    @Test
    public void parallel_member_join_whenPreJoinOperationPresent() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        PreJoinAwareServiceImpl service = new PreJoinAwareServiceImpl(latch);
        final Config config = getConfigWithService(service, PreJoinAwareServiceImpl.SERVICE_NAME);

        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(6);
        for (int i = 0; i < instances.length(); i++) {
            final int ix = i;
            spawn(new Runnable() {
                @Override
                public void run() {
                    instances.set(ix, factory.newHazelcastInstance(config));
                }
            });
        }

        sleepSeconds(3);
        latch.countDown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < instances.length(); i++) {
                    HazelcastInstance instance = instances.get(i);
                    assertNotNull(instance);
                    assertClusterSize(instances.length(), instance);
                }
            }
        });
    }

    @Test
    public void sequential_member_join_and_removal() {
        HazelcastInstance[] instances = new HazelcastInstance[4];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        instances[instances.length - 1].shutdown();

        for (int i = 0; i < instances.length - 1; i++) {
            HazelcastInstance instance = instances[i];
            assertClusterSizeEventually(instances.length - 1, instance);
        }

        MemberMap referenceMemberMap = getMemberMap(instances[0]);
        // version = number of started members + 1 removal
        assertEquals(instances.length + 1, referenceMemberMap.getVersion());

        for (int i = 0; i < instances.length - 1; i++) {
            HazelcastInstance instance = instances[i];
            MemberMap memberMap = getMemberMap(instance);
            assertMemberViewsAreSame(referenceMemberMap, memberMap);
        }
    }

    @Test
    public void sequential_member_join_and_restart() {
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        instances[instances.length - 1].shutdown();
        instances[instances.length - 1] = factory.newHazelcastInstance();

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(instances.length, instance);
        }

        MemberMap referenceMemberMap = getMemberMap(instances[0]);
        // version = number of started members + 1 removal + 1 start
        assertEquals(instances.length + 2, referenceMemberMap.getVersion());

        for (HazelcastInstance instance : instances) {
            MemberMap memberMap = getMemberMap(instance);
            assertMemberViewsAreSame(referenceMemberMap, memberMap);
        }
    }

    @Test
    public void parallel_member_join_and_removal() {
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(4);
        for (int i = 0; i < instances.length(); i++) {
            final int ix = i;
            spawn(new Runnable() {
                @Override
                public void run() {
                    instances.set(ix, factory.newHazelcastInstance());
                }
            });
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < instances.length(); i++) {
                    HazelcastInstance instance = instances.get(i);
                    assertNotNull(instance);
                    assertClusterSize(instances.length(), instance);
                }
            }
        });

        for (int i = 0; i < instances.length(); i++) {
            if (getNode(instances.get(i)).isMaster()) {
                continue;
            }

            instances.getAndSet(i, null).shutdown();
            break;
        }

        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            if (instance != null) {
                assertClusterSizeEventually(instances.length() - 1, instance);
            }
        }

        HazelcastInstance master = null;
        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            if (instance != null && getNode(instance).isMaster()) {
                master = instance;
                break;
            }
        }

        assertNotNull(master);

        MemberMap referenceMemberMap = getMemberMap(master);
        // version = number of started members + 1 removal
        assertEquals(instances.length() + 1, referenceMemberMap.getVersion());

        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            if (instance != null) {
                MemberMap memberMap = getMemberMap(instance);
                assertMemberViewsAreSame(referenceMemberMap, memberMap);
            }
        }
    }

    @Test
    public void parallel_member_join_and_restart() {
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(3);
        for (int i = 0; i < instances.length(); i++) {
            final int ix = i;
            spawn(new Runnable() {
                @Override
                public void run() {
                    instances.set(ix, factory.newHazelcastInstance());
                }
            });
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < instances.length(); i++) {
                    HazelcastInstance instance = instances.get(i);
                    assertNotNull(instance);
                    assertClusterSize(instances.length(), instance);
                }
            }
        });

        for (int i = 0; i < instances.length(); i++) {
            if (getNode(instances.get(i)).isMaster()) {
                continue;
            }

            instances.get(i).shutdown();
            instances.set(i, factory.newHazelcastInstance());
            break;
        }

        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            assertClusterSizeEventually(instances.length(), instance);
        }

        HazelcastInstance master = null;
        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            if (getNode(instances.get(i)).isMaster()) {
                master = instance;
                break;
            }
        }

        assertNotNull(master);

        MemberMap referenceMemberMap = getMemberMap(master);
        // version = number of started members + 1 removal + 1 start
        assertEquals(instances.length() + 2, referenceMemberMap.getVersion());

        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            MemberMap memberMap = getMemberMap(instance);
            assertMemberViewsAreSame(referenceMemberMap, memberMap);
        }
    }

    @Test
    public void memberListsConverge_whenMemberUpdateMissed() {
        Config config = new Config();

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        assertClusterSize(2, hz1, hz2);

        rejectOperationsFrom(hz1, F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSize(3, hz1, hz3);
        assertClusterSize(2, hz2);

        resetPacketFiltersFrom(hz1);
        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(hz1);
        clusterService.getMembershipManager().sendMemberListToMember(getAddress(hz2));

        assertClusterSizeEventually(3, hz2);

        MemberMap referenceMemberMap = getMemberMap(hz1);
        assertMemberViewsAreSame(referenceMemberMap, getMemberMap(hz2));
        assertMemberViewsAreSame(referenceMemberMap, getMemberMap(hz3));
    }

    @Test
    public void memberListsConverge_whenMemberUpdateMissed_withPeriodicUpdates() {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        assertClusterSize(2, hz1, hz2);

        rejectOperationsFrom(hz1, F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSize(3, hz1, hz3);
        assertClusterSize(2, hz2);

        resetPacketFiltersFrom(hz1);
        assertClusterSizeEventually(3, hz2);

        MemberMap referenceMemberMap = getMemberMap(hz1);
        assertMemberViewsAreSame(referenceMemberMap, getMemberMap(hz2));
        assertMemberViewsAreSame(referenceMemberMap, getMemberMap(hz3));
    }

    @Test
    public void memberListsConverge_whenMembershipUpdatesSent_outOfOrder() {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        delayOperationsFrom(hz1, F_ID, singletonList(MEMBER_INFO_UPDATE));

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        HazelcastInstance hz4 = factory.newHazelcastInstance(config);
        HazelcastInstance hz5 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = new HazelcastInstance[]{hz1, hz2, hz3, hz4, hz5};

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(5, instance);
        }

        MemberMap referenceMemberMap = getMemberMap(hz1);
        for (HazelcastInstance instance : instances) {
            assertMemberViewsAreSame(referenceMemberMap, getMemberMap(instance));
        }
    }

    @Test
    public void memberListsConverge_whenFinalizeJoinAndMembershipUpdatesSent_outOfOrder() {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        delayOperationsFrom(hz1, F_ID, asList(MEMBER_INFO_UPDATE, FINALIZE_JOIN));

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);
        HazelcastInstance hz4 = factory.newHazelcastInstance(config);
        HazelcastInstance hz5 = factory.newHazelcastInstance(config);

        HazelcastInstance[] instances = new HazelcastInstance[]{hz1, hz2, hz3, hz4, hz5};

        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(5, instance);
        }

        MemberMap referenceMemberMap = getMemberMap(hz1);
        for (HazelcastInstance instance : instances) {
            assertMemberViewsAreSame(referenceMemberMap, getMemberMap(instance));
        }
    }

    @Test
    public void memberListsConverge_whenExistingMemberMissesMemberRemove_withPeriodicUpdates() {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        dropOperationsBetween(hz1, hz3, F_ID, singletonList(MEMBER_INFO_UPDATE));
        hz2.getLifecycleService().terminate();

        assertClusterSizeEventually(2, hz1);
        assertClusterSize(3, hz3);

        resetPacketFiltersFrom(hz1);

        assertClusterSizeEventually(2, hz3);
        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz3));
    }

    @Test
    public void memberListsConverge_whenExistingMemberMissesMemberRemove_afterNewMemberJoins() {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        dropOperationsBetween(hz1, hz3, F_ID, singletonList(MEMBER_INFO_UPDATE));
        hz2.getLifecycleService().terminate();

        assertClusterSizeEventually(2, hz1);
        assertClusterSize(3, hz3);

        resetPacketFiltersFrom(hz1);

        HazelcastInstance hz4 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz3);

        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz3));
        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz4));
    }

    @Test
    public void memberReceives_memberUpdateNotContainingItself() throws Exception {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz2);

        Node node = getNode(hz1);
        ClusterServiceImpl clusterService = node.getClusterService();
        MembershipManager membershipManager = clusterService.getMembershipManager();

        MembersView membersView = MembersView.createNew(membershipManager.getMemberListVersion() + 1,
                asList(membershipManager.getMember(getAddress(hz1)), membershipManager.getMember(getAddress(hz2))));

        Operation memberUpdate = new MembersUpdateOp(membershipManager.getMember(getAddress(hz3)).getUuid(),
                membersView, clusterService.getClusterTime(), null, true);
        memberUpdate.setCallerUuid(node.getThisUuid());

        Future<Object> future =
                node.getNodeEngine().getOperationService().invokeOnTarget(null, memberUpdate, getAddress(hz3));

        try {
            future.get();
            fail("Membership update should fail!");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void memberReceives_memberUpdateFromInvalidMaster() throws Exception {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz2);

        Node node = getNode(hz1);
        ClusterServiceImpl clusterService = node.getClusterService();
        MembershipManager membershipManager = clusterService.getMembershipManager();

        MemberInfo newMemberInfo = new MemberInfo(new Address("127.0.0.1", 6000), newUnsecureUuidString(),
                Collections.<String, Object>emptyMap(), node.getVersion());
        MembersView membersView =
                MembersView.cloneAdding(membershipManager.getMembersView(), singleton(newMemberInfo));

        Operation memberUpdate = new MembersUpdateOp(membershipManager.getMember(getAddress(hz3)).getUuid(),
                membersView, clusterService.getClusterTime(), null, true);
        NodeEngineImpl nonMasterNodeEngine = getNodeEngineImpl(hz2);
        memberUpdate.setCallerUuid(nonMasterNodeEngine.getNode().getThisUuid());
        Future<Object> future =
                nonMasterNodeEngine.getOperationService().invokeOnTarget(null, memberUpdate, getAddress(hz3));
        future.get();

        // member update should not be applied
        assertClusterSize(3, hz1, hz2, hz3);

        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz2));
        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz3));
    }

    @Test
    public void memberListOrder_shouldBeSame_whenMemberRestartedWithSameIdentity() {
        Config configMaster = new Config();
        configMaster.setProperty(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        // Needed only on master to prevent accepting stale join requests.
        // See ClusterJoinManager#checkRecentlyJoinedMemberUuidBeforeJoin(target, uuid)
        configMaster.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "5");
        final HazelcastInstance hz1 = factory.newHazelcastInstance(configMaster);

        final HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();
        HazelcastInstance hz4 = factory.newHazelcastInstance();

        assertClusterSizeEventually(4, hz2, hz3);

        rejectOperationsBetween(hz1, hz2, F_ID, singletonList(MEMBER_INFO_UPDATE));

        final MemberImpl member3 = getNode(hz3).getLocalMember();
        hz3.getLifecycleService().terminate();

        assertClusterSizeEventually(3, hz1, hz4);
        assertClusterSize(4, hz2);

        hz3 = newHazelcastInstance(new Config(), "test-instance", new StaticMemberNodeContext(factory, member3));

        assertClusterSizeEventually(4, hz1, hz4);

        resetPacketFiltersFrom(hz1);

        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz3));
        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz4));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz2));
            }
        });
    }

    @Test
    public void shouldNotProcessStaleJoinRequest() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        JoinRequest staleJoinReq = getNode(hz2).createJoinRequest(true);
        hz2.shutdown();
        assertClusterSizeEventually(1, hz1);

        ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(hz1);
        clusterService.getClusterJoinManager().handleJoinRequest(staleJoinReq, null);

        assertClusterSize(1, hz1);
    }

    // On a joining member assert that no operations are executed before pre join operations execution is completed.
    @Test
    public void noOperationExecuted_beforePreJoinOpIsDone() {
        CountDownLatch latch = new CountDownLatch(1);
        PreJoinAwareServiceImpl service = new PreJoinAwareServiceImpl(latch);
        final Config config = getConfigWithService(service, PreJoinAwareServiceImpl.SERVICE_NAME);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final Address instance2Address = factory.nextAddress();
        final OperationService operationService = getNode(instance1).getNodeEngine().getOperationService();
        // send operations from master to joining member. The master has already added the joining member to its member list
        // while the FinalizeJoinOp is being executed on joining member, so it might send operations to the joining member.
        Future sendOpsFromMaster = spawn(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        ExecutionTrackerOp op = new ExecutionTrackerOp();
                        operationService.send(op, instance2Address);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (currentThread().isInterrupted()) {
                        break;
                    }
                    LockSupport.parkNanos(1);
                }
            }
        });

        final AtomicReference<HazelcastInstance> instanceReference = new AtomicReference<HazelcastInstance>(null);
        spawn(new Runnable() {
            @Override
            public void run() {
                instanceReference.set(factory.newHazelcastInstance(instance2Address, config));
            }
        });

        sleepSeconds(10);
        // on latch countdown, the pre-join op completes
        latch.countDown();
        sleepSeconds(5);
        sendOpsFromMaster.cancel(true);
        assertFalse(service.otherOpExecutedBeforePreJoin.get());
    }

    @Test
    public void joiningMemberShouldShutdown_whenExceptionDeserializingPreJoinOp() {
        Config config = getConfigWithService(new FailingPreJoinOpService(), FailingPreJoinOpService.SERVICE_NAME);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        // joining member fails while deserializing pre-join op and should shutdown
        try {
            factory.newHazelcastInstance(config);
            fail("Second HazelcastInstance should not have started");
        } catch (IllegalStateException e) {
            // expected
        }
        assertClusterSize(1, hz1);
    }

    @Test
    public void joiningMemberShouldShutdown_whenExceptionDeserializingPostJoinOp() {
        Config config = getConfigWithService(new FailingPostJoinOpService(), FailingPostJoinOpService.SERVICE_NAME);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);

        // joining member fails while deserializing post-join op and should shutdown
        try {
            factory.newHazelcastInstance(config);
            fail("Second HazelcastInstance should not have started");
        } catch (IllegalStateException e) {
            // expected
        }
        assertClusterSize(1, hz1);
    }

    @Test
    public void connectionsToTerminatedMember_shouldBeClosed() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, hz1, hz2, hz3);

        Address target = getAddress(hz2);
        hz2.getLifecycleService().terminate();

        assertClusterSizeEventually(2, hz1, hz3);

        assertNull(getConnectionManager(hz1).getConnection(target));
        assertNull(getConnectionManager(hz3).getConnection(target));
    }

    @Test
    public void connectionsToRemovedMember_shouldBeClosed() {
        Config config = new Config()
            .setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10")
            .setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz1, hz2, hz3);

        Address target = getAddress(hz2);

        FirewallingConnectionManager cm2 = getFireWallingConnectionManager(hz2);
        cm2.blockNewConnection(getAddress(hz1));
        cm2.blockNewConnection(getAddress(hz3));

        FirewallingConnectionManager cm3 = getFireWallingConnectionManager(hz3);
        cm3.blockNewConnection(getAddress((hz2)));

        dropOperationsBetween(hz2, hz1, F_ID, singletonList(HEARTBEAT));
        assertClusterSizeEventually(2, hz1, hz3);

        assertNull(getConnectionManager(hz1).getConnection(target));
        assertNull(getConnectionManager(hz3).getConnection(target));
    }

    private static FirewallingConnectionManager getFireWallingConnectionManager(HazelcastInstance hz) {
        return (FirewallingConnectionManager) getConnectionManager(hz);
    }

    private Config getConfigWithService(Object service, String serviceName) {
        final Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig().setEnabled(true)
                .setName(serviceName).setImplementation(service);
        config.getServicesConfig().addServiceConfig(serviceConfig);
        return config;
    }

    static void assertMemberViewsAreSame(MemberMap expectedMemberMap, MemberMap actualMemberMap) {
        assertEquals(expectedMemberMap.getVersion(), actualMemberMap.getVersion());
        assertEquals(expectedMemberMap.size(), actualMemberMap.size());

        // order is important
        List<MemberImpl> expectedMembers = new ArrayList<MemberImpl>(expectedMemberMap.getMembers());
        List<MemberImpl> actualMembers = new ArrayList<MemberImpl>(actualMemberMap.getMembers());

        assertEquals(expectedMembers, actualMembers);
    }

    static MemberMap getMemberMap(HazelcastInstance instance) {
        ClusterServiceImpl clusterService = getNode(instance).getClusterService();
        return clusterService.getMembershipManager().getMemberMap();
    }

    public static class StaticMemberNodeContext implements NodeContext {
        final NodeContext delegate;
        final MemberImpl member;

        public StaticMemberNodeContext(TestHazelcastInstanceFactory factory, MemberImpl member) {
            this.member = member;
            delegate = factory.getRegistry().createNodeContext(member.getAddress());
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new DefaultNodeExtension(node) {
                @Override
                public String createMemberUuid(Address address) {
                    return member.getUuid();
                }
            };
        }

        @Override
        public AddressPicker createAddressPicker(Node node) {
            return delegate.createAddressPicker(node);
        }

        @Override
        public Joiner createJoiner(Node node) {
            return delegate.createJoiner(node);
        }

        @Override
        public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
            return delegate.createConnectionManager(node, serverSocketChannel);
        }
    }

    private static class PostJoinAwareServiceImpl implements PostJoinAwareService {
        static final String SERVICE_NAME = "post-join-service";

        final CountDownLatch latch;

        private PostJoinAwareServiceImpl(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Operation getPostJoinOperation() {
            return new TimeConsumingPostJoinOperation();
        }
    }

    private static class TimeConsumingPostJoinOperation extends Operation {
        @Override
        public void run() throws Exception {
            PostJoinAwareServiceImpl service = getService();
            service.latch.await();
        }

        @Override
        public String getServiceName() {
            return PostJoinAwareServiceImpl.SERVICE_NAME;
        }
    }

    private static class PreJoinAwareServiceImpl implements PreJoinAwareService {
        static final String SERVICE_NAME = "pre-join-service";

        final CountDownLatch latch;
        final AtomicBoolean preJoinOpExecutionCompleted = new AtomicBoolean();
        final AtomicBoolean otherOpExecutedBeforePreJoin = new AtomicBoolean();

        private PreJoinAwareServiceImpl(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Operation getPreJoinOperation() {
            return new TimeConsumingPreJoinOperation();
        }
    }

    private static class TimeConsumingPreJoinOperation extends Operation {
        @Override
        public void run() throws Exception {
            PreJoinAwareServiceImpl service = getService();
            service.latch.await();
            service.preJoinOpExecutionCompleted.set(true);
        }

        @Override
        public String getServiceName() {
            return PreJoinAwareServiceImpl.SERVICE_NAME;
        }
    }

    private static class ExecutionTrackerOp extends Operation {

        @Override
        public void run()
                throws Exception {
            PreJoinAwareServiceImpl preJoinAwareService = getService();
            if (!preJoinAwareService.preJoinOpExecutionCompleted.get()) {
                preJoinAwareService.otherOpExecutedBeforePreJoin.set(true);
            }
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        public String getServiceName() {
            return PreJoinAwareServiceImpl.SERVICE_NAME;
        }
    }

    private static class FailingPreJoinOpService implements PreJoinAwareService {
        static final String SERVICE_NAME = "failing-pre-join-service";

        @Override
        public Operation getPreJoinOperation() {
            return new FailsDeserializationOperation();
        }
    }

    private static class FailingPostJoinOpService implements PostJoinAwareService {
        static final String SERVICE_NAME = "failing-post-join-service";

        @Override
        public Operation getPostJoinOperation() {
            return new FailsDeserializationOperation();
        }
    }

    public static class FailsDeserializationOperation extends Operation {

        @Override
        public void run() throws Exception {

        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            throw new RuntimeException("This operation always fails during deserialization");
        }
    }
}
