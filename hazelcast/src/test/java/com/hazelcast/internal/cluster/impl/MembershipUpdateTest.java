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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.StaticMemberNodeContext;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.FINALIZE_JOIN;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.ClusterJoinManager.STALE_JOIN_PREVENTION_DURATION_PROP;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.spi.properties.ClusterProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getConnectionManager;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static com.hazelcast.test.PacketFiltersUtil.delayOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static com.hazelcast.test.TestHazelcastInstanceFactory.initOrCreateConfig;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class MembershipUpdateTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule ruleStaleJoinPreventionDuration = clear(STALE_JOIN_PREVENTION_DURATION_PROP);

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
        AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<>(4);
        for (int i = 0; i < instances.length(); i++) {
            int ix = i;
            spawn(() -> instances.set(ix, factory.newHazelcastInstance()));
        }

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length(); i++) {
                HazelcastInstance instance = instances.get(i);
                assertNotNull(instance);
                assertClusterSize(instances.length(), instance);
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
    public void parallel_member_join_whenPostJoinOperationPresent() {
        CountDownLatch latch = new CountDownLatch(1);

        Config config = getConfigWithService(new PostJoinAwareServiceImpl(latch), PostJoinAwareServiceImpl.SERVICE_NAME);

        AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(6);
        for (int i = 0; i < instances.length(); i++) {
            int ix = i;
            spawn(() -> instances.set(ix, factory.newHazelcastInstance(config)));
        }

        // just a random latency
        sleepSeconds(3);
        latch.countDown();

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length(); i++) {
                HazelcastInstance instance = instances.get(i);
                assertNotNull(instance);
                assertClusterSize(instances.length(), instance);
            }
        });
    }

    @Test
    public void parallel_member_join_whenPreJoinOperationPresent() {
        CountDownLatch latch = new CountDownLatch(1);
        PreJoinAwareServiceImpl service = new PreJoinAwareServiceImpl(latch);
        Config config = getConfigWithService(service, PreJoinAwareServiceImpl.SERVICE_NAME);

        AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<>(6);
        for (int i = 0; i < instances.length(); i++) {
            int ix = i;
            spawn(() -> instances.set(ix, factory.newHazelcastInstance(config)));
        }

        sleepSeconds(3);
        latch.countDown();

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length(); i++) {
                HazelcastInstance instance = instances.get(i);
                assertNotNull(instance);
                assertClusterSize(instances.length(), instance);
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
        AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<>(4);
        for (int i = 0; i < instances.length(); i++) {
            int ix = i;
            spawn(() -> instances.set(ix, factory.newHazelcastInstance()));
        }

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length(); i++) {
                HazelcastInstance instance = instances.get(i);
                assertNotNull(instance);
                assertClusterSize(instances.length(), instance);
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
        AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<>(3);
        for (int i = 0; i < instances.length(); i++) {
            int ix = i;
            spawn(() -> instances.set(ix, factory.newHazelcastInstance()));
        }

        assertTrueEventually(() -> {
            for (int i = 0; i < instances.length(); i++) {
                HazelcastInstance instance = instances.get(i);
                assertNotNull(instance);
                assertClusterSize(instances.length(), instance);
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

        assertTrueEventually(() -> assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz3)));
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

        MemberInfo newMemberInfo = new MemberInfo(new Address("127.0.0.1", 6000), newUnsecureUUID(),
                Collections.emptyMap(), false, node.getVersion());
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
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        Config configMaster = new Config();
        configMaster.setProperty(ClusterProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance hz1 = factory.newHazelcastInstance(configMaster);

        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();
        HazelcastInstance hz4 = factory.newHazelcastInstance();

        assertClusterSizeEventually(4, hz2, hz3);

        rejectOperationsBetween(hz1, hz2, F_ID, singletonList(MEMBER_INFO_UPDATE));

        MemberImpl member3 = getNode(hz3).getLocalMember();
        hz3.getLifecycleService().terminate();

        assertClusterSizeEventually(3, hz1, hz4);
        assertClusterSize(4, hz2);

        hz3 = newHazelcastInstance(initOrCreateConfig(new Config()), randomName(), new StaticMemberNodeContext(factory, member3));

        assertClusterSizeEventually(4, hz1, hz4);

        resetPacketFiltersFrom(hz1);

        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz3));
        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz4));

        assertTrueEventually(() -> assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz2)));
    }

    @Test
    public void shouldNotProcessStaleJoinRequest() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        assertClusterSizeEventually(2, hz1, hz2);

        JoinRequest staleJoinReq = getNode(hz2).createJoinRequest(getNode(hz1).address);
        hz2.shutdown();
        assertClusterSizeEventually(1, hz1);

        assertTrueAllTheTime(() -> {
            ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(hz1);
            clusterService.getClusterJoinManager().handleJoinRequest(staleJoinReq, null);

            assertClusterSize(1, hz1);
        }, 3);
    }

    @Test
    public void shouldNotProcessStaleJoinRequest_whenMasterChanges() {
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();
        assertClusterSizeEventually(3, hz1, hz2, hz3);

        JoinRequest staleJoinReq = getNode(hz3).createJoinRequest(getNode(hz2).address);
        hz3.shutdown();
        hz1.shutdown();
        assertClusterSizeEventually(1, hz2);

        assertTrueAllTheTime(() -> {
            ClusterServiceImpl clusterService = (ClusterServiceImpl) getClusterService(hz2);
            clusterService.getClusterJoinManager().handleJoinRequest(staleJoinReq, null);

            assertClusterSize(1, hz2);
        }, 3);
    }

    @Test
    public void memberJoinsEventually_whenMemberRestartedWithSameUuid_butMasterDoesNotNoticeItsLeave() throws Exception {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        Config configMaster = new Config();
        HazelcastInstance hz1 = factory.newHazelcastInstance(configMaster);
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();
        assertClusterSizeEventually(3, hz2, hz3);

        MemberImpl member3 = getNode(hz3).getLocalMember();

        // A new member is restarted with member3's UUID.
        // Then after some time, member3 is terminated.
        // This is to emulate the case, member3 is restarted with preserving its UUID (using hot-restart),
        // but master does not realize its leave in time.
        // When master realizes, member3 is terminated,
        // new member should eventually join the cluster.
        Future<HazelcastInstance> future = spawn(() -> {
            NodeContext nodeContext = new StaticMemberNodeContext(factory, member3.getUuid(), factory.nextAddress());
            return newHazelcastInstance(initOrCreateConfig(new Config()), randomName(), nodeContext);
        });

        spawn(() -> {
            sleepSeconds(5);
            hz3.getLifecycleService().terminate();
        });

        HazelcastInstance hz4 = future.get();
        assertClusterSize(3, hz1, hz4);
        assertClusterSizeEventually(3, hz2);
    }

    // On a joining member assert that no operations are executed before pre join operations execution is completed.
    @Test
    public void noOperationExecuted_beforePreJoinOpIsDone() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        PreJoinAwareServiceImpl service = new PreJoinAwareServiceImpl(latch);
        Config config = getConfigWithService(service, PreJoinAwareServiceImpl.SERVICE_NAME);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        Address instance2Address = factory.nextAddress();
        OperationService operationService = getNode(instance1).getNodeEngine().getOperationService();
        // send operations from master to joining member. The master has already added the joining member to its member list
        // while the FinalizeJoinOp is being executed on joining member, so it might send operations to the joining member.
        Future sendOpsFromMaster = spawn(() -> {
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
        });

        Future<HazelcastInstance> future = spawn(() -> factory.newHazelcastInstance(instance2Address, config));

        sleepSeconds(10);
        // on latch countdown, the pre-join op completes
        latch.countDown();
        sleepSeconds(5);
        sendOpsFromMaster.cancel(true);

        HazelcastInstance instance2 = future.get();
        assertClusterSize(2, instance2);
        assertFalse(service.otherOpExecutedBeforePreJoin.get());
        assertTrue(service.preJoinOpExecutionCompleted.get());
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

        assertNull(getConnectionManager(hz1).get(target));

        ServerConnectionManager cm3 = getConnectionManager(hz3);
        assertTrueEventually(() -> assertNull(cm3.get(target)));
    }

    @Test
    public void connectionsToRemovedMember_shouldBeClosed() {
        Config config = new Config()
                .setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10")
                .setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz1, hz2, hz3);

        Address target = getAddress(hz3);

        ConnectionRemovedListener connListener1 = new ConnectionRemovedListener(target);
        getConnectionManager(hz1).addConnectionListener(connListener1);

        ConnectionRemovedListener connListener2 = new ConnectionRemovedListener(target);
        getConnectionManager(hz2).addConnectionListener(connListener2);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(HEARTBEAT));
        // Artificially suspect from hz3
        suspectMember(hz1, hz3);
        assertClusterSizeEventually(2, hz1, hz2);

        connListener1.assertConnectionRemoved();
        connListener2.assertConnectionRemoved();
    }

    private Config getConfigWithService(Object service, String serviceName) {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig().setEnabled(true)
                .setName(serviceName).setImplementation(service);
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);
        return config;
    }

    static void assertMemberViewsAreSame(MemberMap expectedMemberMap, MemberMap actualMemberMap) {
        assertEquals(expectedMemberMap.getVersion(), actualMemberMap.getVersion());
        assertEquals(expectedMemberMap.size(), actualMemberMap.size());

        // order is important
        List<MemberImpl> expectedMembers = new ArrayList<>(expectedMemberMap.getMembers());
        List<MemberImpl> actualMembers = new ArrayList<>(actualMemberMap.getMembers());

        assertEquals(expectedMembers, actualMembers);
    }

    static MemberMap getMemberMap(HazelcastInstance instance) {
        ClusterServiceImpl clusterService = getNode(instance).getClusterService();
        return clusterService.getMembershipManager().getMemberMap();
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

    private static class ConnectionRemovedListener implements ConnectionListener {
        private final Address endpoint;
        private final CountDownLatch latch = new CountDownLatch(1);

        ConnectionRemovedListener(Address endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void connectionAdded(Connection connection) {
        }

        @Override
        public void connectionRemoved(Connection connection) {
            if (endpoint.equals(connection.getRemoteAddress())) {
                latch.countDown();
            }
        }

        void assertConnectionRemoved() {
            assertOpenEventually(latch);
        }
    }
}
