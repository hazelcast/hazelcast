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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.FINALIZE_JOIN;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.delayOperationsFrom;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.resetPacketFiltersFrom;
import static com.hazelcast.spi.properties.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipUpdateTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    // TODO: add membership update tests
    // ✔ sequential member join
    // ✔ concurrent member join
    // ✔ sequential member join and removal
    // ✔ concurrent member join and removal
    // ✔ existing members missing member updates (join), convergence
    // ✔ existing member missing member removal, then receives periodic member publish
    // ✔ existing member missing member removal, then receives new member join update
    // ✔ existing member receiving out-of-order member updates
    // ✔ new member receiving out-of-order finalize join & member updates
    // ✔ existing member receiving a member list that's not containing itself
    // - new member receiving a finalize join that's not containing itself
    // ✔ existing member receives member update from non-master
    // - new member receives finalize join from non-master
    // - byzantine member updates published
    // - so on...

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

        instances.get(instances.length() - 1).shutdown();
        for (int i = 0; i < instances.length() - 1; i++) {
            HazelcastInstance instance = instances.get(i);
            assertClusterSizeEventually(instances.length() - 1, instance);
        }

        MemberMap referenceMemberMap = getMemberMap(instances.get(0));
        // version = number of started members + 1 removal
        assertEquals(instances.length() + 1, referenceMemberMap.getVersion());

        for (int i = 0; i < instances.length() - 1; i++) {
            HazelcastInstance instance = instances.get(i);
            MemberMap memberMap = getMemberMap(instance);
            assertMemberViewsAreSame(referenceMemberMap, memberMap);
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

        instances.get(instances.length() - 1).shutdown();
        instances.set(instances.length() - 1, factory.newHazelcastInstance());

        for (int i = 0; i < instances.length(); i++) {
            HazelcastInstance instance = instances.get(i);
            assertClusterSizeEventually(instances.length(), instance);
        }

        MemberMap referenceMemberMap = getMemberMap(instances.get(0));
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

        assertClusterSizeEventually(2, hz1);

        dropOperationsFrom(hz1, MEMBER_INFO_UPDATE);

        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz1);
        assertClusterSizeEventually(3, hz3);
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

        assertClusterSizeEventually(2, hz1);

        dropOperationsFrom(hz1, MEMBER_INFO_UPDATE);

        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, hz1);
        assertClusterSizeEventually(3, hz3);
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
        delayOperationsFrom(hz1, MEMBER_INFO_UPDATE);

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
        delayOperationsFrom(hz1, MEMBER_INFO_UPDATE, FINALIZE_JOIN);

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

        assertClusterSizeEventually(3, hz2);

        dropOperationsBetween(hz1, hz3, MEMBER_INFO_UPDATE);
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

        assertClusterSizeEventually(3, hz2);

        dropOperationsBetween(hz1, hz3, MEMBER_INFO_UPDATE);
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
    @RequireAssertEnabled
    public void memberReceives_memberUpdateNotContainingItself() throws Exception {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        Node node = getNode(hz1);
        ClusterServiceImpl clusterService = node.getClusterService();
        MembershipManager membershipManager = clusterService.getMembershipManager();
        
        MembersView membersView = MembersView.createNew(membershipManager.getMemberListVersion() + 1, 
                Arrays.asList(membershipManager.getMember(getAddress(hz1)), membershipManager.getMember(getAddress(hz2))));

        Operation memberUpdate = new MembersUpdateOp(membershipManager.getMember(getAddress(hz3)).getUuid(),
                membersView, clusterService.getClusterTime(), null, true);

        Future<Object> future =
                node.getNodeEngine().getOperationService().invokeOnTarget(null, memberUpdate, getAddress(hz3));

        try {
            future.get();
            fail("Membership update should fail!");
        } catch (AssertionError error) {
            // AssertionError expected (requires assertions enabled)
        }
    }

    @Test
    public void memberReceives_memberUpdateFromInvalidMaster() throws Exception {
        Config config = new Config();
        config.setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        HazelcastInstance hz3 = factory.newHazelcastInstance(config);

        Node node = getNode(hz1);
        ClusterServiceImpl clusterService = node.getClusterService();
        MembershipManager membershipManager = clusterService.getMembershipManager();

        MemberInfo newMemberInfo = new MemberInfo(new Address("127.0.0.1", 6000), newUnsecureUuidString(),
                Collections.<String, Object>emptyMap(), node.getVersion());
        MembersView membersView =
                MembersView.cloneAdding(membershipManager.createMembersView(), singleton(newMemberInfo));

        Operation memberUpdate = new MembersUpdateOp(membershipManager.getMember(getAddress(hz3)).getUuid(),
                membersView, clusterService.getClusterTime(), null, true);

        NodeEngineImpl nonMasterNodeEngine = getNodeEngineImpl(hz2);
        Future<Object> future =
                nonMasterNodeEngine.getOperationService().invokeOnTarget(null, memberUpdate, getAddress(hz3));
        future.get();

        // member update should not be applied
        assertClusterSize(3, hz1);
        assertClusterSize(3, hz2);
        assertClusterSize(3, hz3);

        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz2));
        assertMemberViewsAreSame(getMemberMap(hz1), getMemberMap(hz3));
    }

    static void assertMemberViewsAreSame(MemberMap expectedMemberMap, MemberMap actualMemberMap) {
        assertEquals(expectedMemberMap.getVersion(), actualMemberMap.getVersion());
        assertEquals(expectedMemberMap.size(), actualMemberMap.size());

        Set<MemberImpl> expectedMembers = expectedMemberMap.getMembers();
        Set<MemberImpl> actualMembers = actualMemberMap.getMembers();
        assertEquals(expectedMembers, actualMembers);
    }

    static MemberMap getMemberMap(HazelcastInstance instance) {
        ClusterServiceImpl clusterService = getNode(instance).getClusterService();
        return clusterService.getMembershipManager().getMemberMap();
    }

}
