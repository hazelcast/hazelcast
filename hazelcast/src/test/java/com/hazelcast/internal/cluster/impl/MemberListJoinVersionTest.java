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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.instance.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.SPLIT_BRAIN_MERGE_VALIDATION;
import static com.hazelcast.internal.cluster.impl.MemberMap.SINGLETON_MEMBER_LIST_VERSION;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static com.hazelcast.spi.properties.GroupProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class, SerializationSamplesExcluded.class})
public class MemberListJoinVersionTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void when_singletonClusterStarted_then_memberListJoinVersionShouldBeAssigned() {
        HazelcastInstance instance = factory.newHazelcastInstance();

        MemberImpl localMember = getNode(instance).getLocalMember();

        assertEquals(SINGLETON_MEMBER_LIST_VERSION, localMember.getMemberListJoinVersion());
        assertEquals(SINGLETON_MEMBER_LIST_VERSION, getClusterService(instance).getMemberListJoinVersion());
        MemberImpl member = getClusterService(instance).getMember(localMember.getAddress());
        assertEquals(SINGLETON_MEMBER_LIST_VERSION, member.getMemberListJoinVersion());
    }

    @Test
    public void when_multipleMembersStarted_then_memberListJoinVersionShouldBeAssigned() {
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance slave1 = factory.newHazelcastInstance();
        HazelcastInstance slave2 = factory.newHazelcastInstance();
        HazelcastInstance slave3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(4, slave1, slave2);

        assertJoinMemberListVersions(master, slave1, slave2, slave3);
    }

    @Test
    public void when_splitSubClustersMerge_then_targetClusterShouldIncrementMemberListVersion() {
        Config config = new Config();
        config.setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5")
                .setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
                .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");

        final HazelcastInstance member1 = factory.newHazelcastInstance(config);
        final HazelcastInstance member2 = factory.newHazelcastInstance(config);
        final HazelcastInstance member3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, member2);

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        member3.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == MERGED) {
                    mergeLatch.countDown();
                }
            }
        });

        rejectOperationsFrom(member3, F_ID, asList(HEARTBEAT, SPLIT_BRAIN_MERGE_VALIDATION));

        assertClusterSizeEventually(2, member1, member2);

        rejectOperationsFrom(member3, F_ID, singletonList(SPLIT_BRAIN_MERGE_VALIDATION));

        assertClusterSizeEventually(1, member3);

        int beforeJoinVersionOnMember3 = getClusterService(member3).getMemberListVersion();

        resetPacketFiltersFrom(member3);

        assertOpenEventually(mergeLatch);

        int afterJoinVersionOnMember1 = getClusterService(member1).getMemberListVersion();
        assertNotEquals(afterJoinVersionOnMember1, beforeJoinVersionOnMember3);

        int versionOnLocalMember3 = getNode(member3).getLocalMember().getMemberListJoinVersion();
        assertEquals(afterJoinVersionOnMember1, versionOnLocalMember3);

        assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member3));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member2));
            }
        });
        assertJoinMemberListVersions(member1, member2, member3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_310MemberJoinsWith39Mode_memberListJoinVersionCannotBeQueried() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, V3_9.toString());

        HazelcastInstance member1 = factory.newHazelcastInstance();

        getClusterService(member1).getMemberListJoinVersion();
    }

    @Test
    public void when_310MemberJoinsWith39Mode_itDoesNotPublishJoinVersions() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, V3_9.toString());

        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();
        HazelcastInstance member3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, member2);

        assertNotEquals(NA_MEMBER_LIST_JOIN_VERSION, getNode(member1).getLocalMember().getMemberListJoinVersion());
        assertEquals(NA_MEMBER_LIST_JOIN_VERSION, getNode(member2).getLocalMember().getMemberListJoinVersion());
        assertEquals(NA_MEMBER_LIST_JOIN_VERSION, getNode(member3).getLocalMember().getMemberListJoinVersion());

        for (MemberImpl member : getClusterService(member1).getMemberImpls()) {
            assertNotEquals(NA_MEMBER_LIST_JOIN_VERSION, member.getMemberListJoinVersion());
        }

        for (HazelcastInstance instance : asList(member2, member3)) {
            for (MemberImpl member : getClusterService(instance).getMemberImpls()) {
                assertEquals(NA_MEMBER_LIST_JOIN_VERSION, member.getMemberListJoinVersion());
            }
        }
    }

    @Test
    public void when_310MemberClaimsMastershipWith39Mode_itGeneratesJoinVersions() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, V3_9.toString());

        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();
        HazelcastInstance member3 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, member2);

        member1.getLifecycleService().terminate();

        assertClusterSizeEventually(2, member2, member3);

        // new master has created its local member list join version but it is not exposed
        assertNotEquals(NA_MEMBER_LIST_JOIN_VERSION, getNode(member2).getLocalMember().getMemberListJoinVersion());

        // others has not learnt their member list join versions
        assertEquals(NA_MEMBER_LIST_JOIN_VERSION, getNode(member3).getLocalMember().getMemberListJoinVersion());

        for (MemberImpl member : getClusterService(member2).getMemberImpls()) {
            assertNotEquals(NA_MEMBER_LIST_JOIN_VERSION, member.getMemberListJoinVersion());
        }

        for (MemberImpl member : getClusterService(member3).getMemberImpls()) {
            assertEquals(NA_MEMBER_LIST_JOIN_VERSION, member.getMemberListJoinVersion());
        }
    }

    public static void assertJoinMemberListVersions(HazelcastInstance... instances) {
        for (HazelcastInstance instance1 : instances) {
            assertNotEquals(NA_MEMBER_LIST_JOIN_VERSION, getClusterService(instance1).getMemberListJoinVersion());
            for (MemberImpl instance1Member : getClusterService(instance1).getMemberImpls()) {
                assertNotEquals(NA_MEMBER_LIST_JOIN_VERSION, instance1Member.getMemberListJoinVersion());
                for (HazelcastInstance instance2 : instances) {
                    MemberImpl instance2Member = getClusterService(instance2).getMember(instance1Member.getUuid());
                    assertEquals(instance1Member.getMemberListJoinVersion(), instance2Member.getMemberListJoinVersion());
                }
            }
        }
    }
}
