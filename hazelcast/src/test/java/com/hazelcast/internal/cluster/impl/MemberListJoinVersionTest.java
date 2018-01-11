package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
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

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.instance.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.SPLIT_BRAIN_MERGE_VALIDATION;
import static com.hazelcast.internal.cluster.impl.MemberMap.SINGLETON_MEMBER_LIST_VERSION;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static com.hazelcast.spi.properties.GroupProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MASTERSHIP_CLAIM_MEMBER_LIST_VERSION_INCREMENT;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
    public void when_masterShutsDown_then_memberListVersionShouldIncrementSufficiently() {
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance slave1 = factory.newHazelcastInstance();

        ClusterService slave1ClusterService = getClusterService(slave1);
        int memberListVersion = slave1ClusterService.getMemberListVersion();

        master.shutdown();

        assertClusterSizeEventually(1, slave1);

        int newMemberListVersion = slave1ClusterService.getMemberListVersion();
        int versionIncPerMember = getMastershipClaimMemberListVersionIncrementConfig(slave1);

        assertTrue((newMemberListVersion - memberListVersion) >= versionIncPerMember);
    }

    @Test
    public void when_masterTerminates_then_memberListVersionShouldIncrementSufficiently() {
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance slave1 = factory.newHazelcastInstance();

        ClusterService slave1ClusterService = getClusterService(slave1);
        int memberListVersion = slave1ClusterService.getMemberListVersion();

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(1, slave1);

        int newMemberListVersion = slave1ClusterService.getMemberListVersion();
        int versionIncPerMember = getMastershipClaimMemberListVersionIncrementConfig(slave1);

        assertTrue((newMemberListVersion - memberListVersion) >= versionIncPerMember);
    }

    @Test
    public void when_masterAndNextMasterTerminates_then_memberListVersionShouldIncrementBasedOnMemberIndex() {
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance slave1 = factory.newHazelcastInstance();
        HazelcastInstance slave2 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, slave1);

        ClusterService slave2ClusterService = getClusterService(slave2);
        int memberListVersion = slave2ClusterService.getMemberListVersion();

        dropOperationsBetween(master, slave2, F_ID, singletonList(MEMBER_INFO_UPDATE));

        slave1.getLifecycleService().terminate();
        master.getLifecycleService().terminate();

        assertClusterSizeEventually(1, slave2);

        int newMemberListVersion = slave2ClusterService.getMemberListVersion();
        int versionIncPerMember = getMastershipClaimMemberListVersionIncrementConfig(slave2);

        assertTrue((newMemberListVersion - memberListVersion) >= (versionIncPerMember * 2));
    }

    @Test
    public void when_splitSubClustersMerge_then_targetClusterShouldIncrementMemberListVersion() {
        Config config = new Config();
        config.setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
              .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
              .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5")
              .setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5")
              .setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        HazelcastInstance member1 = factory.newHazelcastInstance(config);
        HazelcastInstance member2 = factory.newHazelcastInstance(config);
        HazelcastInstance member3 = factory.newHazelcastInstance(config);

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

        dropOperationsFrom(member3, F_ID, asList(HEARTBEAT, SPLIT_BRAIN_MERGE_VALIDATION));

        assertClusterSizeEventually(2, member1, member2);

        dropOperationsFrom(member3, F_ID, singletonList(SPLIT_BRAIN_MERGE_VALIDATION));

        assertClusterSizeEventually(1, member3);

        int memberListVersionBeforeMerge = getClusterService(member3).getMemberListVersion();

        resetPacketFiltersFrom(member3);

        assertOpenEventually(mergeLatch);
        assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member2));
        assertMemberViewsAreSame(getMemberMap(member1), getMemberMap(member3));

        int memberListVersionAfterMerge = getClusterService(member1).getMemberListVersion();
        assertTrue(memberListVersionAfterMerge != memberListVersionBeforeMerge);
        assertEquals(memberListVersionAfterMerge, getNode(member3).getLocalMember().getMemberListJoinVersion());

        assertJoinMemberListVersions(member1, member2, member3);
    }

    @Test
    public void when_memberListIncrementIsConfiguredTooLow_then_itShouldIncrementAtLeastAsMemberCount() {
        int memberCount = 3;
        Config config = new Config();
        config.setProperty(MASTERSHIP_CLAIM_MEMBER_LIST_VERSION_INCREMENT.toString(), "1");
        HazelcastInstance[] instances = factory.newInstances(config, memberCount);

        HazelcastInstance slave1 = instances[1];
        assertClusterSizeEventually(3, slave1);

        ClusterService slave1ClusterService = getClusterService(slave1);
        int memberListVersion = slave1ClusterService.getMemberListVersion();

        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, slave1);

        int newMemberListVersion = slave1ClusterService.getMemberListVersion();

        assertTrue((newMemberListVersion - memberListVersion) >= memberCount);
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

    private int getMastershipClaimMemberListVersionIncrementConfig(HazelcastInstance instance) {
        return getNode(instance).getProperties().getInteger(MASTERSHIP_CLAIM_MEMBER_LIST_VERSION_INCREMENT);
    }
}
