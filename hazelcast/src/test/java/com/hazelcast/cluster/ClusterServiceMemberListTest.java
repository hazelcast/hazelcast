package com.hazelcast.cluster;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterServiceMemberListTest
        extends HazelcastTestSupport {

    private Config liteConfig = new Config().setLiteMember(true);

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance liteInstance;

    private HazelcastInstance dataInstance;

    private HazelcastInstance dataInstance2;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(3);
        liteInstance = factory.newHazelcastInstance(liteConfig);
        dataInstance = factory.newHazelcastInstance();
        dataInstance2 = factory.newHazelcastInstance();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testGetMembersWithMemberSelector() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                verifyMembersFromLiteMember(liteInstance);
                verifyMembersFromDataMember(dataInstance);
                verifyMembersFromDataMember(dataInstance2);
            }
        });
    }

    @Test
    public void testSizeWithMemberSelector() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                verifySizeFromLiteMember(liteInstance);
                verifySizeFromDataMember(dataInstance);
                verifySizeFromDataMember(dataInstance2);
            }
        });
    }

    private void verifyMembersFromLiteMember(final HazelcastInstance instance) {
        final Member localMember = getLocalMember(instance);
        final ClusterService clusterService = getClusterService(instance);
        final Collection<Member> liteMembers = clusterService.getMembers(LITE_MEMBER_SELECTOR);
        final Collection<Member> dataMembers = clusterService.getMembers(DATA_MEMBER_SELECTOR);

        assertTrue(liteMembers.contains(localMember));
        assertFalse(dataMembers.contains(localMember));
        final Collection<Member> liteMembersWithoutThis = clusterService
                .getMembers(MemberSelectors.and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(liteMembersWithoutThis.contains(localMember));
        final Collection<Member> dataMembersWithThis = clusterService
                .getMembers(MemberSelectors.or(DATA_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR));
        assertTrue(dataMembersWithThis.contains(localMember));
    }

    private void verifyMembersFromDataMember(final HazelcastInstance instance) {
        final Member localMember = getLocalMember(instance);
        final ClusterService clusterService = getClusterService(instance);
        final Collection<Member> liteMembers = clusterService.getMembers(LITE_MEMBER_SELECTOR);
        final Collection<Member> dataMembers = clusterService.getMembers(DATA_MEMBER_SELECTOR);

        assertTrue(dataMembers.contains(localMember));
        assertFalse(liteMembers.contains(localMember));
        final Collection<Member> dataMembersWithoutThis = clusterService
                .getMembers(MemberSelectors.and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(dataMembersWithoutThis.contains(localMember));
        final Collection<Member> liteMembersWithThis = clusterService
                .getMembers(MemberSelectors.or(LITE_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR));
        assertTrue(liteMembersWithThis.contains(localMember));
    }

    private void verifySizeFromLiteMember(final HazelcastInstance instance) {
        final ClusterService clusterService = getClusterService(instance);

        assertEquals(1, clusterService.getSize(MemberSelectors.LITE_MEMBER_SELECTOR));
        assertEquals(2, clusterService.getSize(MemberSelectors.DATA_MEMBER_SELECTOR));
        assertEquals(0, clusterService.getSize(MemberSelectors.and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)));
        assertEquals(3, clusterService.getSize(MemberSelectors.or(DATA_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR)));
    }

    private void verifySizeFromDataMember(final HazelcastInstance instance) {
        final ClusterService clusterService = getClusterService(instance);

        assertEquals(1, clusterService.getSize(MemberSelectors.LITE_MEMBER_SELECTOR));
        assertEquals(2, clusterService.getSize(MemberSelectors.DATA_MEMBER_SELECTOR));
        assertEquals(1, clusterService.getSize(MemberSelectors.and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)));
        assertEquals(2, clusterService.getSize(MemberSelectors.or(LITE_MEMBER_SELECTOR, LOCAL_MEMBER_SELECTOR)));
    }

    private Member getLocalMember(HazelcastInstance instance) {
        return getNode(instance).getLocalMember();
    }

}
