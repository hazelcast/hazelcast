package com.hazelcast.client.cluster;

import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientClusterServiceMemberListTest {

    private Config liteConfig = new Config().setLiteMember(true);

    private TestHazelcastFactory factory;

    private HazelcastInstance liteInstance;

    private HazelcastInstance dataInstance;

    private HazelcastInstance dataInstance2;

    private HazelcastInstance client;


    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        liteInstance = factory.newHazelcastInstance(liteConfig);
        dataInstance = factory.newHazelcastInstance();
        dataInstance2 = factory.newHazelcastInstance();
        client = factory.newHazelcastClient();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testLiteMembers() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final ClientClusterService clusterService = getClientClusterService(client);
                final Collection<Member> members = clusterService.getMembers(LITE_MEMBER_SELECTOR);
                verifyMembers(members, singletonList(liteInstance));

                assertEquals(1, clusterService.getSize(LITE_MEMBER_SELECTOR));
            }
        });
    }

    @Test
    public void testDataMembers() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final ClientClusterService clusterService = getClientClusterService(client);
                final Collection<Member> members = clusterService.getMembers(DATA_MEMBER_SELECTOR);
                verifyMembers(members, asList(dataInstance, dataInstance2));

                assertEquals(2, clusterService.getSize(DATA_MEMBER_SELECTOR));
            }
        });
    }

    @Test
    public void testMemberListOrderConsistentWithServer() {
        Set<Member> membersFromClient = client.getCluster().getMembers();
        Set<Member> membersFromServer = dataInstance.getCluster().getMembers();
        assertArrayEquals(membersFromClient.toArray(), membersFromServer.toArray());
    }

    private void verifyMembers(Collection<Member> membersToCheck, Collection<HazelcastInstance> membersToExpect) {
        for (HazelcastInstance instance : membersToExpect) {
            assertTrue(membersToCheck.contains(getLocalMember(instance)));
        }

        assertEquals(membersToExpect.size(), membersToCheck.size());
    }

    private Member getLocalMember(HazelcastInstance instance) {
        return getNode(instance).getLocalMember();
    }

    private ClientClusterService getClientClusterService(HazelcastInstance client) {
        return ClientTestUtil.getHazelcastClientInstanceImpl(client).getClientClusterService();
    }
}
