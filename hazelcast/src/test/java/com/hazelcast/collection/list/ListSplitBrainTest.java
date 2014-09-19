package com.hazelcast.collection.list;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ListSplitBrainTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testListSplitBrain_Multicast() throws InterruptedException {
        testListSplitBrain(true);
    }

    @Test
    public void testListSplitBrain_TcpIp() throws InterruptedException {
        testListSplitBrain(false);
    }

    private void testListSplitBrain(boolean multicast) throws InterruptedException {
        String name = randomString();
        Config config = getConfig(multicast);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        IList<Object> list = h1.getList(name);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(2);
        h3.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h3.getLifecycleService().addLifecycleListener(lifeCycleListener);

        for (int i = 0; i < 100; i++) {
            list.add("item" + i);
        }

        assertSizeEventually(100, list);

        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertTrue(memberShipListener.splitLatch.await(10, TimeUnit.SECONDS));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(1, h3.getCluster().getMembers().size());

        for (int i = 100; i < 200; i++) {
            list.add("item" + i);
        }

        assertSizeEventually(200, list);

        IList<Object> list3 = h3.getList(name);
        for (int i = 0; i < 50; i++) {
            list3.add("lostListItem" + i);
        }

        assertTrue(lifeCycleListener.mergeLatch.await(60, TimeUnit.SECONDS));
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());

        IList<Object> testList = h1.getList(name);
        assertFalse(testList.contains("lostListItem0"));
        assertFalse(testList.contains("lostListItem49"));
        assertTrue(testList.contains("item0"));
        assertTrue(testList.contains("item199"));
        assertTrue(testList.contains("item121"));
        assertTrue(testList.contains("item45"));
    }

    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null || h2 == null) return;
        final Node n1 = TestUtil.getNode(h1);
        final Node n2 = TestUtil.getNode(h2);
        n1.clusterService.removeAddress(n2.address);
        n2.clusterService.removeAddress(n1.address);
    }

    private Config getConfig(boolean multicast) {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "30");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(multicast);
        join.getTcpIpConfig().setEnabled(!multicast);
        join.getTcpIpConfig().addMember("127.0.0.1");

        return config;
    }

    private class TestLifeCycleListener implements LifecycleListener {

        CountDownLatch mergeLatch;

        TestLifeCycleListener(int latch) {
            mergeLatch = new CountDownLatch(latch);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                mergeLatch.countDown();
            }
        }
    }

    private class TestMemberShipListener implements MembershipListener {

        final CountDownLatch splitLatch;

        TestMemberShipListener(int latch) {
            splitLatch = new CountDownLatch(latch);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {

        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            splitLatch.countDown();
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

        }
    }
}

