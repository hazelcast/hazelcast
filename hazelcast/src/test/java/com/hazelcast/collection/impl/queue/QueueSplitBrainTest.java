package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class QueueSplitBrainTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testQueueSplitBrain() {
        Config config = newConfig();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        String name = generateKeyOwnedBy(h1);
        IQueue<Object> queue = h1.getQueue(name);

        TestMemberShipListener memberShipListener = new TestMemberShipListener(2);
        h3.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h3.getLifecycleService().addLifecycleListener(lifeCycleListener);

        for (int i = 0; i < 100; i++) {
            queue.add("item" + i);
        }

        waitAllForSafeState(h1, h2, h3);

        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(1, h3);

        for (int i = 100; i < 200; i++) {
            queue.add("item" + i);
        }

        IQueue<Object> queue3 = h3.getQueue(name);
        for (int i = 0; i < 50; i++) {
            queue3.add("lostQueueItem" + i);
        }

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        IQueue<Object> testQueue = h1.getQueue(name);
        assertNotContains(testQueue, "lostQueueItem0");
        assertNotContains(testQueue, "lostQueueItem49");
        assertContains(testQueue, "item0");
        assertContains(testQueue, "item199");
        assertContains(testQueue, "item121");
        assertContains(testQueue, "item45");
    }

    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "30");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        return config;
    }

    private class TestLifeCycleListener implements LifecycleListener {

        CountDownLatch latch;

        TestLifeCycleListener(int countdown) {
            this.latch = new CountDownLatch(countdown);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }
    }

    private class TestMemberShipListener implements MembershipListener {

        final CountDownLatch latch;

        TestMemberShipListener(int countdown) {
            this.latch = new CountDownLatch(countdown);
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            latch.countDown();
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }
    }
}
