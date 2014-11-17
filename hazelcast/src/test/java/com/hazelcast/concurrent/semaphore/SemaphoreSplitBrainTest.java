package com.hazelcast.concurrent.semaphore;


import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SemaphoreSplitBrainTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testSemaphoreSplitBrain() throws InterruptedException {
        Config config = newConfig();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        final String key = generateKeyOwnedBy(h3);
        ISemaphore semaphore = h3.getSemaphore(key);
        semaphore.init(5);
        semaphore.acquire(3);
        assertEquals(2, semaphore.availablePermits());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(h3.getPartitionService().isLocalMemberSafe());
            }
        });

        TestMemberShipListener memberShipListener = new TestMemberShipListener(2);
        h3.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h3.getLifecycleService().addLifecycleListener(lifeCycleListener);

        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(1, h3);

        ISemaphore semaphore1 = h1.getSemaphore(key);
        //when member is down, permits are released.
        assertEquals(5, semaphore1.availablePermits());
        semaphore1.acquire(4);

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        ISemaphore testSemaphore = h3.getSemaphore(key);
        assertEquals(1, testSemaphore.availablePermits());
    }

    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null || h2 == null) return;
        final Node n1 = TestUtil.getNode(h1);
        final Node n2 = TestUtil.getNode(h2);
        n1.clusterService.removeAddress(n2.address);
        n2.clusterService.removeAddress(n1.address);
    }

    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "3");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "1");
        return config;
    }

    private class TestLifeCycleListener implements LifecycleListener {

        CountDownLatch latch;

        TestLifeCycleListener(int countdown) {
            latch = new CountDownLatch(countdown);
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
            latch = new CountDownLatch(countdown);
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
