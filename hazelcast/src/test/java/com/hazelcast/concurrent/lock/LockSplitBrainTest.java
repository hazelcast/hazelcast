package com.hazelcast.concurrent.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class LockSplitBrainTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testLockSplitBrain_acquireSameLock() throws InterruptedException {
        Config config = newConfig();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        final String key = generateKeyOwnedBy(h3);
        ILock lock = h3.getLock(key);
        lock.lock();

        TestMemberShipListener memberShipListener = new TestMemberShipListener(2);
        h3.getCluster().addMembershipListener(memberShipListener);
        TestLifeCycleListener lifeCycleListener = new TestLifeCycleListener(1);
        h3.getLifecycleService().addLifecycleListener(lifeCycleListener);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(h3.getPartitionService().isLocalMemberSafe());
            }
        });

        closeConnectionBetween(h1,h3);
        closeConnectionBetween(h2,h3);

        assertOpenEventually(memberShipListener.latch);
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(1, h3);

        ILock h1Lock = h1.getLock(key);
        h1Lock.lock();

        lock.forceUnlock();

        assertOpenEventually(lifeCycleListener.latch);
        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        ILock testLock = h3.getLock(key);
        assertTrue(testLock.isLocked());
    }


    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS, "3");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS, "1");
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