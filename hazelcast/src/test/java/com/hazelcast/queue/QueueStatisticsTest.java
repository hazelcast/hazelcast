package com.hazelcast.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueStatisticsTest extends AbstractQueueTest {

    @Test
    @Ignore
    public void testQueueStats() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final String name = randomString();

        HazelcastInstance ins1 = factory.newHazelcastInstance();
        final int items = 20;
        IQueue q1 = ins1.getQueue(name);
        for (int i = 0; i < items / 2; i++) {
            q1.offer("item" + i);
        }

        HazelcastInstance ins2 = factory.newHazelcastInstance();
        IQueue q2 = ins2.getQueue(name);
        for (int i = 0; i < items / 2; i++) {
            q2.offer("item" + i);
        }

        LocalQueueStats stats1 = ins1.getQueue(name).getLocalQueueStats();
        LocalQueueStats stats2 = ins2.getQueue(name).getLocalQueueStats();

        assertTrue(stats1.getOwnedItemCount() == items || stats2.getOwnedItemCount() == items);
        assertFalse(stats1.getOwnedItemCount() == items && stats2.getOwnedItemCount() == items);

        if (stats1.getOwnedItemCount() == items) {
            assertEquals(items, stats2.getBackupItemCount());
            assertEquals(0, stats1.getBackupItemCount());
        } else {
            assertEquals(items, stats1.getBackupItemCount());
            assertEquals(0, stats2.getBackupItemCount());
        }
    }

    @Test
    public void testQueueStats_OfferOperationCount() throws InterruptedException {
        IQueue queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        for (int i = 0; i < 10; i++) {
            queue.add("item" + i);
        }
        for (int i = 0; i < 10; i++) {
            queue.put("item" + i);
        }

        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(30, stats.getOfferOperationCount());
    }

    @Test
    public void testQueueStats_RejectedOfferOperationCount() {
        IQueue queue = newQueue_WithMaxSizeConfig(30);
        for (int i = 0; i < 30; i++) {
            queue.offer("item" + i);
        }

        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(10, stats.getRejectedOfferOperationCount());
    }

    @Test
    public void testQueueStats_PollOperationCount() throws InterruptedException {
        IQueue queue = newQueue();
        for (int i = 0; i < 30; i++) {
            queue.offer("item" + i);
        }

        for (int i = 0; i < 10; i++) {
            queue.remove();
        }
        for (int i = 0; i < 10; i++) {
            queue.take();
        }
        for (int i = 0; i < 10; i++) {
            queue.poll();
        }

        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(30, stats.getPollOperationCount());
    }

    @Test
    public void testQueueStats_EmptyPollOperationCount() {
        IQueue queue = newQueue();

        for (int i = 0; i < 10; i++) {
            queue.poll();
        }

        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(10, stats.getEmptyPollOperationCount());
    }

    @Test
    public void testQueueStats_OtherOperationCount() {
        IQueue queue = newQueue();
        for (int i = 0; i < 30; i++) {
            queue.offer("item" + i);
        }
        ArrayList<String> list = new ArrayList<String>();
        queue.drainTo(list);
        queue.addAll(list);
        queue.removeAll(list);

        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(3, stats.getOtherOperationsCount());
    }

    @Test
    public void testQueueStats_Age() {
        IQueue queue = newQueue();
        queue.offer("maxAgeItem");
        queue.offer("minAgeItem");

        LocalQueueStats stats = queue.getLocalQueueStats();
        long maxAge = stats.getMaxAge();
        long minAge = stats.getMinAge();
        long testAge = (maxAge + minAge) / 2;
        long avgAge = stats.getAvgAge();
        assertEquals(testAge, avgAge);
    }

    @Test
    public void testQueueStats_EventOperationCount() {
        IQueue queue = newQueue();
        TestListener listener = new TestListener(30);
        queue.addItemListener(listener, true);
        for (int i = 0; i < 30; i++) {
            queue.offer("item" + i);
        }
        for (int i = 0; i < 30; i++) {
            queue.poll();
        }
        LocalQueueStats stats = queue.getLocalQueueStats();
        assertOpenEventually(listener.addedLatch);
        assertOpenEventually(listener.removedLatch);
        assertEquals(60, stats.getEventOperationCount());
    }

    private static class TestListener implements ItemListener {

        public final CountDownLatch addedLatch;
        public final CountDownLatch removedLatch;

        public TestListener(int latchCount) {
            addedLatch = new CountDownLatch(latchCount);
            removedLatch = new CountDownLatch(latchCount);
        }

        public void itemAdded(ItemEvent item) {
            addedLatch.countDown();
        }

        public void itemRemoved(ItemEvent item) {
            removedLatch.countDown();
        }
    }

}