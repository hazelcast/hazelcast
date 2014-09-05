package com.hazelcast.queue;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueStatisticsTest extends AbstractQueueTest {

    @Test
    public void testItemCount() {
        IQueue queue = newQueue();
        int items = 20;
        for (int i = 0; i < items; i++) {
            queue.offer("item" + i);
        }
        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(20, stats.getOwnedItemCount());
        assertEquals(0, stats.getBackupItemCount());
    }

    @Test
    public void testOfferOperationCount() throws InterruptedException {
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
    public void testRejectedOfferOperationCount() {
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
    public void testPollOperationCount() throws InterruptedException {
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


        final LocalQueueStats stats = queue.getLocalQueueStats();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(30, stats.getPollOperationCount());
            }
        };
        assertTrueEventually(task);
    }

    @Test
    public void testEmptyPollOperationCount() {
        IQueue queue = newQueue();

        for (int i = 0; i < 10; i++) {
            queue.poll();
        }

        final LocalQueueStats stats = queue.getLocalQueueStats();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(10, stats.getEmptyPollOperationCount());
            }
        };
        assertTrueEventually(task);
    }

    @Test
    public void testOtherOperationCount() {
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
    public void testAge() {
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
    public void testEventOperationCount() {
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