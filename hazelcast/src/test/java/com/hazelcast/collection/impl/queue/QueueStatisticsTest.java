package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueStatisticsTest extends HazelcastTestSupport {

    protected IQueue newQueue() {
        HazelcastInstance instance = createHazelcastInstance();
        return instance.getQueue(randomString());
    }

    protected IQueue newQueue_WithMaxSizeConfig(int maxSize) {
        Config config = new Config();
        final String name = randomString();
        config.getQueueConfig(name).setMaxSize(maxSize);
        HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getQueue(name);
    }

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

        final LocalQueueStats stats = queue.getLocalQueueStats();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(30, stats.getOfferOperationCount());
            }
        };
        assertTrueEventually(task);
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

        final LocalQueueStats stats = queue.getLocalQueueStats();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(10, stats.getRejectedOfferOperationCount());
            }
        };
        assertTrueEventually(task);

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

        final LocalQueueStats stats = queue.getLocalQueueStats();
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, stats.getOtherOperationsCount());
            }
        };
        assertTrueEventually(task);
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
        final LocalQueueStats stats = queue.getLocalQueueStats();
        assertOpenEventually(listener.addedLatch);
        assertOpenEventually(listener.removedLatch);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(60, stats.getEventOperationCount());
            }
        });
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