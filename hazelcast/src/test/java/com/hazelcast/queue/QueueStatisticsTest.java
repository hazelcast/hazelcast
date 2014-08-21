package com.hazelcast.queue;

import com.hazelcast.core.IQueue;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueStatisticsTest extends AbstractQueueTest {

    //todo: can you clean this up in multiple smaller tests?
    // and there is a lot more statistics to be tested.

    @Test
    public void testQueueStats_ItemCount() {
        IQueue queue = newQueue();
        int items = 20;
        for (int i = 0; i < items; i++) {
            queue.offer("item" + i);
        }
        LocalQueueStats stats = queue.getLocalQueueStats();
        assertEquals(20, stats.getOwnedItemCount());
        assertEquals(0, stats.getBackupItemCount());
    }
}