package com.hazelcast.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueStatisticsTest extends AbstractQueueTest{

    //todo: can you clean this up in multiple smaller tests?
    // and there is a lot more statistics to be tested.

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

}