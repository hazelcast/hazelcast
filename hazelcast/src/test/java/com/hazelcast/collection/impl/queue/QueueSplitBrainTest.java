package com.hazelcast.collection.impl.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueSplitBrainTest extends SplitBrainTestSupport {

    private String name = randomString();
    private final int initialCount = 100;
    private final int finalCount = initialCount + 50;

    @Override
    protected int[] brains() {
        // 2nd merges to the 1st
        return new int[] {2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        IQueue<Object> queue = instances[0].getQueue(name);

        for (int i = 0; i < initialCount; i++) {
            queue.offer("item" + i);
        }

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {

        IQueue<Object> queue1 = firstBrain[0].getQueue(name);
        for (int i = initialCount; i < finalCount; i++) {
            queue1.offer("item" + i);
        }

        IQueue<Object> queue2 = secondBrain[0].getQueue(name);
        for (int i = initialCount; i < finalCount + 10; i++) {
            queue2.offer("lost-item" + i);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        for (HazelcastInstance instance : instances) {
            IQueue<Object> queue = instance.getQueue(name);
            assertEquals(finalCount, queue.size());
        }

        IQueue<Object> queue = instances[instances.length - 1].getQueue(name);
        for (int i = 0; i < finalCount; i++) {
            assertEquals("item" + i, queue.poll());
        }
    }
}
