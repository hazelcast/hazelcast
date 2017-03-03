package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CountDownLatchSplitBrainTest extends SplitBrainTestSupport {

    private String name;
    private int count = 5;

    @Override
    protected int[] brains() {
        // 2nd merges to the 1st
        return new int[] {2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        warmUpPartitions(instances);
        name = generateKeyOwnedBy(instances[instances.length - 1]);

        ICountDownLatch latch = instances[0].getCountDownLatch(name);
        latch.trySetCount(count);

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {

        ICountDownLatch latch1 = firstBrain[0].getCountDownLatch(name);
        // count = 4
        latch1.countDown();
        count = latch1.getCount();

        ICountDownLatch latch2 = secondBrain[0].getCountDownLatch(name);
        // count = 0
        while (latch2.getCount() > 0) {
            latch2.countDown();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        for (HazelcastInstance instance : instances) {
            ICountDownLatch latch = instance.getCountDownLatch(name);
            assertEquals(count, latch.getCount());
        }
    }
}
