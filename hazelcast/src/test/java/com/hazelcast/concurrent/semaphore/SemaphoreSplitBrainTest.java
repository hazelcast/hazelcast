package com.hazelcast.concurrent.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SemaphoreSplitBrainTest extends SplitBrainTestSupport {

    private String name;
    private int permits = 5;

    @Override
    protected int[] brains() {
        // 2nd merges to the 1st
        return new int[] {2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        warmUpPartitions(instances);

        HazelcastInstance lastInstance = instances[instances.length - 1];
        name = generateKeyOwnedBy(lastInstance);

        HazelcastInstance firstInstance = instances[0];
        firstInstance.getSemaphore(name).init(permits);
        
        lastInstance.getSemaphore(name).acquire(permits - 2);

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {

        final ISemaphore semaphore = firstBrain[0].getSemaphore(name);

        // when member is down, permits are released.
        // since releasing the permits is async, we use assert eventually
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(permits, semaphore.availablePermits());
            }
        });

        semaphore.acquire(permits - 1);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        for (HazelcastInstance instance : instances) {
            ISemaphore semaphore = instance.getSemaphore(name);
            assertEquals(1, semaphore.availablePermits());
        }
    }
}
