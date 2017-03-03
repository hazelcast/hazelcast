package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockSplitBrainTest extends SplitBrainTestSupport {

    private String key;

    @Override
    protected int[] brains() {
        // 2nd merges to the 1st
        return new int[] {2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        warmUpPartitions(instances);

        HazelcastInstance lastInstance = instances[instances.length - 1];
        key = generateKeyOwnedBy(lastInstance);

        ILock lock = lastInstance.getLock(key);
        lock.lock();

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {

        // acquire lock on 1st brain
        firstBrain[0].getLock(key).lock();

        // release lock on 2nd brain
        secondBrain[0].getLock(key).forceUnlock();
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        // all instances observe lock as acquired
        for (HazelcastInstance instance : instances) {
            ILock lock = instance.getLock(key);
            assertTrue(lock.isLocked());
        }
    }
}
