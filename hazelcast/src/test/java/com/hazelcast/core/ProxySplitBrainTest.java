package com.hazelcast.core;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ProxySplitBrainTest extends SplitBrainTestSupport {

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        HazelcastInstance isolatedInstance = firstBrain[0];
        isolatedInstance.getLock("isolatedLock");
        assertDistributedObjectCountEventually(1, isolatedInstance);

        for (HazelcastInstance hz : secondBrain) {
            String name = generateKeyOwnedBy(hz);
            hz.getLock(name);
        }

        for (HazelcastInstance hz : secondBrain) {
            int expectedCount = secondBrain.length;
            assertDistributedObjectCountEventually(expectedCount, hz);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] allInstances) {
        for (HazelcastInstance hz : allInstances) {
            assertDistributedObjectCountEventually(allInstances.length, hz);
        }
    }

    private static void assertDistributedObjectCountEventually(final int expectedCount, final HazelcastInstance hz) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int actualSize = hz.getDistributedObjects().size();
                assertEquals(expectedCount, actualSize);
            }
        });
    }
}
