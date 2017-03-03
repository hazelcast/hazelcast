package com.hazelcast.collection.impl.list;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListSplitBrainTest extends SplitBrainTestSupport {

    private String name = randomString();
    private int initialCount = 100;
    private int finalCount = initialCount + 50;

    @Override
    protected int[] brains() {
        // 2nd merges to the 1st
        return new int[] {2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        IList<Object> list = instances[0].getList(name);

        for (int i = 0; i < initialCount; i++) {
            list.add("item" + i);
        }

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {

        IList<Object> list1 = firstBrain[0].getList(name);
        for (int i = initialCount; i < finalCount; i++) {
            list1.add("item" + i);
        }

        IList<Object> list2 = secondBrain[0].getList(name);
        for (int i = initialCount; i < finalCount + 10; i++) {
            list2.add("lost-item" + i);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        for (HazelcastInstance instance : instances) {
            IList<Object> list = instance.getList(name);
            assertListContents(list);
        }
    }

    private void assertListContents(IList<Object> list) {
        assertEquals(finalCount, list.size());

        for (int i = 0; i < finalCount; i++) {
            assertEquals("item" + i, list.get(i));
        }
    }
}
