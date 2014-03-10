package com.hazelcast.client.stress;

import com.hazelcast.client.stress.helpers.ItemCounter;
import com.hazelcast.client.stress.support.StressTestSupport;
import com.hazelcast.client.stress.support.TestThread;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that set adds are not lost.
 * we have a number of HazelCast instances, client / or node.  each instance in used by a number of threads
 * to do set add operations.
 * in the end we check the total number of set add operations done by all threads to the size of the set.
 * we also check an Item listener instance in each thread, and check it had the correct number of add events called
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class SetStressTest extends StressTestSupport<SetStressTest.StressThread> {

    protected String setName = "setName1";

    @Before
    public void setUp() {
        cluster.initCluster();
        initStressThreadsWithClient(this);
    }

    @Test
    public void testChangingCluster() {
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    public void assertResult() {

        HazelcastInstance hz = cluster.getRandomNode();
        ISet<Long> expected = hz.getSet(setName);

        long total=0;
        for ( StressThread s : stressThreads ) {
            total += s.count;

            long itemCount = s.itemCounter.totalAdded.get();
            assertEquals(s.itemCounter + " itemCounter instance has wrong total ", itemCount, expected.size());
        }

        assertEquals(expected+" total count != set size ", total, (long) expected.size());
    }

    public class StressThread extends TestThread {
        public ISet<Long> set;
        public long count=0;
        public long key=0;

        public ItemCounter itemCounter = new ItemCounter();

        public StressThread(HazelcastInstance node){
            super(node);
            set = instance.getSet(setName);
            set.addItemListener(itemCounter, false);
        }

        @Override
        public void testLoop() throws Exception {
            if ( set.add(key) ) {
                count++;
            }
            key++;
        }
    }

}
