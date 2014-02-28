package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.ItemCounter;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
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
public class SetStressTest extends StressTestSupport {

    public static int TOTAL_HZ_INSTANCES = 5;
    public static int THREADS_PER_INSTANCE = 2;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_INSTANCES * THREADS_PER_INSTANCE];

    @Before
    public void setUp() {
        super.setUp();

        int index=0;
        for (int i = 0; i < TOTAL_HZ_INSTANCES; i++) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(new ClientConfig());

            for (int j = 0; j < THREADS_PER_INSTANCE; j++) {

                StressThread t = new StressThread(instance);
                t.start();
                stressThreads[index++] = t;
            }
        }
    }


    @After
    public void tearDown() {
        super.tearDown();
    }

    //@Test
    public void testChangingCluster() {
        runTest(true, stressThreads);
    }

    @Test
    public void testFixedCluster() {
        runTest(false, stressThreads);
    }

    public void assertResult() {

        HazelcastInstance hz = cluster.getRandomNode();
        ISet<Long> expeted = hz.getSet("set");

        long total=0;
        for ( StressThread s : stressThreads ) {
            total += s.count;

            long itemCount = s.itemCounter.totalAdded.get();
            assertEquals(s.itemCounter + " itemCounter instance has wrong total ", itemCount, expeted.size());
        }

        assertEquals(expeted+" total count != set size ", total, (long) expeted.size());
    }


    public class StressThread extends TestThread{

        private HazelcastInstance instance;

        ISet<Long> set;

        public long count=0;
        public long key=0;

        public ItemCounter itemCounter = new ItemCounter();

        public StressThread(HazelcastInstance node){
            super();

            instance = node;

            set = instance.getSet("set");
            set.addItemListener(itemCounter, false);
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {
                if ( set.add(key) ) {
                    count++;
                }
                key++;
            }
        }
    }

}
