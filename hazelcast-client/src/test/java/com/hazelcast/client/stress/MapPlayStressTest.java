package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.EntryCounter;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that map putIfAbsent call is thread safe and not lost.
 * we have a number of HazelCast instances, client / or node.  each instance in used by a number of threads
 * to do the operations.
 * in the end we check that no to threads put the same key.
 * we also check an Entery listener instance in each thread, and check it had the correct number of put events called
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPlayStressTest extends StressTestSupport {

    public static int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public static int THREADS_PER_INSTANCE = 5;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_CLIENT_INSTANCES * THREADS_PER_INSTANCE];

    private static final String MAP_NAME = "putIfAbsentStressTest";

    private IMap map;

    @Before
    public void setUp() {
        super.setUp();

        HazelcastInstance hz = cluster.getRandomNode();
        map = hz.getMap(MAP_NAME);

        for(int i=0; i<1000; i++){
            map.put(i, i);
        }

        int index=0;
        for (int i = 0; i < TOTAL_HZ_CLIENT_INSTANCES; i++) {

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

        for(StressThread s: stressThreads){
            s.instance.shutdown();
        }
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

        long total=0;
        for ( int i = 0; i < stressThreads.length; i++ ) {

            total += stressThreads[i].count;

        }

        assertEquals("total evected count and map size don't match", 0, map.size());
        assertEquals("total evected count and map size don't match", 1000, total);
    }

    public class StressThread extends TestThread {

        private HazelcastInstance instance;
        private IMap map;

        private int key=0;
        public long count=0;

        public StressThread(HazelcastInstance instance){

            this.instance = instance;
            map = instance.getMap(MAP_NAME);
        }

        @Override
        public void doRun() throws Exception {
            while ( !isStopped() ) {
                if ( map.containsKey(key) ) {
                    map.evict(key);
                    count++;
                }
                key++;
            }
        }
    }

}
