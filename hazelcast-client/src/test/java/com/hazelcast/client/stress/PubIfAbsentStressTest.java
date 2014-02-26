package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.EnteryCounter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.IMap;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that map putIfAbsent call are thread safe and not lost.
 * we have a number of HazelCast instances, client / or node.  each instance in used by a number of threads
 * to do the operations.
 * in the end we check that no to threads put the same key.
 * we also check an Entery listener instance in each thread, and check it had the correct number of put events called
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PubIfAbsentStressTest extends StressTestSupport {

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

    @Test
    public void testChangingCluster() {
        runTest(true, stressThreads);
    }

    @Test
    public void testFixedCluster() {
        runTest(false, stressThreads);
    }


    public void assertResult() {

        //there should be no intersection of the set ok keys put but any threads
        for ( int i = 0; i < stressThreads.length; i++ ) {
            for ( int j = i+1; j < stressThreads.length; j++ ) {

                Set a = stressThreads[i].iput;
                Set b = stressThreads[j].iput;

                Set interSec = new HashSet(a);

                interSec.retainAll(b);

                assertEquals("put if absent broken keys "+interSec+" put while present ", 0, interSec.size());
            }
        }


        //checking the map size and EnteryCounter add up
        IMap map = instances.get(0).getMap("map");

        long total=0;
        for ( int i = 0; i < stressThreads.length; i++ ) {

            total += stressThreads[i].iPutCount;

            long enterysAdded = stressThreads[i].enteryCounter.totalAdded.get();

            assertEquals("entery Counter ", enterysAdded, map.size());
        }

        assertEquals("total iPutCount and map size don't match", total, map.size());
    }


    public class StressThread extends TestThread {

        private HazelcastInstance instance;
        private IMap map;

        private int key=0;

        public long iPutCount=0;

        public Set iput = new HashSet();

        public EnteryCounter enteryCounter = new EnteryCounter();

        public StressThread(HazelcastInstance instance){
            super();
            this.instance = instance;
            map = instance.getMap("map");

            map.addEntryListener(enteryCounter, false);
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                if ( map.putIfAbsent(key, this.getName()+" "+key) == null ){

                    iput.add(key);
                    iPutCount++;
                    //System.out.println(this.getName() +" put "+ key);
                }
                key++;
            }
        }
    }


}
