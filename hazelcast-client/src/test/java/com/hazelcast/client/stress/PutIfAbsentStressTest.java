package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.EntryCounter;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TestThread;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.IMap;

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
public class PutIfAbsentStressTest extends StressTestSupport<PutIfAbsentStressTest.StressThread>{

    private static final String MAP_NAME = "putIfAbsentStressTest";

    @Before
    public void setUp() {
        super.setUp();
    }

    //@Test
    public void testChangingCluster() {
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    public void assertResult() {
        //there should be no intersection of the set of keys put but any threads
        for ( int i = 0; i < stressThreads.size(); i++ ) {
            for ( int j = i+1; j < stressThreads.size(); j++ ) {

                Set a = stressThreads.get(i).iput;
                Set b = stressThreads.get(j).iput;

                Set interSec = new HashSet(a);

                interSec.retainAll(b);

                assertEquals("put if absent broken keys "+interSec+" put while present ", 0, interSec.size());
            }
        }

        //checking the map size and EntryCounter add up
        HazelcastInstance hz = cluster.getRandomNode();
        IMap map = hz.getMap(MAP_NAME);

        long total=0;
        for ( int i = 0; i < stressThreads.size(); i++ ) {

            total += stressThreads.get(i).iPutCount;

            long enterysAdded = stressThreads.get(i).enteryCounter.totalAdded.get();

            assertEquals("entry Counter ", enterysAdded, map.size());
        }

        assertEquals("total putCount and map size don't match", total, map.size());
    }

    public class StressThread extends TestThread {

        private HazelcastInstance instance;
        private IMap map;

        private int key=0;
        public long iPutCount=0;

        public Set iput = new HashSet();

        public EntryCounter enteryCounter = new EntryCounter();

        public StressThread(HazelcastInstance node){
            super(node);
            map = instance.getMap(MAP_NAME);

            map.addEntryListener(enteryCounter, false);
        }

        @Override
        public void doRun() throws Exception {
            if ( map.putIfAbsent(key, this.getName()+" "+key) == null ){
                iput.add(key);
                iPutCount++;
            }
            key++;
        }
    }

}
