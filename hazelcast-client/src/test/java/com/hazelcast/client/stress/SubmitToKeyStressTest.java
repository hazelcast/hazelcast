package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.Incromentor;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that submit to keys called on map keys are exicuted the expeted number of times in a
 * multi threaded test.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class SubmitToKeyStressTest extends StressTestSupport {

    public static int TOTAL_HZ_INSTANCES = 5;
    public static int THREADS_PER_INSTANCE = 1;


    private static final String MAP_NAME = "submitToKeys";

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_INSTANCES * THREADS_PER_INSTANCE];

    @Before
    public void setUp() {
        super.setUp();

        //make the initial key val that all thread will run task on
        HazelcastInstance hz = super.cluster.getRandomNode();
        hz.getMap(MAP_NAME).put(0, 0);

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

        int total=0;
        for ( StressThread s : stressThreads ) {
            total += s.totalOpps;
        }

        IMap map = cluster.getRandomNode().getMap(MAP_NAME);

        assertEquals(total, map.get(0));
    }


    public class StressThread extends TestThread{

        private HazelcastInstance instance;
        private EntryProcessor task;
        private IMap map;
        private int totalOpps=0;

        public StressThread(HazelcastInstance node){
            super();

            instance = node;
            map = instance.getMap(MAP_NAME);

            task = new Incromentor();
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                Future f = map.submitToKey(0, task);
                Object obj = f.get();
                totalOpps++;
            }
        }
    }

}
