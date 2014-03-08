package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.Incrementor;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TestThread;
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
 * This tests verifies that submit to keys called on map keys are executed the expected number of times in a
 * multi threaded test.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class SubmitToKeyStressTest extends StressTestSupport<SubmitToKeyStressTest.StressThread> {

    private static final String MAP_NAME = "submitToKeys";

    @Before
    public void setUp() {
        cluster.initCluster();
        initStressThreadsWithClient(this);

        HazelcastInstance hz = cluster.getRandomNode();
        hz.getMap(MAP_NAME).put(0, 0);
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

        int total=0;
        for ( StressThread s : stressThreads ) {
            total += s.totalOpps;
        }

        IMap map = cluster.getRandomNode().getMap(MAP_NAME);

        assertEquals(total, map.get(0));
    }

    public class StressThread extends TestThread {
        private EntryProcessor task;
        private IMap map;
        private int totalOpps=0;

        public StressThread(HazelcastInstance node){
            super(node);
            map = instance.getMap(MAP_NAME);
            task = new Incrementor();
        }

        @Override
        public void doRun() throws Exception {
            Future f = map.submitToKey(0, task);
            Object obj = f.get();
            totalOpps++;
        }
    }

}
