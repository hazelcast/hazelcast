package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TestThread;
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

import static org.junit.Assert.fail;

/**
 * This tests verifies that map updates are not lost. So we have a client which is going to do updates on a map
 * and in the end we verify that the actual updates in the map, are the same as the expected updates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapUpdateStressTest extends StressTestSupport<MapUpdateStressTest.StressThread>{

    public static final int CLIENT_THREAD_COUNT = 5;
    public static final int MAP_SIZE = 100 * 1000;

    private HazelcastInstance client;
    private IMap<Integer, Integer> map;
    private StressThread[] stressThreads;

    @Before
    public void setUp() {
        super.setUp();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);

        client = HazelcastClient.newHazelcastClient(clientConfig);
        map = client.getMap("map");

        fillMap();


    }

    @After
    public void tearDown() {
        super.tearDown();

        if (client != null) {
            client.shutdown();
        }
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
        int[] increments = new int[MAP_SIZE];
        for (StressThread t : stressThreads) {
            t.addIncrements(increments);
        }

        Set<Integer> failedKeys = new HashSet<Integer>();
        for (int k = 0; k < MAP_SIZE; k++) {
            int expectedValue = increments[k];
            int foundValue = map.get(k);
            if (expectedValue != foundValue) {
                failedKeys.add(k);
            }
        }

        if (failedKeys.isEmpty()) {
            return;
        }

        int index = 1;
        for (Integer key : failedKeys) {
            System.err.println("Failed write: " + index + " found:" + map.get(key) + " expected:" + increments[key]);
            index++;
        }

        fail("There are failed writes, number of failures:" + failedKeys.size());
    }

    private void fillMap() {

        for (int k = 0; k < MAP_SIZE; k++) {
            map.put(k, 0);
            if (k % 10000 == 0) {
                System.out.println("Inserted data: " + k);
            }
        }
    }

    public class StressThread extends TestThread {

        private final int[] increments = new int[MAP_SIZE];

        public StressThread(HazelcastInstance node){
            super(node);
        }

        @Override
        public void doRun() throws Exception {

                int key = random.nextInt(MAP_SIZE);
                int increment = random.nextInt(10);
                increments[key] += increment;

                for (; ; ) {
                    int oldValue = map.get(key);
                    if (map.replace(key, oldValue, oldValue + increment)) {

                        break;
                    }
                }

        }

        public void addIncrements(int[] increments) {
            for (int k = 0; k < increments.length; k++) {
                increments[k] += this.increments[k];
            }
        }
    }
}
