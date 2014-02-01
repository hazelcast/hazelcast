package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapUpdateStressTest extends StressTestSupport {

    public static final int CLIENT_THREAD_COUNT = 5;
    public static final int RUNNING_TIME_SECONDS = 60;
    public static final int MAP_SIZE = 100 * 1000;

    private HazelcastInstance client;
    private IMap<Integer, Integer> map;
    private StressThread[] stressThreads;

    @Before
    public void setUp() {
        super.setUp();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);
        map = client.getMap("map");

        stressThreads = new StressThread[CLIENT_THREAD_COUNT];
        for (int k = 0; k < stressThreads.length; k++) {
            stressThreads[k] = new StressThread();
            stressThreads[k].start();
        }
    }

    @After
    public void tearDown() {
        super.tearDown();

        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void test() throws Exception {
        System.out.println("==================================================================");
        System.out.println("Inserting data in map");
        System.out.println("==================================================================");

        for (int k = 0; k < MAP_SIZE; k++) {
            map.put(k, 0);
            if (k % 10000 == 0) {
                System.out.println("Inserted data: " + k);
            }
        }

        System.out.println("==================================================================");
        System.out.println("Completed with inserting data in map");
        System.out.println("==================================================================");


        startTest();

        Thread.sleep(TimeUnit.SECONDS.toMillis(RUNNING_TIME_SECONDS));

        System.out.println("==================================================================");
        System.out.println("Completed the running period, shutting down threads.");
        System.out.println("==================================================================");

        stopTest();

        joinAll(stressThreads);

        assertNoErrors(stressThreads);
    }

    public class StressThread extends TestThread {

        private final int[] increments = new int[MAP_SIZE];

        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                //int key = random.nextInt(MAP_SIZE);
                //int increment = map.put()
                //assertEquals("The value for the key was not consistent", key, value);
            }
        }
    }
}
