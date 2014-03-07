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

import static org.junit.Assert.assertEquals;

/**
 * This tests puts a lot of key/values in a map, where the value is the same as the key. With a client these
 * key/values are read and are expected to be consistent, even if member join and leave the cluster all the time.
 * <p/>
 * If there would be a bug in replicating the data, it could pop up here.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapStableReadStressTest extends StressTestSupport<MapStableReadStressTest.StressThread>{

    public static final int MAP_SIZE = 100 * 1000;
    private static final String mapName = "map";

    @Before
    public void setUp() {
        RUNNING_TIME_SECONDS=10;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);

        super.setClientConfig(clientConfig);
        super.setUp(this);

        IMap map = cluster.getRandomNode().getMap(mapName);
        for (int k = 0; k < MAP_SIZE; k++) {
            map.put(k, k);
        }
    }

    @Test
    public void testChangingCluster() {
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    public class StressThread extends TestThread {

        private IMap<Integer, Integer> map;

        public StressThread(HazelcastInstance node){
            super(node);
            map = instance.getMap(mapName);
        }

        @Override
        public void doRun() throws Exception {
            int key = random.nextInt(MAP_SIZE);
            int value = map.get(key);
            assertEquals("The value for the key was not consistent", key, value);
        }
    }
}
