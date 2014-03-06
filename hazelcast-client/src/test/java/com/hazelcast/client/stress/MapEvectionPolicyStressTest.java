package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.EntryCounter;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
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
 *
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapEvectionPolicyStressTest extends StressTestSupport {

    public static int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public static int THREADS_PER_INSTANCE = 5;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_CLIENT_INSTANCES * THREADS_PER_INSTANCE];

    private static final String MAP_NAME = "evictMap";
    private IMap map;

    @Before
    public void setUp() {

        super.RUNNING_TIME_SECONDS=10;

        Config config = new Config();

        MapConfig mc = config.getMapConfig(MAP_NAME);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(0);

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(1000);
        mc.setMaxSizeConfig(msc);

        cluster.setConfig( config );
        super.setUp();

        map = cluster.getRandomNode().getMap(MAP_NAME);

        int index=0;
        for (int i = 0; i < TOTAL_HZ_CLIENT_INSTANCES; i++) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(new ClientConfig());

            for (int j = 0; j < THREADS_PER_INSTANCE; j++) {

                StressThread t = new StressThread(instance, index);
                t.start();
                stressThreads[index++] = t;
            }
        }
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

        while(true){
            System.out.println("==>>"+map.size());
            sleepSeconds(1);
        }
    }

    public class StressThread extends TestThread {

        private HazelcastInstance instance;
        private IMap map;
        private int key=0;
        private int keySufix;

        public StressThread(HazelcastInstance instance, int keySufix){
            this.instance = instance;
            this.keySufix = keySufix;
            map = instance.getMap(MAP_NAME);
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {
                map.put(key +" "+keySufix, key);
                key++;
            }
        }
    }
}
