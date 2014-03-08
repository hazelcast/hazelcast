package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TestThread;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;

/**
 *
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapEvectionPolicyStressTest extends StressTestSupport<MapEvectionPolicyStressTest.StressThread> {

    private static final String MAP_NAME = "evictMap";
    private IMap map;

    @Before
    public void setUp() {

        Config config = new Config();
        MapConfig mc = config.getMapConfig(MAP_NAME);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(0);

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(1000);
        mc.setMaxSizeConfig(msc);

        cluster.setConfig( config );
        cluster.initCluster();

        initStressThreadsWithClient(this);

        map = cluster.getRandomNode().getMap(MAP_NAME);
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

            System.out.println("==>>"+map.size());
            sleepSeconds(1);

    }

    public class StressThread extends TestThread {
        private IMap map;
        private int key=0;
        private int keySufix=1;

        public StressThread(HazelcastInstance node){
            super(node);
            map = instance.getMap(MAP_NAME);
        }

        @Override
        public void doRun() throws Exception {
            map.put(key +" "+keySufix, key);
            key++;
        }
    }
}
