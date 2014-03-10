package com.hazelcast.client.stress;

import com.hazelcast.client.stress.support.StressTestSupport;
import com.hazelcast.client.stress.support.TestThread;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 *
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapEvectionPolicyStressTest extends StressTestSupport<MapEvectionPolicyStressTest.StressThread> {

    private static final String MAP_NAME = "evictMap";
    private IMap map;

    private int maxSize = 1000;

    @Before
    public void setUp() {

        Config config = new Config();
        MapConfig mc = config.getMapConfig(MAP_NAME);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(0);

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(maxSize);
        mc.setMaxSizeConfig(msc);

        cluster.setConfig( config );
        cluster.initCluster();

        initStressThreadsWithClient(this);

        HazelcastInstance hz = stressThreads.get(0).instance;
        map = hz.getMap(MAP_NAME);
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
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue("map size is bigger than the configured Max size ", map.size() < maxSize * cluster.getSize());
                System.out.println("==>>"+map.size());
            }
        });
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
        public void testLoop() throws Exception {
            map.put(key +" "+keySufix, key);
            key++;
        }
    }
}
