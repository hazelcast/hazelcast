package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * @ali 21/11/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class NearCacheTest extends HazelcastTestSupport {

    @Before
    public void reset(){
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNearCache(){
        final String name = "map";
        final Config config = new Config();
        final MapConfig mapConfig = config.getMapConfig(name);
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        mapConfig.setNearCacheConfig(nearCacheConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        final IMap<Object,Object> map = instance1.getMap(name);
        for (int i=0; i<10; i++){
            map.put(i, "value"+i);
        }

        long begin = System.currentTimeMillis();
        for (int i=0; i<10; i++){
            map.get(i);
        }
        final long beforeCache = System.currentTimeMillis() - begin;


        begin = System.currentTimeMillis();
        for (int i=0; i<10; i++){
            map.get(i);
        }
        final long afterCache = System.currentTimeMillis() - begin;


        assertTrue(beforeCache > afterCache);




    }

}
