package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * User: ahmetmircik
 * Date: 10/31/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class NearCacheTest extends HazelcastTestSupport {

    @Test
    public void testNearCacheEvictionByUsingMapClear() throws InterruptedException {
        final Config cfg = new Config();
        final String mapName = "testNearCacheEviction";
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        cfg.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(cfg);
        final HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(cfg);
        final IMap map1 = hazelcastInstance1.getMap(mapName);
        final IMap map2 = hazelcastInstance2.getMap(mapName);
        final int size = 10;
        //populate map.
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map1.get(i);
            map2.get(i);
        }
        //clear map should trigger near cache eviction.
        map1.clear();
        for (int i = 0; i < size; i++) {
            assertNull(map1.get(i));
        }
    }

    @Test
    public void testNearCacheEvictionByUsingMapTTLEviction() throws InterruptedException {
        final Config cfg = new Config();
        final String mapName = "testNearCacheEvictionByUsingMapTTLEviction";
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        cfg.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        final MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        final MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(50);
        mc.setMaxSizeConfig(msc);
        final int maxTTL = 2;
        final int size = 100000;
        final int nsize = size / 5;
        mc.setTimeToLiveSeconds(maxTTL);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);
        final HazelcastInstance instance3 = factory.newHazelcastInstance(cfg);
        final IMap map1 = instance1.getMap(mapName);
        final IMap map2 = instance2.getMap(mapName);
        final IMap map3 = instance3.getMap(mapName);
        //observe eviction
        final CountDownLatch latch = new CountDownLatch(size - nsize);
        map1.addEntryListener(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, false);
        //populate map
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        //populate near caches
        for (int i = 0; i < nsize; i++) {
            map1.get(i);
            map2.get(i);
            map3.get(i);
        }
        //wait operations to complete
        latch.await(30, TimeUnit.SECONDS);
        //check map sizes after eviction.
        assertEquals(0, map1.size());
        assertEquals(map1.size(), map2.size());
        assertEquals(map1.size(), map3.size());
        // these gets should return null after near cache eviction
        for (int i = 0; i < nsize; i++) {
            assertNull(map1.get(i));
            assertNull(map2.get(i));
            assertNull(map3.get(i));
        }
    }


}
