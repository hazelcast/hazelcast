package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LocalMapStatsTest extends HazelcastTestSupport {

    private final String name = "fooMap";

    @Test
    public void testPutAndGetExpectHitsGenerated() throws Exception {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap<Integer, Integer> map = h1.getMap("hits");
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(100,localMapStats.getHits());
        assertEquals(100,localMapStats.getPutOperationCount());
        assertEquals(100,localMapStats.getGetOperationCount());
        localMapStats =  map.getLocalMapStats(); // to ensure stats are recalculated correct.
        assertEquals(100,localMapStats.getHits());
        assertEquals(100,localMapStats.getPutOperationCount());
        assertEquals(100,localMapStats.getGetOperationCount());
    }

    @Test
    public void testLastAccessTime() throws InterruptedException {
        final TimeUnit timeUnit = TimeUnit.NANOSECONDS;
        final long startTime = timeUnit.toMillis(System.nanoTime());

        HazelcastInstance h1 = createHazelcastInstance();
        IMap<String, String> map1 = h1.getMap(name);

        String key = "key";
        map1.put(key, "value");

        long lastUpdateTime = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime >= startTime);

        Thread.sleep(5);
        map1.put(key, "value2");
        long lastUpdateTime2 = map1.getLocalMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime2 > lastUpdateTime);
    }

    @Test
    public void testEvictAll() throws Exception {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap<String, String> map = h1.getMap(name);
        map.put("key", "value");
        map.evictAll();

        final long heapCost = map.getLocalMapStats().getHeapCost();

        assertEquals(0L, heapCost);
    }
}
