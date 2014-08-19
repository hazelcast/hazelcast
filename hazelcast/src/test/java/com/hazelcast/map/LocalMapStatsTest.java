package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
        assertEquals(100, localMapStats.getHits());
        assertEquals(100, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getGetOperationCount());
        localMapStats = map.getLocalMapStats(); // to ensure stats are recalculated correct.
        assertEquals(100, localMapStats.getHits());
        assertEquals(100, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getGetOperationCount());
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

    @Test
    public void testHits_whenMultipleNodes() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances();
        MultiMap<Object, Object> multiMap0 = instances[0].getMultiMap("testHits_whenMultipleNodes");
        MultiMap<Object, Object> multiMap1 = instances[1].getMultiMap("testHits_whenMultipleNodes");

        // InternalPartitionService is used in order to determine owners of these keys that we will use.
        InternalPartitionService partitionService = getNode(instances[0]).getPartitionService();
        Address address = partitionService.getPartitionOwner(partitionService.getPartitionId("test1"));

        boolean inFirstInstance = address.equals(getNode(instances[0]).getThisAddress());
        multiMap0.get("test0");

        multiMap0.put("test1", 1);
        multiMap1.get("test1");

        assertEquals(inFirstInstance ? 1 : 0, multiMap0.getLocalMultiMapStats().getHits());
        assertEquals(inFirstInstance ? 0 : 1, multiMap1.getLocalMultiMapStats().getHits());

        multiMap0.get("test1");
        multiMap1.get("test1");
        assertEquals(inFirstInstance ? 0 : 3, multiMap1.getLocalMultiMapStats().getHits());
    }
}
