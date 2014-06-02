package com.hazelcast.map;

import java.util.UUID;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.MapEvictionManager.SCHEDULER_PERIOD;
import static com.hazelcast.map.MapEvictionManager.SCHEDULER_TIME_UNIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith (HazelcastParallelClassRunner.class)
@Category (NightlyTest.class)
public class MapEvictionManagerTest extends HazelcastTestSupport
{
    static final int MAX_SIZE = 100;

    @Test
    public void testEvictionOfMultipleMaps()
    {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = nodeFactory.newHazelcastInstance();

        //default map doesn't have eviction policy
        final IMap<String, String> nonEvictableMap = instance.getMap("A");
        final IMap<String, String> halfFullMap = getConfiguredMapFromInstance(instance, "B");
        final IMap<String, String> toBeEvictedMap = getConfiguredMapFromInstance(instance, "C");

        populateMap(nonEvictableMap, MAX_SIZE * 2);
        populateMap(halfFullMap, MAX_SIZE / 2);
        populateMap(toBeEvictedMap, MAX_SIZE * 2);

        waitForEvictionToHappen();

        assertEquals(nonEvictableMap.size(), MAX_SIZE * 2);
        assertEquals(halfFullMap.size(), MAX_SIZE / 2);
        assertTrue(toBeEvictedMap.size() < MAX_SIZE);
    }

    private void waitForEvictionToHappen()
    {
        try
        {
            Thread.sleep(SCHEDULER_TIME_UNIT.toMillis(SCHEDULER_PERIOD * 2));
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void populateMap(IMap<String, String> map, int numberOfElementsToAdd)
    {
        for (int i = 0; i < numberOfElementsToAdd; i++)
        {
            map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
    }

    private IMap<String, String> getConfiguredMapFromInstance(HazelcastInstance instance, String mapName)
    {
        instance.getConfig().getMapConfig(mapName)
                .setEvictionPolicy(MapConfig.EvictionPolicy.LRU)
                .setMaxSizeConfig(new MaxSizeConfig(MAX_SIZE, MaxSizeConfig.MaxSizePolicy.PER_NODE));

        return instance.getMap(mapName);
    }
}
