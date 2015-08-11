package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalCacheStatsImplTest {

    @Test
    public void testDefaultConstructor() {
        LocalCacheStatsImpl localCacheStats = new LocalCacheStatsImpl();

        assertEquals(0, localCacheStats.getCacheHits());
        assertEquals(0, localCacheStats.getCacheHitPercentage(), 0.0001);
        assertEquals(0, localCacheStats.getCacheMisses());
        assertEquals(0, localCacheStats.getCacheMissPercentage(), 0.0001);
        assertEquals(0, localCacheStats.getCachePuts());
        assertEquals(0, localCacheStats.getCacheGets());
        assertEquals(0, localCacheStats.getCacheRemovals());
        assertEquals(0, localCacheStats.getCacheEvictions());
        assertEquals(0, localCacheStats.getAverageGetTime(), 0.0001);
        assertEquals(0, localCacheStats.getAveragePutTime(), 0.0001);
        assertEquals(0, localCacheStats.getAverageRemoveTime(), 0.0001);
        assertEquals(0, localCacheStats.getCreationTime());
        assertNotNull(localCacheStats.toString());
    }

    @Test
    public void testSerialization() {
        CacheStatistics cacheStatistics = new CacheStatistics() {
            @Override
            public long getCacheHits() {
                return 127;
            }

            @Override
            public float getCacheHitPercentage() {
                return 12.5f;
            }

            @Override
            public long getCacheMisses() {
                return 5;
            }

            @Override
            public float getCacheMissPercentage() {
                return 11.4f;
            }

            @Override
            public long getCacheGets() {
                return 6;
            }

            @Override
            public long getCachePuts() {
                return 7;
            }

            @Override
            public long getCacheRemovals() {
                return 8;
            }

            @Override
            public long getCacheEvictions() {
                return 9;
            }

            @Override
            public float getAverageGetTime() {
                return 23.42f;
            }

            @Override
            public float getAveragePutTime() {
                return 42.23f;
            }

            @Override
            public float getAverageRemoveTime() {
                return 127.45f;
            }
        };

        LocalCacheStatsImpl localCacheStats = new LocalCacheStatsImpl(cacheStatistics);

        JsonObject serialized = localCacheStats.toJson();
        LocalCacheStatsImpl deserialized = new LocalCacheStatsImpl();
        deserialized.fromJson(serialized);

        assertEquals(127, deserialized.getCacheHits());
        assertEquals(12.5f, deserialized.getCacheHitPercentage(), 0.0001);
        assertEquals(5, deserialized.getCacheMisses());
        assertEquals(11.4f, deserialized.getCacheMissPercentage(), 0.0001);
        assertEquals(6, deserialized.getCacheGets());
        assertEquals(7, deserialized.getCachePuts());
        assertEquals(8, deserialized.getCacheRemovals());
        assertEquals(9, deserialized.getCacheEvictions());
        assertEquals(23.42f, deserialized.getAverageGetTime(), 0.0001);
        assertEquals(42.23f, deserialized.getAveragePutTime(), 0.0001);
        assertEquals(127.45f, deserialized.getAverageRemoveTime(), 0.0001);
        assertTrue(deserialized.getCreationTime() > 0);
        assertNotNull(deserialized.toString());
    }
}
