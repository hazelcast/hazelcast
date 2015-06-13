package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NearCacheStatsImplTest {

    private NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();

    @Before
    public void setUp() {
        nearCacheStats.setOwnedEntryCount(501);
        nearCacheStats.incrementOwnedEntryCount();
        nearCacheStats.decrementOwnedEntryCount();
        nearCacheStats.decrementOwnedEntryCount();

        nearCacheStats.setOwnedEntryMemoryCost(1024);
        nearCacheStats.incrementOwnedEntryMemoryCost(512);
        nearCacheStats.decrementOwnedEntryMemoryCost(256);

        nearCacheStats.setHits(600);
        nearCacheStats.incrementHits();
        nearCacheStats.incrementHits();

        nearCacheStats.setMisses(304);
        nearCacheStats.incrementMisses();
    }

    @Test
    public void testDefaultConstructor() {
        assertTrue(nearCacheStats.getCreationTime() > 0);
        assertEquals(500, nearCacheStats.getOwnedEntryCount());
        assertEquals(1280, nearCacheStats.getOwnedEntryMemoryCost());
        assertEquals(602, nearCacheStats.getHits());
        assertEquals(305, nearCacheStats.getMisses());
        assertNotNull(nearCacheStats.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = nearCacheStats.toJson();
        NearCacheStatsImpl deserialized = new NearCacheStatsImpl();
        deserialized.fromJson(serialized);

        assertTrue(deserialized.getCreationTime() > 0);
        assertEquals(500, deserialized.getOwnedEntryCount());
        assertEquals(1280, deserialized.getOwnedEntryMemoryCost());
        assertEquals(602, deserialized.getHits());
        assertEquals(305, deserialized.getMisses());
        assertNotNull(deserialized.toString());
    }

    @Test
    public void testGetRatio_NaN() {
        NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();
        assertEquals(Double.NaN, nearCacheStats.getRatio(), 0.0001);
    }

    @Test
    public void testGetRatio_POSITIVE_INFINITY() {
        NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();
        nearCacheStats.setHits(1);
        assertEquals(Double.POSITIVE_INFINITY, nearCacheStats.getRatio(), 0.0001);
    }

    @Test
    public void testGetRatio_100() {
        NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();
        nearCacheStats.setHits(1);
        nearCacheStats.setMisses(1);
        assertEquals(100d, nearCacheStats.getRatio(), 0.0001);
    }
}
