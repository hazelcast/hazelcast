package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheStatsImplTest {

    private NearCacheStatsImpl nearCacheStats;

    @Before
    public void setUp() {
        nearCacheStats = new NearCacheStatsImpl();

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

        nearCacheStats.incrementEvictions();
        nearCacheStats.incrementEvictions();
        nearCacheStats.incrementEvictions();
        nearCacheStats.incrementEvictions();

        nearCacheStats.incrementExpirations();
        nearCacheStats.incrementExpirations();
        nearCacheStats.incrementExpirations();

        nearCacheStats.addPersistence(200, 300, 400);
    }

    @Test
    public void testDefaultConstructor() {
        assertTrue(nearCacheStats.getCreationTime() > 0);
        assertEquals(500, nearCacheStats.getOwnedEntryCount());
        assertEquals(1280, nearCacheStats.getOwnedEntryMemoryCost());
        assertEquals(602, nearCacheStats.getHits());
        assertEquals(305, nearCacheStats.getMisses());
        assertEquals(4, nearCacheStats.getEvictions());
        assertEquals(3, nearCacheStats.getExpirations());
        assertEquals(1, nearCacheStats.getPersistenceCount());
        assertTrue(nearCacheStats.getLastPersistenceTime() > 0);
        assertEquals(200, nearCacheStats.getLastPersistenceDuration());
        assertEquals(300, nearCacheStats.getLastPersistenceWrittenBytes());
        assertEquals(400, nearCacheStats.getLastPersistenceKeyCount());
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
        assertEquals(4, deserialized.getEvictions());
        assertEquals(3, deserialized.getExpirations());
        assertEquals(1, deserialized.getPersistenceCount());
        assertTrue(deserialized.getLastPersistenceTime() > 0);
        assertEquals(200, deserialized.getLastPersistenceDuration());
        assertEquals(300, deserialized.getLastPersistenceWrittenBytes());
        assertEquals(400, deserialized.getLastPersistenceKeyCount());
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
