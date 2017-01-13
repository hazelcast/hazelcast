package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheStatsImplTest extends HazelcastTestSupport {

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
        assertNearCacheStats(nearCacheStats, 1, 200, 300, 400, false);
    }

    @Test
    public void testSerialization() {
        NearCacheStatsImpl deserialized = serializeAndDeserializeNearCacheStats(nearCacheStats);

        assertNearCacheStats(deserialized, 1, 200, 300, 400, false);
    }

    @Test
    public void testSerialization_withPersistenceFailure() {
        Throwable throwable = new FileNotFoundException("expected exception");
        nearCacheStats.addPersistenceFailure(throwable);

        NearCacheStatsImpl deserialized = serializeAndDeserializeNearCacheStats(nearCacheStats);

        assertNearCacheStats(deserialized, 2, 0, 0, 0, true);

        String lastPersistenceFailure = deserialized.getLastPersistenceFailure();
        assertContains(lastPersistenceFailure, throwable.getClass().getSimpleName());
        assertContains(lastPersistenceFailure, "expected exception");
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

    private static NearCacheStatsImpl serializeAndDeserializeNearCacheStats(NearCacheStatsImpl original) {
        JsonObject serialized = original.toJson();

        NearCacheStatsImpl deserialized = new NearCacheStatsImpl();
        deserialized.fromJson(serialized);
        return deserialized;
    }

    private static void assertNearCacheStats(NearCacheStatsImpl stats, long expectedPersistenceCount, long expectedDuration,
                                             long expectedWrittenBytes, long expectedKeyCount, boolean expectedFailure) {
        assertTrue(stats.getCreationTime() > 0);
        assertEquals(500, stats.getOwnedEntryCount());
        assertEquals(1280, stats.getOwnedEntryMemoryCost());
        assertEquals(602, stats.getHits());
        assertEquals(305, stats.getMisses());
        assertEquals(4, stats.getEvictions());
        assertEquals(3, stats.getExpirations());
        assertEquals(expectedPersistenceCount, stats.getPersistenceCount());
        assertTrue(stats.getLastPersistenceTime() > 0);
        assertEquals(expectedDuration, stats.getLastPersistenceDuration());
        assertEquals(expectedWrittenBytes, stats.getLastPersistenceWrittenBytes());
        assertEquals(expectedKeyCount, stats.getLastPersistenceKeyCount());
        if (expectedFailure) {
            assertFalse(stats.getLastPersistenceFailure().isEmpty());
        } else {
            assertTrue(stats.getLastPersistenceFailure().isEmpty());
        }
        assertNotNull(stats.toString());
    }
}
