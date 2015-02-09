package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NearCacheRecordStoreTest extends NearCacheTestSupport {

    private static final int DEFAULT_RECORD_COUNT = 100;

    private void putAndGetRecord(InMemoryFormat inMemoryFormat) {
        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat),
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("Record-" + i, nearCacheRecordStore.get(i));
        }
    }

    @Test
    public void putAndGetRecordFromNearCacheObjectRecordStore() {
        putAndGetRecord(InMemoryFormat.OBJECT);
    }

    @Test
    public void putAndGetRecordSuccessfullyFromNearCacheDataRecordStore() {
        putAndGetRecord(InMemoryFormat.BINARY);
    }

    private void putAndRemoveRecord(InMemoryFormat inMemoryFormat) {
        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat),
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
            // Sure that they are stored
            assertNotNull(nearCacheRecordStore.get(i));
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.remove(i);
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    @Test
    public void putAndRemoveRecordFromNearCacheObjectRecordStore() {
        putAndRemoveRecord(InMemoryFormat.OBJECT);
    }

    @Test
    public void putAndRemoveRecordSuccessfullyFromNearCacheDataRecordStore() {
        putAndRemoveRecord(InMemoryFormat.BINARY);
    }

    private void clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(
            InMemoryFormat inMemoryFormat, boolean destroy) {
        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat),
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
            // Sure that they are stored
            assertNotNull(nearCacheRecordStore.get(i));
        }

        if (destroy) {
            nearCacheRecordStore.destroy();
        } else {
            nearCacheRecordStore.clear();
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    @Test
    public void clearRecordsFromNearCacheObjectRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.OBJECT, false);
    }

    @Test
    public void clearRecordsSuccessfullyFromNearCacheDataRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.BINARY, false);
    }

    @Test(expected = IllegalStateException.class)
    public void destoryStoreFromNearCacheObjectRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.OBJECT, true);
    }

    @Test(expected = IllegalStateException.class)
    public void destoryStoreFromNearCacheDataRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.BINARY, true);
    }

    private void statsCalculated(InMemoryFormat inMemoryFormat) {
        long creationStartTime = System.currentTimeMillis();
        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat),
                        createNearCacheContext(),
                        inMemoryFormat);
        long creationEndTime = System.currentTimeMillis();

        int expectedEntryCount = 0;
        int expectedHits = 0;
        int expectedMisses = 0;

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
            expectedEntryCount++;
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            if (nearCacheRecordStore.get(i * 3) != null) {
                expectedHits++;
            } else {
                expectedMisses++;
            }
        }

        NearCacheStats nearCacheStats = nearCacheRecordStore.getNearCacheStats();
        long memoryCostWhenFull = nearCacheStats.getOwnedEntryMemoryCost();

        assertTrue(nearCacheStats.getCreationTime() >= creationStartTime);
        assertTrue(nearCacheStats.getCreationTime() <= creationEndTime);
        assertEquals(expectedHits, nearCacheStats.getHits());
        assertEquals(expectedMisses, nearCacheStats.getMisses());
        assertEquals(expectedEntryCount, nearCacheStats.getOwnedEntryCount());
        switch (inMemoryFormat) {
            case BINARY:
                assertTrue(memoryCostWhenFull > 0);
                break;
            case OBJECT:
                assertEquals(0, memoryCostWhenFull);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            if (nearCacheRecordStore.remove(i * 3)) {
                expectedEntryCount--;
            }
        }

        assertEquals(expectedEntryCount, nearCacheStats.getOwnedEntryCount());
        switch (inMemoryFormat) {
            case BINARY:
                assertTrue(nearCacheStats.getOwnedEntryMemoryCost() > 0);
                assertTrue(nearCacheStats.getOwnedEntryMemoryCost() < memoryCostWhenFull);
                break;
            case OBJECT:
                assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
                break;
        }

        nearCacheRecordStore.clear();

        switch (inMemoryFormat) {
            case BINARY:
            case OBJECT:
                assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
                break;
        }
    }

    @Test
    public void statsCalculatedOnNearCacheObjectRecordStore() {
        statsCalculated(InMemoryFormat.OBJECT);
    }

    @Test
    public void statsCalculatedOnNearCacheDataRecordStore() {
        statsCalculated(InMemoryFormat.BINARY);
    }

    private void ttlEvaluated(InMemoryFormat inMemoryFormat) {
        final int TTL_SECONDS = 3;

        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        nearCacheConfig.setTimeToLiveSeconds(TTL_SECONDS);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        nearCacheConfig,
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNotNull(nearCacheRecordStore.get(i));
        }

        sleepSeconds(TTL_SECONDS + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    @Test
    public void ttlEvaluatedOnNearCacheObjectRecordStore() {
        ttlEvaluated(InMemoryFormat.OBJECT);
    }

    @Test
    public void ttlEvaluatedSuccessfullyOnNearCacheDataRecordStore() {
        ttlEvaluated(InMemoryFormat.BINARY);
    }

    private void maxIdleTimeEvaluatedSuccessfully(InMemoryFormat inMemoryFormat) {
        final int MAX_IDLE_SECONDS = 3;

        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        nearCacheConfig.setMaxIdleSeconds(MAX_IDLE_SECONDS);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        nearCacheConfig,
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNotNull(nearCacheRecordStore.get(i));
        }

        sleepSeconds(MAX_IDLE_SECONDS + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfullyOnNearCacheObjectRecordStore() {
        maxIdleTimeEvaluatedSuccessfully(InMemoryFormat.OBJECT);
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfullyOnNearCacheDataRecordStore() {
        maxIdleTimeEvaluatedSuccessfully(InMemoryFormat.BINARY);
    }

}
