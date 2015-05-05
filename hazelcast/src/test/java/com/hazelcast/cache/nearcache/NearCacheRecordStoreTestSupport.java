package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.NearCacheStats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class NearCacheRecordStoreTestSupport extends CommonNearCacheTestSupport {

    protected void putAndGetRecord(InMemoryFormat inMemoryFormat) {
        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat),
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
        }

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("Record-" + i, nearCacheRecordStore.get(i));
        }
    }

    protected void putAndRemoveRecord(InMemoryFormat inMemoryFormat) {
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

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.remove(i);
            assertNull(nearCacheRecordStore.get(i));
        }

        assertEquals(0, nearCacheRecordStore.size());
    }

    protected void clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(
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

        assertEquals(0, nearCacheRecordStore.size());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    protected void statsCalculated(InMemoryFormat inMemoryFormat) {
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

    protected void ttlEvaluated(InMemoryFormat inMemoryFormat) {
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

    protected void maxIdleTimeEvaluatedSuccessfully(InMemoryFormat inMemoryFormat) {
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

    protected void expiredRecordsCleanedUpSuccessfully(InMemoryFormat inMemoryFormat, boolean useIdleTime) {
        final int CLEAN_UP_THRESHOLD_SECONDS = 3;

        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        if (useIdleTime) {
            nearCacheConfig.setMaxIdleSeconds(CLEAN_UP_THRESHOLD_SECONDS);
        } else {
            nearCacheConfig.setTimeToLiveSeconds(CLEAN_UP_THRESHOLD_SECONDS);
        }

        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        nearCacheConfig,
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
        }

        sleepSeconds(CLEAN_UP_THRESHOLD_SECONDS + 1);

        nearCacheRecordStore.doExpiration();

        assertEquals(0, nearCacheRecordStore.size());

        NearCacheStats nearCacheStats = nearCacheRecordStore.getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
        assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
    }

    protected void createNearCacheWithMaxSizePolicy(InMemoryFormat inMemoryFormat,
                                                    EvictionConfig.MaxSizePolicy maxSizePolicy,
                                                    int size) {
        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);

        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(maxSizePolicy);
        evictionConfig.setSize(size);
        nearCacheConfig.setEvictionConfig(evictionConfig);

        createNearCacheRecordStore(nearCacheConfig, createNearCacheContext(), inMemoryFormat);
    }

}
