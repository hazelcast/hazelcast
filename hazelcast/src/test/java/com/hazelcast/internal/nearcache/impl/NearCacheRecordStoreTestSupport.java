/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.nearcache.NearCacheStats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

abstract class NearCacheRecordStoreTestSupport extends CommonNearCacheTestSupport {

    void putAndGetRecord(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
        }

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("Record-" + i, nearCacheRecordStore.get(i));
        }
    }

    void putAndRemoveRecord(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
            // ensure that they are stored
            assertNotNull(nearCacheRecordStore.get(i));
        }

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.invalidate(i);
            assertNull(nearCacheRecordStore.get(i));
        }

        assertEquals(0, nearCacheRecordStore.size());
    }

    void clearRecordsOrDestroyStore(InMemoryFormat inMemoryFormat, boolean destroy) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
            // ensure that they are stored
            assertNotNull(nearCacheRecordStore.get(i));
        }

        if (destroy) {
            nearCacheRecordStore.destroy();
        } else {
            nearCacheRecordStore.clear();
        }

        assertEquals(0, nearCacheRecordStore.size());
    }

    void statsCalculated(InMemoryFormat inMemoryFormat) {
        long creationStartTime = System.currentTimeMillis();
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);
        long creationEndTime = System.currentTimeMillis();

        int expectedEntryCount = 0;
        int expectedHits = 0;
        int expectedMisses = 0;

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
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

        // Note that System.currentTimeMillis() is not monotonically increasing.
        // Below assertions can fail anytime but for testing purposes we can use `assertTrueEventually`.
        long nearCacheStatsCreationTime = nearCacheStats.getCreationTime();
        assertTrue("nearCacheStatsCreationTime=" + nearCacheStatsCreationTime + ", and creationStartTime=" + creationStartTime,
                nearCacheStatsCreationTime >= creationStartTime);
        assertTrue("nearCacheStatsCreationTime=" + nearCacheStatsCreationTime + ", and creationEndTime=" + creationEndTime,
                nearCacheStatsCreationTime <= creationEndTime);
        assertEquals(expectedHits, nearCacheStats.getHits());
        assertEquals(expectedMisses, nearCacheStats.getMisses());
        assertEquals(expectedEntryCount, nearCacheStats.getOwnedEntryCount());
        switch (inMemoryFormat) {
            case NATIVE:
            case BINARY:
                assertTrue(memoryCostWhenFull > 0);
                break;
            case OBJECT:
                assertEquals(0, memoryCostWhenFull);
                break;
            default:
                // NOP
        }

        int sizeBefore = nearCacheRecordStore.size();
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.invalidate(i * 3);
        }
        int sizeAfter = nearCacheRecordStore.size();
        int invalidatedSize = sizeBefore - sizeAfter;
        expectedEntryCount -= invalidatedSize;

        assertEquals(expectedEntryCount, nearCacheStats.getOwnedEntryCount());
        switch (inMemoryFormat) {
            case NATIVE:
            case BINARY:
                assertTrue(nearCacheStats.getOwnedEntryMemoryCost() > 0);
                assertTrue(nearCacheStats.getOwnedEntryMemoryCost() < memoryCostWhenFull);
                break;
            case OBJECT:
                assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
                break;
            default:
                // NOP
        }

        nearCacheRecordStore.clear();
        assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
    }

    void ttlEvaluated(InMemoryFormat inMemoryFormat) {
        int ttlSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat)
                .setTimeToLiveSeconds(ttlSeconds);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNotNull(nearCacheRecordStore.get(i));
        }

        sleepSeconds(ttlSeconds + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    void maxIdleTimeEvaluatedSuccessfully(InMemoryFormat inMemoryFormat) {
        int maxIdleSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat)
                .setMaxIdleSeconds(maxIdleSeconds);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNotNull(nearCacheRecordStore.get(i));
        }

        sleepSeconds(maxIdleSeconds + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    void expiredRecordsCleanedUpSuccessfully(InMemoryFormat inMemoryFormat, boolean useIdleTime) {
        int cleanUpThresholdSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        if (useIdleTime) {
            nearCacheConfig.setMaxIdleSeconds(cleanUpThresholdSeconds);
        } else {
            nearCacheConfig.setTimeToLiveSeconds(cleanUpThresholdSeconds);
        }

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
        }

        sleepSeconds(cleanUpThresholdSeconds + 1);

        nearCacheRecordStore.doExpiration();

        assertEquals(0, nearCacheRecordStore.size());

        NearCacheStats nearCacheStats = nearCacheRecordStore.getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
        assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
    }

    void createNearCacheWithMaxSizePolicy(InMemoryFormat inMemoryFormat, MaxSizePolicy maxSizePolicy, int size) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(maxSizePolicy)
                .setSize(size);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat)
                .setEvictionConfig(evictionConfig);

        createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);
    }
}
