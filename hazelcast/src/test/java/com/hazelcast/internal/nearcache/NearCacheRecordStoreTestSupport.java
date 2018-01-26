/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache;

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
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat), inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i);
        }

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertEquals("Record-" + i, nearCacheRecordStore.get(i));
        }
    }

    protected void putAndRemoveRecord(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i);
            // ensure that they are stored
            assertNotNull(nearCacheRecordStore.get(i));
        }

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.remove(i);
            assertNull(nearCacheRecordStore.get(i));
        }

        assertEquals(0, nearCacheRecordStore.size());
    }

    protected void clearRecordsOrDestroyStore(InMemoryFormat inMemoryFormat, boolean destroy) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i);
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

    protected void statsCalculated(InMemoryFormat inMemoryFormat) {
        long creationStartTime = System.currentTimeMillis();
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);
        long creationEndTime = System.currentTimeMillis();

        int expectedEntryCount = 0;
        int expectedHits = 0;
        int expectedMisses = 0;

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i);
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
        assertTrue("nearCacheStatsCreationTime=" + nearCacheStatsCreationTime
                + ", and creationStartTime=" + creationStartTime, nearCacheStatsCreationTime >= creationStartTime);
        assertTrue("nearCacheStatsCreationTime=" + nearCacheStatsCreationTime
                + ", and creationEndTime=" + creationEndTime, nearCacheStatsCreationTime <= creationEndTime);
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
        int ttlSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        nearCacheConfig.setTimeToLiveSeconds(ttlSeconds);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(
                nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNotNull(nearCacheRecordStore.get(i));
        }

        sleepSeconds(ttlSeconds + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    protected void maxIdleTimeEvaluatedSuccessfully(InMemoryFormat inMemoryFormat) {
        int maxIdleSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        nearCacheConfig.setMaxIdleSeconds(maxIdleSeconds);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(
                nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i);
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNotNull(nearCacheRecordStore.get(i));
        }

        sleepSeconds(maxIdleSeconds + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(nearCacheRecordStore.get(i));
        }
    }

    protected void expiredRecordsCleanedUpSuccessfully(InMemoryFormat inMemoryFormat, boolean useIdleTime) {
        int cleanUpThresholdSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        if (useIdleTime) {
            nearCacheConfig.setMaxIdleSeconds(cleanUpThresholdSeconds);
        } else {
            nearCacheConfig.setTimeToLiveSeconds(cleanUpThresholdSeconds);
        }

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(
                nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i);
        }

        sleepSeconds(cleanUpThresholdSeconds + 1);

        nearCacheRecordStore.doExpiration();

        assertEquals(0, nearCacheRecordStore.size());

        NearCacheStats nearCacheStats = nearCacheRecordStore.getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
        assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
    }

    protected void createNearCacheWithMaxSizePolicy(InMemoryFormat inMemoryFormat, EvictionConfig.MaxSizePolicy maxSizePolicy,
                                                    int size) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);

        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(maxSizePolicy);
        evictionConfig.setSize(size);
        nearCacheConfig.setEvictionConfig(evictionConfig);

        createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);
    }
}
