/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

abstract class NearCacheRecordStoreTestSupport extends CommonNearCacheTestSupport {

    void putAndGetRecord(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            putToRecordStore(nearCacheRecordStore, i, "Record-" + i);
        }

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

    }

    private String putToRecordStore(NearCacheRecordStore<Integer, String> nearCacheRecordStore, int key, final Object value) {
        return nearCacheRecordStore.getOrFetch(key, new Supplier<ICompletableFuture>() {
            @Override
            public ICompletableFuture get() {
                return new CompletedFuture(ss, value, new Executor() {
                    @Override
                    public void execute(Runnable command) {
                        command.run();
                    }
                });
            }
        });
    }

    void putAndRemoveRecord(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = putToRecordStore(nearCacheRecordStore, i, "Record-" + i);
            assertNotNull(value);
        }

        assertEquals(DEFAULT_RECORD_COUNT, nearCacheRecordStore.size());

        assertEquals(0, nearCacheRecordStore.size());
    }

    void clearRecordsOrDestroyStore(InMemoryFormat inMemoryFormat, boolean destroy) {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = putToRecordStore(nearCacheRecordStore, i, "Record-" + i);
            assertNotNull(value);
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
            putToRecordStore(nearCacheRecordStore, i, "Record-" + i);
            expectedEntryCount++;
        }

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = putToRecordStore(nearCacheRecordStore, i * 3, null);
            if (value != null) {
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
            assertNotNull(putToRecordStore(nearCacheRecordStore, i, "Record-" + i));
        }

        sleepSeconds(ttlSeconds + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(putToRecordStore(nearCacheRecordStore, i, null));
        }
    }

    void maxIdleTimeEvaluatedSuccessfully(InMemoryFormat inMemoryFormat) {
        int maxIdleSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat)
                .setMaxIdleSeconds(maxIdleSeconds);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNotNull(putToRecordStore(nearCacheRecordStore, i, "Record-" + i));
        }

        sleepSeconds(maxIdleSeconds + 1);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            assertNull(putToRecordStore(nearCacheRecordStore, i, null));
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
            // nearCacheRecordStore.put(i, null, "Record-" + i, null);
        }

        sleepSeconds(cleanUpThresholdSeconds + 1);

        nearCacheRecordStore.doExpiration();

        assertEquals(0, nearCacheRecordStore.size());

        NearCacheStats nearCacheStats = nearCacheRecordStore.getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
        assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
    }

    void createNearCacheWithMaxSizePolicy(InMemoryFormat inMemoryFormat, EvictionConfig.MaxSizePolicy maxSizePolicy, int size) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(maxSizePolicy)
                .setSize(size);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat)
                .setEvictionConfig(evictionConfig);

        createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);
    }
}
