/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.adapter.MethodAvailableMatcher;
import com.hazelcast.internal.adapter.ReplicatedMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.record.NearCacheDataRecord;
import com.hazelcast.internal.nearcache.impl.record.NearCacheObjectRecord;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * Provides utility methods for unified Near Cache tests.
 */
@SuppressWarnings("WeakerAccess")
public final class NearCacheTestUtils extends HazelcastTestSupport {

    private NearCacheTestUtils() {
    }

    /**
     * Retrieves the value of a {@link Future} and throws {@link AssertionError} on failures.
     *
     * @param future  the {@link Future} to get the value from
     * @param message a failure message
     * @param <T>     the return type of the {@link Future}
     * @return the value of the {@link Future}
     */
    public static <T> T getFuture(Future<T> future, String message) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new AssertionError(message + " " + e.getMessage());
        } catch (ExecutionException e) {
            throw new AssertionError(message + " " + e.getMessage());
        }
    }

    /**
     * Creates a {@link NearCacheConfig} with a given {@link InMemoryFormat}.
     *
     * @param inMemoryFormat the {@link InMemoryFormat} to set
     * @param serializeKeys  defines if Near Caches keys should be serialized
     * @return the {@link NearCacheConfig}
     */
    public static NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat, boolean serializeKeys) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(AbstractNearCacheBasicTest.DEFAULT_NEAR_CACHE_NAME + "*")
                .setInMemoryFormat(inMemoryFormat)
                .setSerializeKeys(serializeKeys)
                .setInvalidateOnChange(false);

        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            setEvictionConfig(nearCacheConfig, LRU, USED_NATIVE_MEMORY_PERCENTAGE, 90);
        }
        return nearCacheConfig;
    }

    /**
     * Configures the {@link EvictionConfig} of the given {@link NearCacheConfig}.
     *
     * @param nearCacheConfig the {@link NearCacheConfig} to configure
     * @param evictionPolicy  the {@link EvictionPolicy} to set
     * @param maxSizePolicy   the {@link MaxSizePolicy} to set
     * @param maxSize         the max size to set
     */
    public static void setEvictionConfig(NearCacheConfig nearCacheConfig, EvictionPolicy evictionPolicy,
                                         MaxSizePolicy maxSizePolicy, int maxSize) {
        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaximumSizePolicy(maxSizePolicy)
                .setSize(maxSize);
    }

    /**
     * Returns the {@link MapNearCacheManager} from a given {@link HazelcastInstance}.
     *
     * @param instance the {@link HazelcastInstance} to retrieve the {@link MapNearCacheManager} from
     * @return the {@link MapNearCacheManager}
     */
    public static MapNearCacheManager getMapNearCacheManager(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNode(instance).nodeEngine;
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);

        return service.getMapServiceContext().getMapNearCacheManager();
    }

    /**
     * Returns the key used by internally by the NearCache, depending on the data structure and key serialization.
     *
     * @param context the {@link NearCacheTestContext} to retrieve the Near Cache from
     * @param key     the key in the user format
     * @return the key in the Near Cache format
     */
    public static Object getNearCacheKey(NearCacheTestContext<?, ?, ?, ?> context, Object key) {
        boolean serializeKeys = context.nearCacheConfig.isSerializeKeys();
        boolean isReplicatedMap = context.nearCacheAdapter instanceof ReplicatedMapDataStructureAdapter;
        // the ReplicatedMap already uses keys by-reference
        return (serializeKeys && !isReplicatedMap) ? context.serializationService.toData(key) : key;
    }

    /**
     * Returns a value directly from the Near Cache.
     *
     * @param context the {@link NearCacheTestContext} to retrieve the Near Cache from
     * @param key     the key to get the value from
     * @param <NK>    the key type of the Near Cache
     * @param <NV>    the value type of the Near Cache
     * @return the value of the given key from the Near Cache
     */
    @SuppressWarnings("unchecked")
    public static <NK, NV> NV getValueFromNearCache(NearCacheTestContext<?, ?, NK, NV> context, Object key) {
        return context.nearCache.get((NK) key);
    }

    /**
     * Returns a {@link NearCacheRecord} directly from the Near Cache.
     *
     * @param context the {@link NearCacheTestContext} to retrieve the Near Cache from
     * @param key     the key to get the value from
     * @return the {@link NearCacheRecord} of the given key from the Near Cache or {@code null} if record store cannot be casted
     */
    @SuppressWarnings("unchecked")
    public static NearCacheRecord getRecordFromNearCache(NearCacheTestContext<?, ?, ?, ?> context, Object key) {
        DefaultNearCache nearCache = (DefaultNearCache) context.nearCache;
        NearCacheRecordStore nearCacheRecordStore = nearCache.getNearCacheRecordStore();
        return nearCacheRecordStore.getRecord(key);
    }

    /**
     * Checks if the {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy} of a {@link NearCacheConfig}
     * is {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE}.
     *
     * @param nearCacheConfig the {@link NearCacheConfig} to check
     * @return {@code true} if the {@code LocalUpdatePolicy} is {@code CACHE_ON_UPDATE}, {@code false} otherwise
     */
    static boolean isCacheOnUpdate(NearCacheConfig nearCacheConfig) {
        return nearCacheConfig != null && nearCacheConfig.getLocalUpdatePolicy() == CACHE_ON_UPDATE;
    }

    /**
     * Checks if the {@link NearCacheConfig#isInvalidateOnChange()} of a {@link NearCacheConfig} is {@code true}.
     *
     * @param nearCacheConfig the {@link NearCacheConfig} to check
     * @return {@code true} if the {@code LocalUpdatePolicy} is {@code CACHE_ON_UPDATE}, {@code false} otherwise
     */
    static boolean isInvalidateOnChange(NearCacheConfig nearCacheConfig) {
        return nearCacheConfig != null && nearCacheConfig.isInvalidateOnChange();
    }

    /**
     * Checks if the given {@link DataStructureAdapter} implements a specified {@link DataStructureMethods}.
     *
     * @param adapter the {@link DataStructureAdapter} to test
     * @param method  {@link DataStructureAdapterMethod} to search for
     */
    public static boolean isMethodAvailable(DataStructureAdapter adapter, DataStructureAdapterMethod method) {
        return new MethodAvailableMatcher(method).matchesSafely(adapter.getClass());
    }

    /**
     * Assumes that the given {@link NearCacheConfig} has
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#INVALIDATE} configured.
     *
     * @param nearCacheConfig the {@link NearCacheConfig} to check
     */
    public static void assumeThatLocalUpdatePolicyIsInvalidate(NearCacheConfig nearCacheConfig) {
        assumeFalse(isCacheOnUpdate(nearCacheConfig));
    }

    /**
     * Assumes that the given {@link NearCacheConfig} has
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} configured.
     *
     * @param nearCacheConfig the {@link NearCacheConfig} to check
     */
    public static void assumeThatLocalUpdatePolicyIsCacheOnUpdate(NearCacheConfig nearCacheConfig) {
        assumeTrue(isCacheOnUpdate(nearCacheConfig));
    }

    /**
     * Assumes that the given {@link DataStructureAdapter} implements a specified {@link DataStructureMethods}.
     *
     * @param adapterClass the {@link DataStructureAdapter} class to test
     * @param method       {@link DataStructureAdapterMethod} to search for
     */
    public static void assumeThatMethodIsAvailable(Class<? extends DataStructureAdapter> adapterClass,
                                                   DataStructureAdapterMethod method) {
        assumeThat(adapterClass, new MethodAvailableMatcher(method));
    }

    /**
     * Asserts the values of the Near Cache itself.
     *
     * @param context the {@link NearCacheTestContext} to retrieve the Near Cache from
     * @param size    the number of entries to check
     */
    public static void assertNearCacheContent(NearCacheTestContext<?, ?, ?, ?> context, int size) {
        InMemoryFormat inMemoryFormat = context.nearCacheConfig.getInMemoryFormat();
        for (int i = 0; i < size; i++) {
            Object nearCacheKey = getNearCacheKey(context, i);

            String value = context.serializationService.toObject(getValueFromNearCache(context, nearCacheKey));
            assertEquals("value-" + i, value);

            assertNearCacheRecord(getRecordFromNearCache(context, nearCacheKey), i, inMemoryFormat);
        }
    }

    /**
     * Asserts that the Near Cache contains the same reference than the given value, when in-memory-format is OBJECT.
     *
     * @param context the {@link NearCacheTestContext} to retrieve the Near Cache from
     * @param key     the key to check the value reference for
     * @param value   the value reference to compare with the Near Cache reference
     */
    public static void assertNearCacheReference(NearCacheTestContext<?, ?, ?, ?> context, int key, Object value) {
        if (context.nearCacheConfig.getInMemoryFormat() != InMemoryFormat.OBJECT) {
            return;
        }
        Object nearCacheKey = getNearCacheKey(context, key);
        Object nearCacheValue = getValueFromNearCache(context, nearCacheKey);
        assertSame(value, nearCacheValue);
    }

    /**
     * Asserts the state and class of a {@link NearCacheRecord} and its value.
     *
     * @param record         the {@link NearCacheRecord}
     * @param key            the key for the {@link NearCacheRecord}
     * @param inMemoryFormat the {@link InMemoryFormat} of the Near Cache
     */
    public static void assertNearCacheRecord(NearCacheRecord record, int key, InMemoryFormat inMemoryFormat) {
        assertNotNull(format("NearCacheRecord for key %d could not be found", key), record);
        assertEquals(format("RecordState of NearCacheRecord for key %d should be READ_PERMITTED (%s)", key, record),
                READ_PERMITTED, record.getRecordState());

        Class<? extends NearCacheRecord> recordClass = record.getClass();
        Class<?> recordValueClass = record.getValue().getClass();
        switch (inMemoryFormat) {
            case OBJECT:
                assertTrue(
                        format("NearCacheRecord for key %d should be a NearCacheObjectRecord, but was %s", key, recordClass),
                        NearCacheObjectRecord.class.isAssignableFrom(recordClass));
                assertFalse(
                        format("Value of NearCacheRecord for key %d should not be Data", key),
                        Data.class.isAssignableFrom(recordValueClass));
                break;
            case BINARY:
                assertTrue(
                        format("NearCacheRecord for key %d should be a NearCacheDataRecord, but was %s", key, recordClass),
                        NearCacheDataRecord.class.isAssignableFrom(recordClass));
                assertTrue(
                        format("Value of NearCacheRecord for key %d should be Data, but was %s", key, recordValueClass),
                        Data.class.isAssignableFrom(recordValueClass));
                break;
            case NATIVE:
                assertFalse(
                        format("NearCacheRecord for key %d should be a HDNearCacheRecord, but was NearCacheObjectRecord", key),
                        NearCacheObjectRecord.class.isAssignableFrom(recordClass));
                assertFalse(
                        format("NearCacheRecord for key %d should be a HDNearCacheRecord, but was NearCacheDataRecord", key),
                        NearCacheDataRecord.class.isAssignableFrom(recordClass));
                assertTrue(
                        format("Value of NearCacheRecord for key %d should be Data, but was %s", key, recordValueClass),
                        Data.class.isAssignableFrom(recordValueClass));
                break;
        }
    }

    /**
     * Waits until the {@link com.hazelcast.internal.adapter.DataStructureLoader} is finished.
     *
     * @param context the given {@link NearCacheTestContext} to retrieve the {@link DataStructureAdapter} from
     */
    public static void waitUntilLoaded(NearCacheTestContext<?, ?, ?, ?> context) {
        if (context.dataAdapter instanceof IMapDataStructureAdapter) {
            ((IMapDataStructureAdapter) context.dataAdapter).waitUntilLoaded();
        }
    }

    /**
     * Asserts the number of Near Cache invalidations.
     *
     * @param context          the given {@link NearCacheTestContext} to retrieve the {@link NearCacheInvalidationListener} from
     * @param minInvalidations lower bound of Near Cache invalidations to wait for
     * @param maxInvalidations upper bound of Near Cache invalidations to wait for
     */
    public static void assertNearCacheInvalidationsBetween(final NearCacheTestContext<?, ?, ?, ?> context,
                                                           final int minInvalidations, final int maxInvalidations) {
        if (context.nearCacheConfig.isInvalidateOnChange() && context.invalidationListener != null && minInvalidations > 0) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    long invalidationCount = context.invalidationListener.getInvalidationCount();
                    assertTrue(format("Expected between %d and %d Near Cache invalidations, but found %d (%s)",
                            minInvalidations, maxInvalidations, invalidationCount, context.stats),
                            minInvalidations <= invalidationCount && invalidationCount <= maxInvalidations);
                }
            });
            context.invalidationListener.resetInvalidationCount();
        }
    }

    /**
     * Asserts the number of Near Cache invalidations.
     *
     * @param context       the given {@link NearCacheTestContext} to retrieve the {@link NearCacheInvalidationListener} from
     * @param invalidations the given number of Near Cache invalidations to wait for
     */
    public static void assertNearCacheInvalidations(NearCacheTestContext<?, ?, ?, ?> context, int invalidations) {
        if (context.nearCacheConfig.isInvalidateOnChange() && context.invalidationListener != null && invalidations > 0) {
            assertNearCacheInvalidations(context.invalidationListener, invalidations, context.stats);
        }
    }

    /**
     * Asserts the number of Near Cache invalidations.
     *
     * @param listener      the given {@link NearCacheInvalidationListener}
     * @param invalidations the given number of Near Cache invalidations to wait for
     * @param stats         the given {@link NearCacheStats} for the assert message
     */
    public static void assertNearCacheInvalidations(final NearCacheInvalidationListener listener, final int invalidations,
                                                    final NearCacheStats stats) {
        if (listener != null && invalidations > 0) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEqualsFormat("Expected %d Near Cache invalidations, but found %d (%s)",
                            invalidations, listener.getInvalidationCount(), stats);
                }
            });
            listener.resetInvalidationCount();
        }
    }

    /**
     * Asserts the number of evicted entries of a {@link NearCache}.
     *
     * @param context       the {@link NearCacheTestContext} to retrieve the eviction count from
     * @param evictionCount the expected eviction count to wait for
     */
    public static void assertNearCacheEvictions(NearCacheTestContext<?, ?, ?, ?> context, int evictionCount) {
        long evictions = context.stats.getEvictions();
        assertTrue(format("Near Cache eviction count didn't reach the desired value (%d vs. %d) (%s)",
                evictions, evictionCount, context.stats),
                evictions >= evictionCount);
    }

    /**
     * Asserts the number of evicted entries of a {@link NearCache}.
     *
     * @param context       the {@link NearCacheTestContext} to retrieve the eviction count from
     * @param evictionCount the expected eviction count to wait for
     */
    public static void assertNearCacheEvictionsEventually(final NearCacheTestContext<?, ?, ?, ?> context,
                                                          final int evictionCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNearCacheEvictions(context, evictionCount);
            }
        });
    }

    /**
     * Asserts the size of a {@link NearCache}.
     *
     * @param context      the {@link NearCacheTestContext} to retrieve the stats from
     * @param expectedSize the expected size of the Near Cache
     * @param messages     an optional assert message
     */
    public static void assertNearCacheSize(NearCacheTestContext<?, ?, ?, ?> context, long expectedSize, String... messages) {
        String message = messages.length > 0 ? messages[0] + " " : "";

        long nearCacheSize = context.nearCache.size();
        assertEquals(format("%sNear Cache size didn't reach the desired value (%d vs. %d) (%s)",
                message, expectedSize, nearCacheSize, context.stats), expectedSize, nearCacheSize);

        long ownedEntryCount = context.stats.getOwnedEntryCount();
        assertEquals(format("%sNear Cache owned entry count didn't reach the desired value (%d vs. %d) (%s)",
                message, expectedSize, ownedEntryCount, context.stats), expectedSize, ownedEntryCount);
    }

    /**
     * Asserts the size of a {@link NearCache}.
     *
     * @param context      the {@link NearCacheTestContext} to retrieve the owned entry count from
     * @param expectedSize the expected size of the of the Near Cache
     * @param messages     an optional assert message
     */
    public static void assertNearCacheSizeEventually(final NearCacheTestContext<?, ?, ?, ?> context, final int expectedSize,
                                                     final String... messages) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNearCacheSize(context, expectedSize, messages);
            }
        });
    }

    /**
     * Asserts the {@link NearCacheStats} for expected values.
     *
     * @param context                 the {@link NearCacheTestContext} to retrieve the stats from
     * @param expectedOwnedEntryCount the expected owned entry count
     * @param expectedHits            the expected Near Cache hits
     * @param expectedMisses          the expected Near Cache misses
     */
    public static void assertNearCacheStats(NearCacheTestContext<?, ?, ?, ?> context,
                                            long expectedOwnedEntryCount, long expectedHits, long expectedMisses) {
        assertNearCacheStats(context, expectedOwnedEntryCount, expectedHits, expectedMisses, 0, 0);
    }

    /**
     * Asserts the {@link NearCacheStats} for expected values.
     *
     * @param context                 the {@link NearCacheTestContext} to retrieve the stats from
     * @param expectedOwnedEntryCount the expected owned entry count
     * @param expectedHits            the expected Near Cache hits
     * @param expectedMisses          the expected Near Cache misses
     * @param expectedEvictions       the expected Near Cache evictions
     * @param expectedExpirations     the expected Near Cache expirations
     */
    public static void assertNearCacheStats(NearCacheTestContext<?, ?, ?, ?> context,
                                            long expectedOwnedEntryCount, long expectedHits, long expectedMisses,
                                            long expectedEvictions, long expectedExpirations) {
        NearCacheStats stats = new NearCacheStatsImpl(context.stats);

        assertEqualsFormat("Near Cache entry count should be %d, but was %d (%s)",
                expectedOwnedEntryCount, stats.getOwnedEntryCount(), stats);
        assertEqualsFormat("Near Cache hits should be %d, but were %d (%s)",
                expectedHits, stats.getHits(), stats);
        assertEqualsFormat("Near Cache misses should be %d, but were %d (%s)",
                expectedMisses, stats.getMisses(), stats);
        assertEqualsFormat("Near Cache evictions should be %d, but were %d (%s)",
                expectedEvictions, stats.getEvictions(), stats);
        assertEqualsFormat("Near Cache expirations should be %d, but were %d (%s)",
                expectedExpirations, stats.getExpirations(), stats);
    }

    public static void assertThatMemoryCostsAreGreaterThanZero(NearCacheTestContext<?, ?, ?, ?> context,
                                                               InMemoryFormat inMemoryFormat) {
        // the heap costs are just calculated if there is local data which is not in OBJECT in-memory-format
        if (context.hasLocalData && inMemoryFormat != OBJECT) {
            long ownedEntryMemoryCost = context.stats.getOwnedEntryMemoryCost();
            assertTrue(format("Expected owned entry memory costs, but found none (%s)", context.stats), ownedEntryMemoryCost > 0);

            boolean hasLocalMapStats = isMethodAvailable(context.nearCacheAdapter, DataStructureMethods.GET_LOCAL_MAP_STATS);
            if (hasLocalMapStats && inMemoryFormat == BINARY) {
                long heapCost = context.nearCacheAdapter.getLocalMapStats().getHeapCost();
                assertTrue("Expected heap costs in the LocalMapStats, but found none", heapCost > 0);
            }
        }
    }

    public static void assertThatMemoryCostsAreZero(NearCacheTestContext<?, ?, ?, ?> context) {
        // these asserts will work in all scenarios, since the default value should be 0 if no costs are calculated
        long ownedEntryMemoryCost = context.stats.getOwnedEntryMemoryCost();
        assertEqualsFormat("Expected %d owned entry memory costs, but found %d (%s)", 0, ownedEntryMemoryCost, context.stats);

        boolean hasLocalMapStats = isMethodAvailable(context.nearCacheAdapter, DataStructureMethods.GET_LOCAL_MAP_STATS);
        if (hasLocalMapStats) {
            long heapCost = context.nearCacheAdapter.getLocalMapStats().getHeapCost();
            assertEquals(format("Expected no heap costs in the LocalMapStats, but found %d", heapCost), 0, heapCost);
        }
    }

    private static void assertEqualsFormat(String message, long expected, long actual, NearCacheStats stats) {
        assertEquals(format(message, expected, actual, stats), expected, actual);
    }
}
