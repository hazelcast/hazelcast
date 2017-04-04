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
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.internal.adapter.DataStructureAdapterMethod;
import com.hazelcast.internal.adapter.MethodAvailableMatcher;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
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
     * @return the {@link NearCacheConfig}
     */
    public static NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(AbstractNearCacheBasicTest.DEFAULT_NEAR_CACHE_NAME + "*")
                .setInMemoryFormat(inMemoryFormat)
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
     * Returns the {@link MapNearCacheManager} from a given {@link HazelcastInstance}.
     *
     * @param instance the {@link HazelcastInstance} to retrieve the {@link MapNearCacheManager} frp,
     * @return the {@link MapNearCacheManager}
     */
    public static MapNearCacheManager getMapNearCacheManager(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNode(instance).nodeEngine;
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);

        return service.getMapServiceContext().getMapNearCacheManager();
    }

    /**
     * Assumes that the given {@link NearCacheConfig} has
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#INVALIDATE} configured.
     *
     * @param nearCacheConfig the {@link NearCacheConfig} to test
     */
    public static void assumeThatLocalUpdatePolicyIsInvalidate(NearCacheConfig nearCacheConfig) {
        assumeFalse(isCacheOnUpdate(nearCacheConfig));
    }

    /**
     * Assumes that the given {@link NearCacheConfig} has
     * {@link com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy#CACHE_ON_UPDATE} configured.
     *
     * @param nearCacheConfig the {@link NearCacheConfig} to test
     */
    public static void assumeThatLocalUpdatePolicyIsCacheOnUpdate(NearCacheConfig nearCacheConfig) {
        assumeTrue(isCacheOnUpdate(nearCacheConfig));
    }

    /**
     * Assumes that the given {@link DataStructureAdapter} implements a specified {@link DataStructureMethods}.
     *
     * @param adapter the {@link DataStructureAdapter} to test
     * @param method  {@link DataStructureAdapterMethod} to search for
     */
    public static void assumeThatMethodIsAvailable(DataStructureAdapter adapter, DataStructureAdapterMethod method) {
        assumeThat(adapter, new MethodAvailableMatcher(method));
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
        NearCacheStats stats = context.stats;

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

    private static void assertEqualsFormat(String message, long expected, long actual, NearCacheStats stats) {
        assertEquals(format(message, expected, actual, stats), expected, actual);
    }
}
