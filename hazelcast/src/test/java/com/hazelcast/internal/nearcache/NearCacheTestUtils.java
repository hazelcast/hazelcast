package com.hazelcast.internal.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Provides utility methods for unified Near Cache tests.
 */
public final class NearCacheTestUtils extends HazelcastTestSupport {

    private NearCacheTestUtils() {
    }

    /**
     * Creates a {@link NearCacheConfig} with a given {@link InMemoryFormat}.
     *
     * @param inMemoryFormat the {@link InMemoryFormat} to set
     * @return the {@link NearCacheConfig}
     */
    public static NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(AbstractBasicNearCacheTest.DEFAULT_NEAR_CACHE_NAME + "*")
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
     * Waits until a Near Cache has a given owned entry count.
     *
     * @param context       the {@link NearCacheTestContext} to retrieve the owned entry count from
     * @param nearCacheSize the expected owned entry count
     */
    public static void waitForNearCacheSize(final NearCacheTestContext<Integer, String, ?, ?> context,
                                            final int nearCacheSize) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                long ownedEntryCount = context.stats.getOwnedEntryCount();
                assertTrue(format("Near Cache owned entry count didn't reach the desired value (%d vs. %d) (%s)",
                        ownedEntryCount, nearCacheSize, context.stats),
                        ownedEntryCount == nearCacheSize);
            }
        });
    }

    /**
     * Waits until a given number of entries are evicted from a Near Cache.
     *
     * @param context       the {@link NearCacheTestContext} to retrieve the eviction count from
     * @param evictionCount the expected eviction count to wait for
     */
    public static void waitForNearCacheEvictions(final NearCacheTestContext<Integer, String, ?, ?> context,
                                                 final int evictionCount) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                long evictions = context.stats.getEvictions();
                assertTrue(format("Near Cache eviction count didn't reach the desired value (%d vs. %d) (%s)",
                        evictions, evictionCount, context.stats),
                        evictions >= evictionCount);
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
    public static void assertNearCacheStats(NearCacheTestContext<Integer, String, ?, ?> context,
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
    public static void assertNearCacheStats(NearCacheTestContext<Integer, String, ?, ?> context,
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
