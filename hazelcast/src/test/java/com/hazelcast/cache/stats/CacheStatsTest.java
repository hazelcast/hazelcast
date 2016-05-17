package com.hazelcast.cache.stats;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class CacheStatsTest extends CacheTestSupport {

    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Override
    protected void onSetup() {
    }

    @Override
    protected void onTearDown() {
        factory.terminateAll();
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return factory.newHazelcastInstance(createConfig());
    }

    @Test
    public void testGettingStatistics() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertNotNull(stats);
    }

    @Test
    public void testStatisticsDisabled() {
        long now = System.currentTimeMillis();

        CacheConfig cacheConfig = createCacheConfig();
        cacheConfig.setStatisticsEnabled(false);
        ICache<Integer, String> cache = createCache(cacheConfig);
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertTrue(stats.getCreationTime() >= now);

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.get(i);
        }

        for (int i = 0; i < 10; i++) {
            cache.remove(i);
        }

        assertEquals(0, stats.getCacheHits());
        assertEquals(0, stats.getCacheHitPercentage(), 0.0f);

        assertEquals(0, stats.getCacheMisses());
        assertEquals(0, stats.getCacheMissPercentage(), 0.0f);

        assertEquals(0, stats.getCacheGets());
        assertEquals(0, stats.getAverageGetTime(), 0.0f);

        assertEquals(0, stats.getCachePuts());
        assertEquals(0, stats.getAveragePutTime(), 0.0f);

        assertEquals(0, stats.getCacheRemovals());
        assertEquals(0, stats.getAverageRemoveTime(), 0.0f);

        assertEquals(0, stats.getLastAccessTime());
        assertEquals(0, stats.getLastUpdateTime());
    }

    @Test
    public void testCreationTime() {
        long now = System.currentTimeMillis();

        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertTrue(stats.getCreationTime() >= now);
    }

    @Test
    public void testPutStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        assertEquals(ENTRY_COUNT, stats.getCachePuts());
    }

    @Test
    public void testAveragePutTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        long start = System.nanoTime();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }
        long end = System.nanoTime();

        float avgPutTime = (end - start) / 1000;

        assertTrue(stats.getAveragePutTime() > 0);
        assertTrue(stats.getAveragePutTime() < avgPutTime);
    }

    @Test
    public void testGetStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
            cache.get(i);
        }

        assertEquals(2 * ENTRY_COUNT, stats.getCacheGets());
    }

    @Test
    public void testAverageGetTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        long start = System.nanoTime();
        for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
            cache.get(i);
        }
        long end = System.nanoTime();

        float avgGetTime = (end - start) / 1000;

        assertTrue(stats.getAverageGetTime() > 0);
        assertTrue(stats.getAverageGetTime() < avgGetTime);
    }

    @Test
    public void testRemoveStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
            cache.remove(i);
        }

        assertEquals(ENTRY_COUNT, stats.getCacheRemovals());
    }

    @Test
    public void testAverageRemoveTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        long start = System.nanoTime();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.remove(i);
        }
        long end = System.nanoTime();

        float avgRemoveTime = (end - start) / 1000;

        assertTrue(stats.getAverageRemoveTime() > 0);
        assertTrue(stats.getAverageRemoveTime() < avgRemoveTime);

        float currentAverageRemoveTime = stats.getAverageRemoveTime();
        sleepAtLeastMillis(1);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.remove(i);
        }

        // Latest removes has no effect since keys are already removed at previous loop
        assertEquals(currentAverageRemoveTime, stats.getAverageRemoveTime(), 0.0f);
    }

    @Test
    public void testHitStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;
        final int GET_COUNT = 3 * ENTRY_COUNT;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < GET_COUNT; i++) {
            cache.get(i);
        }

        assertEquals(ENTRY_COUNT, stats.getCacheHits());
    }

    @Test
    public void testHitPercentageStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;
        final int GET_COUNT = 3 * ENTRY_COUNT;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < GET_COUNT; i++) {
            cache.get(i);
        }

        float expectedHitPercentage = (float) ENTRY_COUNT / GET_COUNT * 100.0f;
        assertEquals(expectedHitPercentage, stats.getCacheHitPercentage(), 0.0f);
    }

    @Test
    public void testMissStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;
        final int GET_COUNT = 3 * ENTRY_COUNT;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < GET_COUNT; i++) {
            cache.get(i);
        }

        assertEquals(GET_COUNT - ENTRY_COUNT, stats.getCacheMisses());
    }

    public void testMissPercentageStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;
        final int GET_COUNT = 3 * ENTRY_COUNT;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < GET_COUNT; i++) {
            cache.get(i);
        }

        float expectedMissPercentage = (float) (GET_COUNT - ENTRY_COUNT) / GET_COUNT * 100.0f;
        assertEquals(expectedMissPercentage, stats.getCacheMissPercentage(), 0.0f);
    }

    @Test
    public void testLastAccessTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;
        long start, end;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        assertEquals(0, stats.getLastAccessTime());

        start = System.currentTimeMillis();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.get(i);
        }
        end = System.currentTimeMillis();

        // Hits effect last access time
        assertTrue(stats.getLastAccessTime() >= start);
        assertTrue(stats.getLastAccessTime() <= end);

        long currentLastAccessTime = stats.getLastAccessTime();
        sleepAtLeastMillis(1);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.remove(i);
        }

        // Removes has no effect on last access time
        assertEquals(currentLastAccessTime, stats.getLastAccessTime());

        sleepAtLeastMillis(1);
        start = System.currentTimeMillis();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.get(i);
        }
        end = System.currentTimeMillis();

        // Misses also effect last access time
        assertTrue(stats.getLastAccessTime() >= start);
        assertTrue(stats.getLastAccessTime() <= end);
    }

    @Test
    public void testLastUpdateTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;
        long start, end;

        start = System.currentTimeMillis();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }
        end = System.currentTimeMillis();

        assertTrue(stats.getLastUpdateTime() >= start);
        assertTrue(stats.getLastUpdateTime() <= end);

        start = System.currentTimeMillis();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.remove(i);
        }
        end = System.currentTimeMillis();

        assertTrue(stats.getLastUpdateTime() >= start);
        assertTrue(stats.getLastUpdateTime() <= end);

        long currentLastUpdateTime = stats.getLastUpdateTime();
        sleepAtLeastMillis(1);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.remove(i);
        }

        // Latest removes has no effect since keys are already removed at previous loop
        assertEquals(currentLastUpdateTime, stats.getLastUpdateTime());
    }

    @Test
    public void testOwnedEntryCountWhenThereIsNoBackup() {
        doTestForOwnedEntryCount(false, false);
    }

    @Test
    public void testOwnedEntryCountWhenThereAreBackupsOnStaticCluster() {
        doTestForOwnedEntryCount(true, false);
    }

    @Test
    public void testOwnedEntryCountWhenThereAreBackupsOnDynamicCluster() {
        doTestForOwnedEntryCount(true, true);
    }

    private void doTestForOwnedEntryCount(boolean useBackups, boolean triggerMigration) {
        final String cacheName = randomName();

        ICache<Integer, String> cache;
        CacheStatistics[] allStats;
        HazelcastInstance instance2 = null;

        if (useBackups) {
            // Create the second instance to store data as backup.
            instance2 = getHazelcastInstance();
            CachingProvider cp = getCachingProvider(instance2);
            CacheManager cm = cp.getCacheManager();
            cache = createCache(cacheName);
            ICache<Integer, String> c = cm.getCache(cacheName).unwrap(ICache.class);
            allStats = new CacheStatistics[]{cache.getLocalCacheStatistics(), c.getLocalCacheStatistics()};
        } else {
            cache = createCache(cacheName);
            allStats = new CacheStatistics[]{cache.getLocalCacheStatistics()};
        }

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        assertOwnedEntryCount(ENTRY_COUNT, allStats);

        if (triggerMigration && instance2 != null) {
            // Shutdown the second instance to trigger migration so first instance will be owner of all partitions.
            instance2.shutdown();
            allStats = new CacheStatistics[]{cache.getLocalCacheStatistics()};

            assertOwnedEntryCount(ENTRY_COUNT, allStats);
        }

        for (int i = 0; i < 10; i++) {
            cache.remove(i);
        }

        assertOwnedEntryCount(ENTRY_COUNT - 10, allStats);

        for (int i = 10; i < ENTRY_COUNT; i++) {
            cache.remove(i);
        }

        assertOwnedEntryCount(0, allStats);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        assertOwnedEntryCount(ENTRY_COUNT, allStats);

        if (triggerMigration) {
            // Create the second instance to trigger migration
            // so the second instance will be owner of some partitions
            // and the first instance will lose ownership of some instances.
            instance2 = getHazelcastInstance();
            CachingProvider cp = getCachingProvider(instance2);
            CacheManager cm = cp.getCacheManager();
            ICache<Integer, String> c = cm.getCache(cacheName).unwrap(ICache.class);
            allStats = new CacheStatistics[]{cache.getLocalCacheStatistics(), c.getLocalCacheStatistics()};

            assertOwnedEntryCount(ENTRY_COUNT, allStats);
        }

        cache.clear();

        assertOwnedEntryCount(0, allStats);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        assertOwnedEntryCount(ENTRY_COUNT, allStats);

        cache.destroy();

        assertOwnedEntryCount(0, allStats);
    }

    private long getOwnedEntryCount(CacheStatistics... statsList) {
        long ownedEntryCount = 0;
        for (CacheStatistics stats : statsList) {
            ownedEntryCount += stats.getOwnedEntryCount();
        }
        return ownedEntryCount;
    }

    private void assertOwnedEntryCount(final int expectedEntryCount, final CacheStatistics... statsList) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedEntryCount, getOwnedEntryCount(statsList));
            }
        });
    }

    @Test
    public void testExpiries() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        // TODO Produce a case that triggers expirations and verify the expiration count
        // At the moment, we are just verifying that this stats is supported
        stats.getCacheEvictions();
    }

    @Test
    public void testEvictions() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        // TODO Produce a case that triggers evictions and verify the eviction count
        // At the moment, we are just verifying that this stats is supported
        stats.getCacheEvictions();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNearCacheStatsWhenNearCacheDisabled() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        stats.getNearCacheStatistics();
    }

}
