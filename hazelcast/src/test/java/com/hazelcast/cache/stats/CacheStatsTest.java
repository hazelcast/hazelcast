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

package com.hazelcast.cache.stats;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheStatsTest extends CacheTestSupport {

    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Override
    protected void onSetup() {
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return factory.newHazelcastInstance(createConfig());
    }

    protected HazelcastInstance getHazelcastInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }

    @Test
    public void testGettingStatistics() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertNotNull(stats);
    }

    @Test
    public void testStatisticsDisabled() {
        CacheConfig cacheConfig = createCacheConfig();
        cacheConfig.setStatisticsEnabled(false);
        ICache<Integer, String> cache = createCache(cacheConfig);
        CacheStatistics stats = cache.getLocalCacheStatistics();

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
    public void testPutStat_whenPutAsync() throws Exception {
        ICache<Integer, String> cache = createCache();
        final CacheStatistics stats = cache.getLocalCacheStatistics();

        final long ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.putAsync(i, "Value-" + i).toCompletableFuture().get();
        }

        assertEqualsEventually(new Callable<Long>() {
            @Override
            public Long call()
                    throws Exception {
                return stats.getCachePuts();
            }
        }, ENTRY_COUNT);
    }

    @Test
    public void testPutStat_whenPutIfAbsent_andKeysDoNotExist() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.putIfAbsent(i, "Value-" + i);
        }

        assertEquals(ENTRY_COUNT, stats.getCachePuts());
    }

    @Test
    public void testPutStat_whenPutIfAbsent_andKeysExist() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.putIfAbsent(i, "NewValue-" + i);
        }

        assertEquals(ENTRY_COUNT, stats.getCachePuts());
    }

    @Test
    public void testAveragePutTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        long startNanos = Timer.nanos();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }
        float avgPutTimeMicros = Timer.microsElapsed(startNanos);

        assertTrue(stats.getAveragePutTime() > 0);
        assertTrue(stats.getAveragePutTime() < avgPutTimeMicros);
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
    public void testGetStat_whenGetAsync() throws Exception {
        ICache<Integer, String> cache = createCache();
        final CacheStatistics stats = cache.getLocalCacheStatistics();

        final long ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
            cache.getAsync(i).toCompletableFuture().get();
        }
        assertEqualsEventually(new Callable<Long>() {
            @Override
            public Long call()
                    throws Exception {
                return stats.getCacheGets();
            }
        }, 2 * ENTRY_COUNT);
    }

    @Test
    public void testAverageGetTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        long startNanos = Timer.nanos();
        for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
            cache.get(i);
        }
        float avgGetTimeMicros = Timer.microsElapsed(startNanos);

        assertTrue(stats.getAverageGetTime() > 0);
        assertTrue(stats.getAverageGetTime() < avgGetTimeMicros);
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
    public void testRemoveStat_whenRemoveAsync() throws Exception {
        ICache<Integer, String> cache = createCache();
        final CacheStatistics stats = cache.getLocalCacheStatistics();

        final long ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
            cache.removeAsync(i).toCompletableFuture().get();
        }

        assertEqualsEventually(new Callable<Long>() {
            @Override
            public Long call()
                    throws Exception {
                return stats.getCacheRemovals();
            }
        }, ENTRY_COUNT);
    }

    @Test
    public void testAverageRemoveTimeStat() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        long startNanos = Timer.nanos();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.remove(i);
        }
        float avgRemoveTimeMicros = Timer.microsElapsed(startNanos);

        assertTrue(stats.getAverageRemoveTime() > 0);
        assertTrue(stats.getAverageRemoveTime() < avgRemoveTimeMicros);

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
    public void testHitStat_whenGetAsync() throws Exception {
        ICache<Integer, String> cache = createCache();
        final CacheStatistics stats = cache.getLocalCacheStatistics();

        final long ENTRY_COUNT = 100;
        final long GET_COUNT = 3 * ENTRY_COUNT;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < GET_COUNT; i++) {
            cache.getAsync(i).toCompletableFuture().get();
        }

        assertEqualsEventually(new Callable<Long>() {
            @Override
            public Long call()
                    throws Exception {
                return stats.getCacheHits();
            }
        }, ENTRY_COUNT);
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

    @Test
    public void testMissStat_whenGetAsync() throws Exception {
        ICache<Integer, String> cache = createCache();
        final CacheStatistics stats = cache.getLocalCacheStatistics();

        final long ENTRY_COUNT = 100;
        final long GET_COUNT = 3 * ENTRY_COUNT;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < GET_COUNT; i++) {
            cache.getAsync(i).toCompletableFuture().get();
        }

        assertEqualsEventually(new Callable<Long>() {
            @Override
            public Long call()
                    throws Exception {
                return stats.getCacheMisses();
            }
        }, GET_COUNT - ENTRY_COUNT);
    }

    @Test
    public void testMissStat_whenPutIfAbsent_andKeysDoNotExist() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.putIfAbsent(i, "Value-" + i);
        }

        assertEquals(ENTRY_COUNT, stats.getCacheMisses());
    }

    @Test
    public void testMissStat_whenPutIfAbsent_andKeysAlreadyExist() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        final int ENTRY_COUNT = 100;

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, "Value-" + i);
        }

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.putIfAbsent(i, "NewValue-" + i);
        }

        assertEquals(0, stats.getCacheMisses());
    }

    @Test
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
        long start;
        long end;

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
        long start;
        long end;

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
            waitAllForSafeState(factory.getAllHazelcastInstances());
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
            waitAllForSafeState(factory.getAllHazelcastInstances());
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
    public void testExpirations() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        // TODO Produce a case that triggers expirations and verify the expiration count
        // At the moment, we are just verifying that this stats is supported
        stats.getCacheEvictions();
    }

    @Test
    public void testEvictions() {
        int partitionCount = 2;
        int maxEntryCount = 2;
        // with given parameters, the eviction checker expects 6 entries per partition to kick-in
        // see EntryCountCacheEvictionChecker#calculateMaxPartitionSize for actual calculation
        int calculatedMaxEntriesPerPartition = 6;
        factory.terminateAll();
        // configure members with 2 partitions, cache with eviction on max size 2
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName("*")
                .setBackupCount(1)
                .setStatisticsEnabled(true)
                .setEvictionConfig(
                        new EvictionConfig().setSize(maxEntryCount)
                                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                                .setEvictionPolicy(EvictionPolicy.LFU)
                );

        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), Integer.toString(partitionCount));
        config.addCacheConfig(cacheConfig);

        HazelcastInstance hz1 = getHazelcastInstance(config);
        HazelcastInstance hz2 = getHazelcastInstance(config);

        ICache<String, String> cache1 = hz1.getCacheManager().getCache("cache1");
        ICache<String, String> cache2 = hz2.getCacheManager().getCache("cache1");

        // put 5 entries in a single partition
        while (cache1.size() < calculatedMaxEntriesPerPartition) {
            String key = generateKeyForPartition(hz1, 0);
            cache1.put(key, randomString());
        }
        String key = generateKeyForPartition(hz1, 0);
        // this put must trigger eviction
        cache1.put(key, "foo");

        try {
            // number of evictions on primary and backup must be 1
            assertEquals(1, cache1.getLocalCacheStatistics().getCacheEvictions()
                    + cache2.getLocalCacheStatistics().getCacheEvictions());
        } finally {
            cache1.destroy();
            cache2.destroy();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNearCacheStats_availableWhenEnabled() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        stats.getNearCacheStatistics();
    }
}
