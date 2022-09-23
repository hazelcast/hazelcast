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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordReaderWriter;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpirationTimeTest extends HazelcastTestSupport {

    private static final long ONE_MINUTE_IN_MILLIS = MINUTES.toMillis(1);

    @Parameterized.Parameter
    public boolean statisticsEnabled;
    @Parameterized.Parameter(1)
    public boolean perEntryStatsEnabled;

    @Parameterized.Parameters(name = "statisticsEnabled:{0}, perEntryStatsEnabled:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, true},
                {false, true},
                {true, false},
                {false, false},
        });
    }

    @Test
    public void expire_based_on_ttl_when_when_ttl_is_smaller_than_max_idle() {
        IMap<Integer, Integer> map = createMap();
        final long ttlSeconds = 5;
        final long maxIdleSeconds = 10;

        map.put(1, 1, ttlSeconds, SECONDS, maxIdleSeconds, SECONDS);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        // access
        sleepSeconds(1);
        map.get(1);

        long expirationTimeAfterGet = getExpirationTime(map, 1);

        assertEquals(expirationTimeAfterGet, expirationTimeAfterPut);
    }

    @Test
    public void ttl_limits_expiration_time_increase_when_max_idle_is_smaller_than_ttl() {
        IMap<Integer, Integer> map = createMap();
        final long ttlSeconds = 12;
        final long maxIdleSeconds = 10;

        map.put(1, 1, ttlSeconds, SECONDS, maxIdleSeconds, SECONDS);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        // access
        for (int i = 0; i < 2; i++) {
            sleepSeconds(2);
            map.get(1);
        }

        long expirationTimeAfterGet = getExpirationTime(map, 1);

        long diff = expirationTimeAfterGet - expirationTimeAfterPut;
        assertEquals(ttlSeconds - maxIdleSeconds, MILLISECONDS.toSeconds(diff));
    }

    @Test
    public void last_update_time_as_version_when_both_ttl_and_max_idle_defined() {
        assumeThat(perEntryStatsEnabled, is(false));

        final long ttlSeconds = 12;
        final long maxIdleSeconds = 10;
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, ttlSeconds, SECONDS, maxIdleSeconds, SECONDS);
        long version1 = map.getEntryView(1).getVersion();

        map.put(1, 1, ttlSeconds, SECONDS, maxIdleSeconds, SECONDS);
        long version2 = map.getEntryView(1).getVersion();

        assertTrue(version2 > version1);
    }

    @Test
    public void put_without_ttl_after_put_with_ttl_cancels_previous_ttl() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);
        long ttlAfter1stPut = map.getEntryView(1).getTtl();

        map.put(1, 2);
        long ttlAfter2ndPut = map.getEntryView(1).getTtl();

        assertEquals(ONE_MINUTE_IN_MILLIS, ttlAfter1stPut);
        assertEquals(Long.MAX_VALUE, ttlAfter2ndPut);
    }

    @Test
    public void put_without_ttl_after_put_with_ttl_gets_next_ttl_value_from_map_config() {
        int ttlSeconds = 100;
        String mapName = randomMapName();
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(inMemoryFormat());
        mapConfig.setTimeToLiveSeconds(ttlSeconds);
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(mapName);

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);
        long ttlAfter1stPut = map.getEntryView(1).getTtl();

        map.put(1, 2);
        long ttlAfter2ndPut = map.getEntryView(1).getTtl();

        assertEquals(ONE_MINUTE_IN_MILLIS, ttlAfter1stPut);
        assertEquals(SECONDS.toMillis(ttlSeconds), ttlAfter2ndPut);
    }

    @Test
    public void put_without_maxIdle_after_put_with_maxIdle_cancels_previous_maxIdle() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, -1, MILLISECONDS, ONE_MINUTE_IN_MILLIS, MILLISECONDS);
        long maxIdleAfter1stPut = map.getEntryView(1).getMaxIdle();

        map.put(1, 2);
        long maxIdleAfter2ndPut = map.getEntryView(1).getMaxIdle();

        assertEquals(ONE_MINUTE_IN_MILLIS, maxIdleAfter1stPut);
        assertEquals(Long.MAX_VALUE, maxIdleAfter2ndPut);
    }

    @Test
    public void put_without_maxIdle_after_put_with_maxIdle_gets_next_maxIdle_value_from_map_config() {
        int maxIdleSeconds = 100;
        String mapName = randomMapName();
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(inMemoryFormat());
        mapConfig.setMaxIdleSeconds(maxIdleSeconds);
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(mapName);

        map.put(1, 1, -1, MILLISECONDS, ONE_MINUTE_IN_MILLIS, MILLISECONDS);
        long maxIdleAfter1stPut = map.getEntryView(1).getMaxIdle();

        map.put(1, 2);
        long maxIdleAfter2ndPut = map.getEntryView(1).getMaxIdle();

        assertEquals(ONE_MINUTE_IN_MILLIS, maxIdleAfter1stPut);
        assertEquals(SECONDS.toMillis(maxIdleSeconds), maxIdleAfter2ndPut);
    }

    @Test
    public void testExpirationTime_withTTL() {
        assumeThat(perEntryStatsEnabled, is(true));

        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + ONE_MINUTE_IN_MILLIS;
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void test_replicated_entries_view_equal_after_cluster_scale_up() {
        Config config = getConfig();
        String mapName = "default";
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat());

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map = node1.getMap(mapName);
        int keyCount = 10;
        for (int i = 0; i < keyCount; i++) {
            map.put(i, i, 111, SECONDS, 222, SECONDS);
            map.get(i);
            map.put(i, i, 112, SECONDS, 223, SECONDS);
        }

        Map<Integer, EntryView> entryViewsBefore = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            entryViewsBefore.put(i, map.getEntryView(i));
        }

        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map2 = node2.getMap(mapName);
        Map<Integer, EntryView> entryViewsAfter = new HashMap<>();
        for (int i = 0; i < keyCount; i++) {
            entryViewsAfter.put(i, map2.getEntryView(i));
        }

        assertEquals(entryViewsBefore, entryViewsAfter);
    }

    @Test
    public void testExpirationTime_withTTL_withShorterMaxIdle() {
        assumeThat(perEntryStatsEnabled, is(true));

        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS, 10, SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + SECONDS.toMillis(10);
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_withShorterTTL_andMaxIdle() {
        assumeThat(perEntryStatsEnabled, is(true));

        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 10, SECONDS, 20, SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + SECONDS.toMillis(10);
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }


    @Test
    public void testExpirationTime_withZeroTTL() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 0, MILLISECONDS);

        assertEquals(Long.MAX_VALUE, getExpirationTime(map, 1));
    }

    @Test
    public void testExpirationTime_withNegativeTTL() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, -1, MILLISECONDS);

        assertEquals(Long.MAX_VALUE, getExpirationTime(map, 1));
    }

    @Test
    public void testExpirationTime_withTTL_andMapConfigTTL() {
        IMap<Integer, Integer> map = createMapWithTTLSeconds(5553152);

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + ONE_MINUTE_IN_MILLIS;
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_withZeroTTL_andMapConfigTTL() {
        IMap<Integer, Integer> map = createMapWithTTLSeconds((int) TimeUnit.MINUTES.toSeconds(1));

        map.put(1, 1, 0, MILLISECONDS);

        assertEquals(Long.MAX_VALUE, getExpirationTime(map, 1));
    }

    @Test
    public void testExpirationTime_withNegativeTTL_andMapConfigTTL() {
        IMap<Integer, Integer> map = createMapWithTTLSeconds((int) TimeUnit.MINUTES.toSeconds(1));

        map.put(1, 1, -1, MILLISECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + ONE_MINUTE_IN_MILLIS;
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_withTTL_afterMultipleUpdates() {
        assumeThat(perEntryStatsEnabled, is(true));

        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);
        sleepMillis(1);

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);
        sleepMillis(1);

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long lastUpdateTime = entryView.getLastUpdateTime();

        long expectedExpirationTime = lastUpdateTime + ONE_MINUTE_IN_MILLIS;
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_withMaxIdleTime() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();
        long expirationTime = entryView.getExpirationTime();

        long expectedExpirationTime = creationTime + TimeUnit.SECONDS.toMillis(10);
        assertEquals(expectedExpirationTime, expirationTime);
    }

    @Test
    public void testExpirationTime_withMaxIdleTime_withEntryCustomMaxIdle() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(20);

        map.put(1, 1, -1, MILLISECONDS, 10, SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();
        long expirationTime = entryView.getExpirationTime();

        long expectedExpirationTime = creationTime + TimeUnit.SECONDS.toMillis(10);
        assertEquals(expectedExpirationTime, expirationTime);
    }

    @Test
    public void testExpirationTime_withMaxIdleTime_withEntryCustomMaxIdleGreaterThanConfig() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1, -1, MILLISECONDS, 2, MINUTES);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();
        long expirationTime = entryView.getExpirationTime();

        long expectedExpirationTime = creationTime + TimeUnit.MINUTES.toMillis(2);
        assertEquals(expectedExpirationTime, expirationTime);
    }

    @Test
    public void testExpirationTime_withNegativeMaxIdleTime() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1, -1, MILLISECONDS, -1, MILLISECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();
        long expirationTime = entryView.getExpirationTime();

        // negative max idle means use value from map config
        long expectedExpirationTime = creationTime + TimeUnit.SECONDS.toMillis(10);
        assertEquals(expectedExpirationTime, expirationTime);
    }

    @Test
    public void testExpirationTime_withMaxIdleTime_afterMultipleAccesses() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1);
        sleepMillis(999);

        map.get(1);
        sleepMillis(23);

        assertTrue(map.containsKey(1));

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long lastAccessTime = entryView.getLastAccessTime();
        long expectedExpirationTime = lastAccessTime + TimeUnit.SECONDS.toMillis(10);
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_whenMaxIdleTime_isSmallerThan_TTL() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1, 100, TimeUnit.SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long delayToExpiration = TimeUnit.SECONDS.toMillis(10);

        long expectedExpirationTime = delayToExpiration + entryView.getCreationTime();
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_whenMaxIdleTime_isBiggerThan_TTL() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1, 5, TimeUnit.SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();
        long expirationTime = entryView.getExpirationTime();

        long expectedExpirationTime = creationTime + TimeUnit.SECONDS.toMillis(5);
        assertEquals(expectedExpirationTime, expirationTime);
    }

    @Test
    public void testCreationTime_is_minus_one_when_per_entry_stats_false() {
        IMap<Integer, Integer> map = createMapWithLRU(false);
        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        assertEquals(-1, entryView.getCreationTime());
    }

    @Test
    public void testLastAccessTime_is_set_at_creation_time__when_per_entry_stats_false() {
        long nowBeforePut = getStripedNow();

        IMap<Integer, Integer> map = createMapWithLRU(false);
        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        assertTrue(entryView.getLastAccessTime() >= nowBeforePut);
    }

    private static long getStripedNow() {
        Record record = newRecord();
        record.setLastAccessTime(Clock.currentTimeMillis());
        return record.getLastAccessTime();
    }

    @Test
    public void testCreationTime_is_set_when_per_entry_stats_true() {
        long nowBeforePut = getStripedNow();

        IMap<Integer, Integer> map = createMapWithLRU(true);
        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        assertTrue(entryView.getCreationTime() >= nowBeforePut);
    }

    @Test
    public void testLastAccessTime_is_not_set_at_creation_time_when_per_entry_stats_true() {
        IMap<Integer, Integer> map = createMapWithLRU(true);
        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        assertEquals(0, entryView.getLastAccessTime());
    }

    @Test
    public void testExpirationTime_calculated_against_lastUpdateTime_after_PutWithNoTTL() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 1, MINUTES);
        sleepMillis(1);
        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long expectedExpirationTime = Long.MAX_VALUE;
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void replace_shifts_expiration_time_when_succeeded() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 100, SECONDS);
        long expirationTimeAfterPut = getExpirationTime(map, 1);
        sleepAtLeastMillis(1000);

        map.replace(1, 1, 2);

        long expirationTimeAfterReplace = getExpirationTime(map, 1);
        assertTrue(expirationTimeAfterReplace > expirationTimeAfterPut);
    }

    @Test
    public void replace_does_not_shift_expiration_time_when_failed() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 100, SECONDS);
        long expirationTimeAfterPut = getExpirationTime(map, 1);
        sleepAtLeastMillis(3);

        int wrongOldValue = -1;
        map.replace(1, wrongOldValue, 2);

        long expirationTimeAfterReplace = getExpirationTime(map, 1);
        assertEquals(expirationTimeAfterReplace, expirationTimeAfterPut);
    }

    @Test
    public void last_access_time_updated_on_primary_when_read_backup_data_enabled() {
        String mapName = "test";

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setBackupCount(0);
        mapConfig.setAsyncBackupCount(0);
        mapConfig.setReadBackupData(true);
        mapConfig.setPerEntryStatsEnabled(true);
        mapConfig.setMaxIdleSeconds(20);
        mapConfig.setInMemoryFormat(inMemoryFormat());

        IMap map = createHazelcastInstance(config).getMap(mapName);
        map.put(1, 1);

        long lastAccessTimeBefore = map.getEntryView(1).getLastAccessTime();

        sleepAtLeastMillis(500);
        map.get(1);
        sleepAtLeastMillis(500);
        map.get(1);

        long lastAccessTimeAfter = map.getEntryView(1).getLastAccessTime();

        String msg = String.format("lastAccessTimeBefore:%d,"
                + " lastAccessTimeAfter:%d", lastAccessTimeBefore, lastAccessTimeAfter);

        assertTrue(msg, lastAccessTimeAfter > lastAccessTimeBefore);
    }

    @Test
    @Category(SlowTest.class)
    public void get_after_multiple_put_with_ttl_shifts_expiration_time() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 30, SECONDS);
        sleepSeconds(10);
        map.put(1, 1, 5, SECONDS, 2, SECONDS);

        map.get(1);
        Integer value = map.get(1);

        if (value == null) {
            fail("Expect second get is non-null");
        }
    }

    @Test
    public void no_expiration_happens_when_max_idle_is_int_max() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(Integer.MAX_VALUE);

        map.set(1, 1);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        assertEquals(Long.MAX_VALUE, expirationTimeAfterPut);
    }

    @Test
    public void no_expiration_happens_when_max_idle_is_zero() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(0);

        map.set(1, 1);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        assertEquals(Long.MAX_VALUE, expirationTimeAfterPut);
    }

    @Test
    public void no_expiration_happens_when_max_idle_is_close_to_int_max() {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(Integer.MAX_VALUE - 100);

        map.set(1, 1);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        assertEquals(Long.MAX_VALUE, expirationTimeAfterPut);
    }

    @Test
    public void no_expiration_happens_when_ttl_is_int_max() {
        IMap<Integer, Integer> map = createMapWithTTLSeconds(Integer.MAX_VALUE);

        map.set(1, 1);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        assertEquals(Long.MAX_VALUE, expirationTimeAfterPut);
    }

    @Test
    public void no_expiration_happens_when_ttl_is_zero() {
        IMap<Integer, Integer> map = createMapWithTTLSeconds(0);

        map.set(1, 1);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        assertEquals(Long.MAX_VALUE, expirationTimeAfterPut);
    }

    @Test
    public void no_expiration_happens_when_ttl_is_close_to_int_max() {
        IMap<Integer, Integer> map = createMapWithTTLSeconds(Integer.MAX_VALUE - 100);

        map.set(1, 1);

        long expirationTimeAfterPut = getExpirationTime(map, 1);

        assertEquals(Long.MAX_VALUE, expirationTimeAfterPut);
    }

    protected InMemoryFormat inMemoryFormat() {
        return BINARY;
    }

    private <T, U> IMap<T, U> createMap() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(inMemoryFormat());
        mapConfig.setPerEntryStatsEnabled(perEntryStatsEnabled);
        mapConfig.setStatisticsEnabled(statisticsEnabled);
        return createHazelcastInstance(config).getMap(mapName);
    }

    private <T, U> IMap<T, U> createMapWithLRU(boolean perEntryStats) {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(inMemoryFormat());
        mapConfig.setPerEntryStatsEnabled(perEntryStats);
        mapConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.LRU);
        return createHazelcastInstance(config).getMap(mapName);
    }

    @SuppressWarnings("SameParameterValue")
    private IMap<Integer, Integer> createMapWithMaxIdleSeconds(int maxIdleSeconds) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        config.getMapConfig(mapName)
                .setMaxIdleSeconds(maxIdleSeconds)
                .setInMemoryFormat(inMemoryFormat())
                .setPerEntryStatsEnabled(true);

        HazelcastInstance node = createHazelcastInstance(config);
        return node.getMap(mapName);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMetricsConfig().setEnabled(false);
        return config;
    }

    @SuppressWarnings("SameParameterValue")
    private IMap<Integer, Integer> createMapWithTTLSeconds(int ttlSeconds) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setTimeToLiveSeconds(ttlSeconds)
                .setInMemoryFormat(inMemoryFormat())
                .setPerEntryStatsEnabled(true);

        HazelcastInstance node = createHazelcastInstance(config);
        return node.getMap(mapName);
    }

    @SuppressWarnings("SameParameterValue")
    private static long getExpirationTime(IMap<Integer, Integer> map, int key) {
        EntryView<Integer, Integer> entryView = map.getEntryView(key);
        return entryView.getExpirationTime();
    }

    private static Record newRecord() {
        return new Record() {
            @Override
            public Object getValue() {
                return null;
            }

            @Override
            public void setValue(Object value) {

            }

            @Override
            public long getCost() {
                return 0;
            }

            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public void setVersion(int version) {

            }

            @Override
            public RecordReaderWriter getMatchingRecordReaderWriter() {
                return null;
            }
        };
    }
}
