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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExpirationTimeTest extends HazelcastTestSupport {

    private static final long ONE_MINUTE_IN_MILLIS = MINUTES.toMillis(1);

    @Test
    public void testExpirationTime_withTTL() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + ONE_MINUTE_IN_MILLIS;
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_withTTL_withShorterMaxIdle() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, ONE_MINUTE_IN_MILLIS, MILLISECONDS, 10, SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + SECONDS.toMillis(10);
        assertEquals(expectedExpirationTime, entryView.getExpirationTime());
    }

    @Test
    public void testExpirationTime_withShorterTTL_andMaxIdle() {
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
        long lastAccessTime = entryView.getLastAccessTime();
        long delayToExpiration = lastAccessTime + TimeUnit.SECONDS.toMillis(10);

        // lastAccessTime is zero after put, we can find expiration by this calculation
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
    public void testLastAccessTime_isZero_afterFirstPut() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        assertEquals(0L, entryView.getLastAccessTime());
    }

    @Test
    public void testExpirationTime_calculated_against_lastUpdateTime_after_PutWithNoTTL() {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 1, MINUTES);
        sleepMillis(1);
        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long expectedExpirationTime = entryView.getLastUpdateTime() + MINUTES.toMillis(1);
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
        mapConfig.setMaxIdleSeconds(20);
        mapConfig.setInMemoryFormat(inMemoryFormat());

        IMap map = createHazelcastInstance(config).getMap(mapName);
        map.put(1, 1);

        long lastAccessTimeBefore = map.getEntryView(1).getLastAccessTime();

        sleepAtLeastMillis(10);
        map.get(1);
        sleepAtLeastMillis(10);
        map.get(1);

        long lastAccessTimeAfter = map.getEntryView(1).getLastAccessTime();

        String msg = String.format("lastAccessTimeBefore:%d,"
                + " lastAccessTimeAfter:%d", lastAccessTimeBefore, lastAccessTimeAfter);

        assertTrue(msg, lastAccessTimeAfter > lastAccessTimeBefore);
    }

    protected InMemoryFormat inMemoryFormat() {
        return BINARY;
    }

    private IMap<Integer, Integer> createMap() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat());
        HazelcastInstance node = createHazelcastInstance(config);
        return node.getMap(mapName);
    }

    @SuppressWarnings("SameParameterValue")
    private IMap<Integer, Integer> createMapWithMaxIdleSeconds(int maxIdleSeconds) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName)
                .setMaxIdleSeconds(maxIdleSeconds)
                .setInMemoryFormat(inMemoryFormat());

        HazelcastInstance node = createHazelcastInstance(config);
        return node.getMap(mapName);
    }

    @SuppressWarnings("SameParameterValue")
    private IMap<Integer, Integer> createMapWithTTLSeconds(int ttlSeconds) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName)
                .setTimeToLiveSeconds(ttlSeconds)
                .setInMemoryFormat(inMemoryFormat());

        HazelcastInstance node = createHazelcastInstance(config);
        return node.getMap(mapName);
    }

    @SuppressWarnings("SameParameterValue")
    private static long getExpirationTime(IMap<Integer, Integer> map, int key) {
        EntryView<Integer, Integer> entryView = map.getEntryView(key);
        return entryView.getExpirationTime();
    }
}
