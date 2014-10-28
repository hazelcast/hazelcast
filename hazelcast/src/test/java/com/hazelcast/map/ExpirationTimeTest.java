/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ExpirationTimeTest extends HazelcastTestSupport {

    @Test
    public void testExpirationTime_withTTL() throws Exception {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 1, TimeUnit.MINUTES);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long TTL = TimeUnit.MINUTES.toMillis(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + TTL;

        assertEquals(expectedExpirationTime, entryView.getExpirationTime());

    }


    @Test
    public void testExpirationTime_withTTL_afterMultipleUpdates() throws Exception {
        IMap<Integer, Integer> map = createMap();

        map.put(1, 1, 1, TimeUnit.MINUTES);

        sleepMillis(1);

        map.put(1, 1, 1, TimeUnit.MINUTES);

        sleepMillis(1);

        map.put(1, 1, 1, TimeUnit.MINUTES);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long TTL = TimeUnit.MINUTES.toMillis(1);
        long creationTime = entryView.getCreationTime();

        long expectedExpirationTime = creationTime + TTL;

        assertEquals(expectedExpirationTime, entryView.getExpirationTime());

    }

    @Test
    public void testExpirationTime_withMaxIdleTime() throws Exception {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long creationTime = entryView.getCreationTime();
        long expirationTime = entryView.getExpirationTime();

        long expectedExpirationTime = creationTime + TimeUnit.SECONDS.toMillis(10);

        assertEquals(expectedExpirationTime, expirationTime);
    }


    @Test
    public void testExpirationTime_withMaxIdleTime_afterMultipleAccesses() throws Exception {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1);

        sleepMillis(999);

        map.get(1);

        sleepMillis(23);

        map.containsKey(1);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long lastAccessTime = entryView.getLastAccessTime();
        long expectedExpirationTime = lastAccessTime + TimeUnit.SECONDS.toMillis(10);

        assertEquals(expectedExpirationTime, entryView.getExpirationTime());

    }


    @Test
    public void testExpirationTime_whenMaxIdleTime_isSmallerThan_TTL() throws Exception {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1, 100, TimeUnit.SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long lastAccessTime = entryView.getLastAccessTime();
        long expectedExpirationTime = lastAccessTime + TimeUnit.SECONDS.toMillis(10);

        assertEquals(expectedExpirationTime, entryView.getExpirationTime());

    }


    @Test
    public void testExpirationTime_whenMaxIdleTime_isBiggerThan_TTL() throws Exception {
        IMap<Integer, Integer> map = createMapWithMaxIdleSeconds(10);

        map.put(1, 1, 5, TimeUnit.SECONDS);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);

        long creationTime = entryView.getCreationTime();
        long expirationTime = entryView.getExpirationTime();

        long expectedExpirationTime = creationTime + TimeUnit.SECONDS.toMillis(5);

        assertEquals(expectedExpirationTime, expirationTime);

    }

    private IMap<Integer, Integer> createMap() {
        final String mapName = randomMapName();
        final HazelcastInstance node = createHazelcastInstance();
        return node.getMap(mapName);
    }

    private IMap<Integer, Integer> createMapWithMaxIdleSeconds(int maxIdleSeconds) {
        final String mapName = randomMapName();

        final Config config = new Config();
        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMaxIdleSeconds(maxIdleSeconds);

        final HazelcastInstance node = createHazelcastInstance(config);
        return node.getMap(mapName);
    }
}
