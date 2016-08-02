/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.RandomPicker.getInt;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class NearCacheStalenessTest extends HazelcastTestSupport {

    private final int ENTRY_COUNT = 1;
    private final int NEAR_CACHE_INVALIDATOR_THREAD_COUNT = 3;
    private final int NEAR_CACHE_REMOVER_THREAD_COUNT = 3;
    private final int NEAR_CACHE_PUTTER_THREAD_COUNT = 10;
    private final String MAP_NAME = randomMapName();
    private final AtomicBoolean stop = new AtomicBoolean(false);

    private IMap map1;
    private IMap map2;

    @Before
    public void setUp() throws Exception {
        Config config = getConfig();

        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true).setCacheLocalEntries(true);

        config.getMapConfig(MAP_NAME).setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        map1 = node1.getMap(MAP_NAME);
        map2 = node2.getMap(MAP_NAME);
    }

    @Test
    public void testNearCache_notContainsStaleValue_whenUpdatedByMultipleThreads() throws Exception {
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < NEAR_CACHE_INVALIDATOR_THREAD_COUNT; i++) {
            Thread putter = new NearCacheInvalidator();
            threads.add(putter);
        }

        for (int i = 0; i < NEAR_CACHE_REMOVER_THREAD_COUNT; i++) {
            Thread getter = new NearCachePutter();
            threads.add(getter);
        }

        for (int i = 0; i < NEAR_CACHE_PUTTER_THREAD_COUNT; i++) {
            Thread remover = new NearCacheRemover();
            threads.add(remover);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(15);

        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        // give some-time to receive possible latest invalidations.
        sleepSeconds(10);

        assertNoStaleDataExistInNearCache(map1);

    }

    protected void assertNoStaleDataExistInNearCache(IMap map) {
        // 1. Get all entries when near-cache is full, so some values will come from near-cache.
        Map fromNearCache = getAllEntries(map);

        // 2. Clear near-cache
        ((NearCachedMapProxyImpl) map).getNearCache().clear();

        // 3. Get all values when near-cache is empty, these requests
        // will go to underlying IMap directly because near-cache is empty.
        Map fromIMap = getAllEntries(map);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals(fromIMap.get(i), fromNearCache.get(i));
        }
    }

    protected HashMap getAllEntries(IMap iMap) {
        HashMap localMap = new HashMap(ENTRY_COUNT);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            localMap.put(i, iMap.get(i));
        }
        return localMap;
    }

    protected NearCacheConfig newNearCacheConfig() {
        return new NearCacheConfig();
    }


    private class NearCacheInvalidator extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                map2.put(0, getInt(ENTRY_COUNT));
            }
        }
    }


    private class NearCachePutter extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                map1.get(0);
            }
        }
    }

    private class NearCacheRemover extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                map1.remove(0);
            }
        }
    }
}