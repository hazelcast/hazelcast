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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.impl.nearcache.KeyStateMarker;
import com.hazelcast.map.impl.nearcache.KeyStateMarkerImpl;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ClientCacheKeyStateMarkerStressTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public NearCacheConfig.LocalUpdatePolicy localUpdatePolicy;

    @Parameterized.Parameters(name = "localUpdatePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {CACHE_ON_UPDATE},
                {INVALIDATE},
        });
    }

    private final int KEY_SPACE = 10000;
    private final int TEST_RUN_SECONDS = 60;
    private final int GET_ALL_THREAD_COUNT = 2;
    private final int PUT_ALL_THREAD_COUNT = 1;
    private final int GET_THREAD_COUNT = 1;
    private final int PUT_THREAD_COUNT = 1;
    private final int PUT_IF_ABSENT_THREAD_COUNT = 1;
    private final int CLEAR_THREAD_COUNT = 1;
    private final int REMOVE_THREAD_COUNT = 1;
    private final String cacheName = "test";
    private final AtomicBoolean stop = new AtomicBoolean();

    private TestHazelcastFactory factory;

    @Before
    public void setUp() throws Exception {
        factory = new TestHazelcastFactory();
        stop.set(false);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void final_state_of_all_slots_are_unmarked() throws Exception {
        HazelcastInstance member = factory.newHazelcastInstance();
        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(member);
        final CacheManager serverCacheManager = provider.getCacheManager();

        factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        // populated from member.
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        final Cache memberCache = serverCacheManager.createCache(cacheName, cacheConfig);
        for (int i = 0; i < KEY_SPACE; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true).setLocalUpdatePolicy(localUpdatePolicy)
                .getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        List<Thread> threads = new ArrayList<Thread>();

        // member
        for (int i = 0; i < PUT_THREAD_COUNT; i++) {
            Put put = new Put(memberCache);
            threads.add(put);
        }

        // client
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        final Cache clientCache = cacheManager.createCache(cacheName, cacheConfig);

        for (int i = 0; i < GET_ALL_THREAD_COUNT; i++) {
            GetAll getAll = new GetAll(clientCache);
            threads.add(getAll);
        }

        for (int i = 0; i < PUT_ALL_THREAD_COUNT; i++) {
            PutAll putAll = new PutAll(clientCache);
            threads.add(putAll);
        }

        for (int i = 0; i < PUT_IF_ABSENT_THREAD_COUNT; i++) {
            PutIfAbsent putIfAbsent = new PutIfAbsent(clientCache);
            threads.add(putIfAbsent);
        }

        for (int i = 0; i < GET_THREAD_COUNT; i++) {
            Get get = new Get(clientCache);
            threads.add(get);
        }

        for (int i = 0; i < REMOVE_THREAD_COUNT; i++) {
            Remove remove = new Remove(clientCache);
            threads.add(remove);
        }

        for (int i = 0; i < CLEAR_THREAD_COUNT; i++) {
            Clear clear = new Clear(clientCache);
            threads.add(clear);
        }

        // start threads
        for (Thread thread : threads) {
            thread.start();
        }

        // stress for a while
        sleepSeconds(TEST_RUN_SECONDS);

        // stop threads
        stop.set(true);
        for (Thread thread : threads) {
            thread.join();
        }

        assertAllKeysInUnmarkedState(clientCache);
    }

    private void assertAllKeysInUnmarkedState(Cache clientCache) {
        ClientCacheProxy proxy = ((ClientCacheProxy) clientCache);
        NearCache nearCache = proxy.getNearCache();
        KeyStateMarker keyStateMarker = proxy.getKeyStateMarker();
        AtomicIntegerArray marks = ((KeyStateMarkerImpl) keyStateMarker).getMarks();

        String msg = format("nearCacheSize=%d,localUpdatePolicy=%s, markerStates=(%s)",
                nearCache.size(), localUpdatePolicy, keyStateMarker);

        for (int i = 0; i < marks.length(); i++) {
            assertEquals(msg, 0, marks.get(i));
        }
    }

    private class Put extends Thread {
        private final Cache cache;

        private Put(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            do {
                for (int i = 0; i < KEY_SPACE; i++) {
                    cache.put(i, getInt(KEY_SPACE));
                }
                sleepAtLeastMillis(5000);
            } while (!stop.get());
        }
    }

    private class Remove extends Thread {
        private final Cache cache;

        private Remove(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            do {
                for (int i = 0; i < KEY_SPACE; i++) {
                    cache.remove(i);
                }
                sleepAtLeastMillis(1000);
            } while (!stop.get());
        }
    }

    private class PutIfAbsent extends Thread {
        private final Cache cache;

        private PutIfAbsent(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            do {
                for (int i = 0; i < KEY_SPACE; i++) {
                    cache.putIfAbsent(i, getInt(MAX_VALUE));
                }
                sleepAtLeastMillis(1000);
            } while (!stop.get());
        }
    }

    private class Clear extends Thread {
        private final Cache cache;

        private Clear(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            do {
                cache.clear();
                sleepAtLeastMillis(5000);
            } while (!stop.get());
        }
    }

    private class GetAll extends Thread {
        private final Cache cache;

        private GetAll(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            HashSet keys = new HashSet();
            for (int i = 0; i < KEY_SPACE; i++) {
                keys.add(i);
            }

            do {
                cache.getAll(keys);
                sleepAtLeastMillis(10);
            } while (!stop.get());
        }
    }

    private class PutAll extends Thread {
        private final Cache cache;

        private PutAll(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            HashMap map = new HashMap();
            do {
                map.clear();
                for (int i = 0; i < KEY_SPACE; i++) {
                    map.put(i, getInt(MAX_VALUE));
                }

                cache.putAll(map);
                sleepAtLeastMillis(2000);
            } while (!stop.get());
        }
    }

    private class Get extends Thread {
        private final Cache cache;

        private Get(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            do {
                for (int i = 0; i < KEY_SPACE; i++) {
                    cache.get(i);
                }
            } while (!stop.get());
        }
    }
}
