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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static com.hazelcast.map.impl.nearcache.MapNearCacheRecordStateStressTest.assertNearCacheRecordStates;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ClientCacheNearCacheRecordStateStressTest extends HazelcastTestSupport {

    private static final int KEY_SPACE = 100;
    private static final int TEST_RUN_SECONDS = 60;
    private static final int GET_ALL_THREAD_COUNT = 2;
    private static final int PUT_ALL_THREAD_COUNT = 1;
    private static final int GET_THREAD_COUNT = 1;
    private static final int PUT_THREAD_COUNT = 1;
    private static final int PUT_IF_ABSENT_THREAD_COUNT = 1;
    private static final int CLEAR_THREAD_COUNT = 1;
    private static final int REMOVE_THREAD_COUNT = 1;

    @Parameter
    public NearCacheConfig.LocalUpdatePolicy localUpdatePolicy;

    @Parameters(name = "localUpdatePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {CACHE_ON_UPDATE},
                {INVALIDATE},
        });
    }

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final AtomicBoolean stop = new AtomicBoolean();

    private Cache<Integer, Integer> clientCache;
    private Cache<Integer, Integer> memberCache;

    @Before
    public void setUp() {
        String cacheName = randomMapName();

        Config config = getBaseConfig();

        ClientConfig clientConfig = new ClientConfig()
                .addNearCacheConfig(newNearCacheConfig(cacheName));

        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        CacheConfig<Integer, Integer> cacheConfig = new CacheConfig<Integer, Integer>()
                .setEvictionConfig(evictionConfig);

        HazelcastInstance member = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        CachingProvider provider = createServerCachingProvider(member);
        CacheManager serverCacheManager = provider.getCacheManager();

        memberCache = serverCacheManager.createCache(cacheName, cacheConfig);

        CachingProvider clientCachingProvider = createClientCachingProvider(client);
        CacheManager cacheManager = clientCachingProvider.getCacheManager();

        clientCache = cacheManager.createCache(cacheName, cacheConfig);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void allRecordsAreInReadableStateInTheEnd() {
        // populated from member
        for (int i = 0; i < KEY_SPACE; i++) {
            memberCache.put(i, i);
        }

        List<Thread> threads = new ArrayList<Thread>();
        // member
        for (int i = 0; i < PUT_THREAD_COUNT; i++) {
            Put put = new Put(memberCache);
            threads.add(put);
        }
        // client
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
            assertJoinable(thread);
        }

        assertFinalRecordStateIsReadPermitted(clientCache);
    }

    private NearCacheConfig newNearCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        return new NearCacheConfig(cacheName)
                .setSerializeKeys(false)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(localUpdatePolicy)
                .setEvictionConfig(evictionConfig);
    }

    private static void assertFinalRecordStateIsReadPermitted(Cache clientCache) {
        NearCachedClientCacheProxy proxy = (NearCachedClientCacheProxy) clientCache;
        DefaultNearCache nearCache = (DefaultNearCache) proxy.getNearCache().unwrap(DefaultNearCache.class);

        assertNearCacheRecordStates(nearCache, KEY_SPACE);
    }

    private class Put extends Thread {

        private final Cache<Integer, Integer> cache;

        private Put(Cache<Integer, Integer> cache) {
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

        private final Cache<Integer, Integer> cache;

        private Remove(Cache<Integer, Integer> cache) {
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

        private final Cache<Integer, Integer> cache;

        private PutIfAbsent(Cache<Integer, Integer> cache) {
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

        private final Cache<Integer, Integer> cache;

        private Clear(Cache<Integer, Integer> cache) {
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

        private final Cache<Integer, Integer> cache;

        private GetAll(Cache<Integer, Integer> cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            HashSet<Integer> keys = new HashSet<Integer>();
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

        private final Cache<Integer, Integer> cache;

        private PutAll(Cache<Integer, Integer> cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
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

        private final Cache<Integer, Integer> cache;

        private Get(Cache<Integer, Integer> cache) {
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
