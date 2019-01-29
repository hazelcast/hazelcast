/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.impl.NearCachedClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ClientCacheRecordStateStressTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "test";
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

    private TestHazelcastFactory factory;
    private AtomicBoolean stop;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();
        stop = new AtomicBoolean();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void allRecordsAreInReadableStateInTheEnd() throws Exception {
        HazelcastInstance member = factory.newHazelcastInstance();
        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(member);
        CacheManager serverCacheManager = provider.getCacheManager();

        factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        // populated from member
        CacheConfig<Integer, Integer> cacheConfig = new CacheConfig<Integer, Integer>();
        cacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        Cache<Integer, Integer> memberCache = serverCacheManager.createCache(CACHE_NAME, cacheConfig);
        for (int i = 0; i < KEY_SPACE; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(newNearCacheConfig());

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
        Cache<Integer, Integer> clientCache = cacheManager.createCache(CACHE_NAME, cacheConfig);

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

        assertFinalRecordStateIsReadPermitted(clientCache, getSerializationService(member));
    }

    private NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(CACHE_NAME)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(localUpdatePolicy);
        nearCacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        return nearCacheConfig;
    }

    private void assertFinalRecordStateIsReadPermitted(Cache clientCache, InternalSerializationService serializationService) {
        NearCachedClientCacheProxy proxy = (NearCachedClientCacheProxy) clientCache;
        DefaultNearCache nearCache = (DefaultNearCache) proxy.getNearCache().unwrap(DefaultNearCache.class);
        NearCacheRecordStore nearCacheRecordStore = nearCache.getNearCacheRecordStore();

        for (int i = 0; i < KEY_SPACE; i++) {
            Data key = serializationService.toData(i);
            NearCacheRecord record = nearCacheRecordStore.getRecord(key);

            if (record != null) {
                assertEquals(record.toString(), READ_PERMITTED, record.getRecordState());
            }
        }
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
