/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheReadWriteThroughTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz1;
    private HazelcastInstance hz2;

    private HazelcastServerCachingProvider cachingProvider1;
    private HazelcastServerCachingProvider cachingProvider2;

    protected Config createConfig() {
        return new Config();
    }

    @Before
    public void setup() {
        factory = new TestHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance(createConfig());
        hz2 = factory.newHazelcastInstance(createConfig());
        cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(hz2);
    }

    @After
    public void tearDown() {
        cachingProvider1.close();
        cachingProvider2.close();
        factory.shutdownAll();
    }

    protected CacheConfig<Integer, Integer> createCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig<Integer, Integer>();
        cacheConfig.setTypes(Integer.class, Integer.class);
        return cacheConfig;
    }

    @Test
    public void test_getAll_readThrough() throws Exception {
        final String cacheName = randomName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, Integer> config = createCacheConfig();
        config.setReadThrough(true);
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(new GetAllAsyncCacheLoader()));

        Cache<Integer, Integer> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 150; i++) {
            keys.add(i);
        }

        Map<Integer, Integer> loaded = cache.getAll(keys);
        assertEquals(100, loaded.size());
    }

    @Test
    public void test_loadAll_readThrough() throws Exception {
        final String cacheName = randomName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, Integer> config = createCacheConfig();
        config.setReadThrough(true);
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(new GetAllAsyncCacheLoader()));

        Cache<Integer, Integer> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 150; i++) {
            keys.add(i);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        cache.loadAll(keys, false, new CompletionListener() {
            @Override
            public void onCompletion() {
                latch.countDown();
            }

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
                latch.countDown();
            }
        });

        latch.await();
        assertEquals(100, cache.unwrap(ICache.class).size());
    }

    public static class GetAllAsyncCacheLoader implements CacheLoader<Integer, Integer>, Serializable {

        @Override
        public Integer load(Integer key) {
            return key != null && key < 100 ? key : null;
        }

        @Override
        public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, Integer> result = new HashMap<Integer, Integer>();
            for (Integer key : keys ) {
                Integer value = load(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }
    }

    private void do_put_writeThrough(boolean acceptAll) {
        final int ENTRY_COUNT = 100;
        final String cacheName = randomName();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        PutCacheWriter putCacheWriter = new PutCacheWriter(acceptAll);
        CacheConfig<Integer, Integer> config = createCacheConfig();
        config.setWriteThrough(true);
        config.setCacheWriterFactory(FactoryBuilder.factoryOf(putCacheWriter));

        ICache<Integer, Integer> cache = cacheManager.createCache(cacheName, config).unwrap(ICache.class);
        assertNotNull(cache);

        List<Integer> bannedKeys = new ArrayList<Integer>();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            try {
                cache.put(i, i);
            } catch (CacheWriterException e) {
                bannedKeys.add(i);
            }
        }

        assertEquals(ENTRY_COUNT - bannedKeys.size(), cache.size());
        for (Integer bannedKey : bannedKeys) {
            assertNull(cache.get(bannedKey));
        }
    }

    @Test
    public void test_put_writeThrough_allKeysAccepted() {
        do_put_writeThrough(true);
    }

    @Test
    public void test_put_writeThrough_someKeysBanned() {
        do_put_writeThrough(false);
    }

    public static class PutCacheWriter implements CacheWriter<Integer, Integer>, Serializable {

        private final boolean acceptAll;

        private PutCacheWriter(boolean acceptAll) {
            this.acceptAll = acceptAll;
        }

        private boolean isAcceptableKey(int key) {
            if (acceptAll) {
                return true;
            }
            return key % 10 != 0;
        }

        @Override
        public void write(Cache.Entry<? extends Integer, ? extends Integer> entry)
                throws CacheWriterException {
            Integer keyValue = entry.getKey().intValue();
            if (!isAcceptableKey(keyValue)) {
                throw new CacheWriterException("Key value is invalid: " + keyValue);
            }
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries)
                throws CacheWriterException {
        }

        @Override
        public void delete(Object key)
                throws CacheWriterException {
        }

        @Override
        public void deleteAll(Collection<?> keys)
                throws CacheWriterException {
        }
    }

}