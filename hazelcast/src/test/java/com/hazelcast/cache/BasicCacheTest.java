/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.util.CacheConcurrentHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BasicCacheTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz1;
    private HazelcastInstance hz2;

    private HazelcastServerCachingProvider cachingProvider1;
    private HazelcastServerCachingProvider cachingProvider2;

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance();
        hz2 = factory.newHazelcastInstance();
        cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(hz2);
    }

    @After
    public void tear() {
        cachingProvider1.close();
        cachingProvider2.close();
        factory.shutdownAll();
    }

    @Test
    public void testJSRExample1()
            throws InterruptedException {
        final String cacheName = "simpleCache";

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                CacheManager cm2 = cachingProvider2.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });

        Integer key = 1;
        String value1 = "value";
        cache.put(key, value1);

        String value2 = cache.get(key);
        assertEquals(value1, value2);
        cache.remove(key);
        assertNull(cache.get(key));

        Cache<Integer, String> cache2 = cacheManager.getCache(cacheName);
        assertNotNull(cache2);

        key = 1;
        value1 = "value";
        cache.put(key, value1);

        value2 = cache.get(key);
        assertEquals(value1, value2);
        cache.remove(key);
        assertNull(cache.get(key));

        cacheManager.destroyCache(cacheName);
        cacheManager.close();
    }

    @Test
    public void testRemoveAll()
            throws InterruptedException {
        String cacheName = "simpleCache";

        CacheManager cacheManager = cachingProvider1.getCacheManager();

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key1 = 1;
        String value1 = "value1";
        cache.put(key1, value1);

        Integer key2 = 1;
        String value2 = "value2";
        cache.put(key2, value2);

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(key1);
        keys.add(key2);
        cache.removeAll(keys);

        assertNull(cache.get(key1));
        assertNull(cache.get(key2));

        cacheManager.destroyCache(cacheName);
    }

    @Test
    public void testCompletionTest()
            throws InterruptedException {
        String cacheName = "simpleCache";

        CacheManager cacheManager = cachingProvider1.getCacheManager();

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        SimpleEntryListener<Integer, String> listener = new SimpleEntryListener<Integer, String>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration = new MutableCacheEntryListenerConfiguration<Integer, String>(
                FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key1 = 1;
        String value1 = "value1";
        cache.put(key1, value1);
        assertEquals(1, listener.created.get());

        Integer key2 = 2;
        String value2 = "value2";
        cache.put(key2, value2);
        assertEquals(2, listener.created.get());

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(key1);
        keys.add(key2);
        cache.removeAll(keys);

        assertEquals(2, listener.removed.get());

    }

    @Test
    public void testJSRCreateDestroyCreate()
            throws InterruptedException {
        String cacheName = "simpleCache";

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Thread.sleep(1000);

        Integer key = 1;
        String value1 = "value";
        cache.put(key, value1);

        String value2 = cache.get(key);
        assertEquals(value1, value2);
        cache.remove(key);
        assertNull(cache.get(key));

        cacheManager.destroyCache(cacheName);
        assertNull(cacheManager.getCache(cacheName));

        Cache<Integer, String> cache1 = cacheManager.createCache(cacheName, config);
        assertNotNull(cache1);
    }

    @Test
    public void testCaches_NotEmpty() {
        CacheManager cacheManager = cachingProvider1.getCacheManager();

        ArrayList<String> caches1 = new ArrayList<String>();
        cacheManager.createCache("c1", new MutableConfiguration());
        cacheManager.createCache("c2", new MutableConfiguration());
        cacheManager.createCache("c3", new MutableConfiguration());
        caches1.add(cacheManager.getCache("c1").getName());
        caches1.add(cacheManager.getCache("c2").getName());
        caches1.add(cacheManager.getCache("c3").getName());

        Iterable<String> cacheNames = cacheManager.getCacheNames();
        Iterator<String> iterator = cacheNames.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    @Test
    public void testCachesDestroy() {
        CacheManager cacheManager = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();
        MutableConfiguration configuration = new MutableConfiguration();
        final Cache c1 = cacheManager.createCache("c1", configuration);
        final Cache c2 = cacheManager2.getCache("c1");
        c1.put("key", "value");
        cacheManager.destroyCache("c1");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                try {
                    c2.get("key");
                    throw new AssertionError("get should throw IllegalStateException");
                } catch (IllegalStateException e) {
                    //ignored as expected
                }
            }
        });
    }

    @Test
    public void testCachesDestroyFromOtherManagers() {
        CacheManager cacheManager = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();
        MutableConfiguration configuration = new MutableConfiguration();
        final Cache c1 = cacheManager.createCache("c1", configuration);
        final Cache c2 = cacheManager2.createCache("c2", configuration);
        c1.put("key", "value");
        c2.put("key", "value");
        cacheManager.close();
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                c2.get("key");
            }
        }, 10);
    }

    @Test
    public void testIterator() {

        CacheManager cacheManager = cachingProvider1.getCacheManager();

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        config.setName("SimpleCache");

        ICache<Integer, String> cache = (ICache<Integer, String>) cacheManager.createCache("simpleCache", config);

        int testSize = 1007;
        for (int i = 0; i < testSize; i++) {
            Integer key = i;
            String value1 = "value" + i;
            cache.put(key, value1);
        }

        assertEquals(testSize, cache.size());

        Iterator<Cache.Entry<Integer, String>> iterator = cache.iterator();
        assertNotNull(iterator);

        HashMap<Integer, String> resultMap = new HashMap<Integer, String>();

        int c = 0;
        while (iterator.hasNext()) {
            Cache.Entry<Integer, String> next = iterator.next();
            Integer key = next.getKey();
            String value = next.getValue();
            resultMap.put(key, value);
            c++;
        }
        assertEquals(testSize, c);

    }

    @Test
    public void testInitableIterator() {
        int testSize = 3007;
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        for (int fetchSize = 1; fetchSize < 102; fetchSize++) {
            CacheConcurrentHashMap<Data, String> cmap = new CacheConcurrentHashMap<Data, String>(1000);
            for (int i = 0; i < testSize; i++) {
                Integer key = i;
                Data data = ss.toData(key);
                String value1 = "value" + i;
                cmap.put(data, value1);
            }

            int nti = Integer.MAX_VALUE;
            int total = 0;
            int remaining = testSize;
            while (remaining > 0 && nti > 0) {
                int fsize = remaining > fetchSize ? fetchSize : remaining;
                CacheKeyIteratorResult iteratorResult = cmap.fetchNext(nti, fsize);
                List<Data> keys = iteratorResult.getKeys();
                nti = iteratorResult.getTableIndex();
                remaining -= keys.size();
                total += keys.size();
            }
            assertEquals(testSize, total);
        }
    }

    @Test
    public void testCachesTypedConfig() {
        CacheManager cacheManager = cachingProvider1.getCacheManager();

        CacheConfig<Integer, Long> config = new CacheConfig();
        String cacheName = "test";
        config.setName(cacheName);
        config.setTypes(Integer.class, Long.class);

        Cache<Integer, Long> cache = cacheManager.createCache(cacheName, config);
        Cache<Integer, Long> cache2 = cacheManager.getCache(cacheName, config.getKeyType(), config.getValueType());

        assertNotNull(cache);
        assertNotNull(cache2);
    }

    public static class SimpleEntryListener<K, V>
            implements CacheEntryListener<K, V>, CacheEntryCreatedListener<K, V>, CacheEntryUpdatedListener<K, V>,
                       CacheEntryRemovedListener<K, V>, CacheEntryExpiredListener<K, V>, Serializable {

        public AtomicInteger created = new AtomicInteger();
        public AtomicInteger expired = new AtomicInteger();
        public AtomicInteger removed = new AtomicInteger();
        public AtomicInteger updated = new AtomicInteger();

        public SimpleEntryListener() {
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            created.incrementAndGet();
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            expired.incrementAndGet();
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            removed.incrementAndGet();
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            updated.incrementAndGet();
        }
    }

}
