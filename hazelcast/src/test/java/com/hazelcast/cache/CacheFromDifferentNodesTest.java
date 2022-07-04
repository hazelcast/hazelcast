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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheFromDifferentNodesTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    private HazelcastServerCachingProvider cachingProvider1;
    private HazelcastServerCachingProvider cachingProvider2;

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        cachingProvider1 = createServerCachingProvider(hz1);
        cachingProvider2 = createServerCachingProvider(hz2);
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
        final String cacheName = randomString();

        CacheManager cacheManager = cachingProvider1.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
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


    // Issue https://github.com/hazelcast/hazelcast/issues/5865
    @Test
    public void testCompletionTestByPuttingAndRemovingFromDifferentNodes()
            throws InterruptedException {
        String cacheName = "simpleCache";

        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        final SimpleEntryListener<Integer, String> listener = new SimpleEntryListener<Integer, String>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Integer, String>(
                        FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache1 = cacheManager1.createCache(cacheName, config);
        Cache<Integer, String> cache2 = cacheManager2.getCache(cacheName);

        assertNotNull(cache1);
        assertNotNull(cache2);

        Integer key1 = 1;
        String value1 = "value1";
        cache1.put(key1, value1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.created.get());
            }
        });

        Integer key2 = 2;
        String value2 = "value2";
        cache1.put(key2, value2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, listener.created.get());
            }
        });

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(key1);
        keys.add(key2);
        cache2.removeAll(keys);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, listener.removed.get());
            }
        });
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
            public void run() throws Exception {
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
            public void run() throws Exception {
                c2.get("key");
            }
        }, 10);
    }

    public static class SimpleEntryListener<K, V>
            implements CacheEntryListener<K, V>,
            CacheEntryCreatedListener<K, V>,
            CacheEntryUpdatedListener<K, V>,
            CacheEntryRemovedListener<K, V>,
            CacheEntryExpiredListener<K, V>,
            Serializable {

        public AtomicInteger created = new AtomicInteger();
        public AtomicInteger expired = new AtomicInteger();
        public AtomicInteger removed = new AtomicInteger();
        public AtomicInteger updated = new AtomicInteger();

        public SimpleEntryListener() {
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            incrementCounter(cacheEntryEvents, created);
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            incrementCounter(cacheEntryEvents, expired);
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            incrementCounter(cacheEntryEvents, removed);
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            incrementCounter(cacheEntryEvents, updated);
        }

        private void incrementCounter(Iterable iterable, AtomicInteger counter) {
            // actual CacheEntryEvents don't matter
            // so we avoid referencing each event for the sake
            // of compatibility tests which have trouble
            // delegating to event classes across classloaders
            Iterator iter = iterable.iterator();
            while (iter.hasNext()) {
                counter.incrementAndGet();
                iter.next();
            }
        }
    }


}
