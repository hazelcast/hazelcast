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
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicCacheLiteMemberTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    private HazelcastServerCachingProvider instanceCachingProvider;
    private HazelcastServerCachingProvider liteCachingProvider;

    private String cacheName;


    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newHazelcastInstance();
        final HazelcastInstance lite = factory.newHazelcastInstance(new Config().setLiteMember(true));

        instanceCachingProvider = createServerCachingProvider(instance);
        liteCachingProvider = createServerCachingProvider(lite);
        cacheName = randomMapName();
    }

    @After
    public void tear() {
        instanceCachingProvider.getCacheManager().destroyCache(cacheName);
        instanceCachingProvider.close();
        liteCachingProvider.close();
        factory.shutdownAll();
    }

    @Test
    public void testCacheCreation()
            throws InterruptedException {
        testCacheCreation(instanceCachingProvider, liteCachingProvider);
    }

    @Test
    public void testCacheCreationFromLiteMember()
            throws InterruptedException {
        testCacheCreation(liteCachingProvider, instanceCachingProvider);
    }

    private void testCacheCreation(final HazelcastServerCachingProvider providerToCreate,
                                   final HazelcastServerCachingProvider providerToValidate) {
        CacheManager cacheManager = providerToCreate.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                CacheManager cm2 = providerToValidate.getCacheManager();
                assertNotNull(cm2.getCache(cacheName));
            }
        });
    }

    @Test
    public void testPut()
            throws InterruptedException {
        CacheManager cacheManager = liteCachingProvider.getCacheManager();

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key1 = 1;
        String value1 = "value1";
        cache.put(key1, value1);
        assertEquals(value1, cache.get(key1));
    }

    @Test
    public void testCompletion()
            throws InterruptedException {

        CacheManager cacheManager = liteCachingProvider.getCacheManager();

        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        final SimpleEntryListener<Integer, String> listener = new SimpleEntryListener<Integer, String>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Integer, String>(
                        FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        final Cache<Integer, String> instanceCache = cacheManager.createCache(cacheName, config);

        Integer key1 = 1;
        String value1 = "value1";
        instanceCache.put(key1, value1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(1, listener.created.get());
            }
        });
        Integer key2 = 2;
        String value2 = "value2";
        instanceCache.put(key2, value2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(2, listener.created.get());
            }
        });

        instanceCache.remove(key1);
        instanceCache.remove(key2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(2, listener.removed.get());
            }
        });
    }

    @Test
    public void testCachesDestroy() {
        CacheManager cacheManager = instanceCachingProvider.getCacheManager();
        CacheManager cacheManager2 = liteCachingProvider.getCacheManager();
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
                    fail("get should throw IllegalStateException");
                } catch (IllegalStateException e) {
                    //ignored as expected
                }
            }
        });
    }

    @Test
    public void testCachesDestroyFromOtherManagers() {
        CacheManager cacheManager = instanceCachingProvider.getCacheManager();
        CacheManager cacheManager2 = liteCachingProvider.getCacheManager();
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
            for (CacheEntryEvent<? extends K, ? extends V> cacheEntryEvent : cacheEntryEvents) {
                created.incrementAndGet();
            }
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> cacheEntryEvent : cacheEntryEvents) {
                expired.incrementAndGet();
            }
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> cacheEntryEvent : cacheEntryEvents) {
                System.out.println("Removed " + removed.incrementAndGet() + " " + this + " >>> " + cacheEntryEvent.getKey());
            }
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> cacheEntryEvent : cacheEntryEvents) {
                updated.incrementAndGet();
            }
        }
    }

}
