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

package com.hazelcast.cache.instance;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHazelcastInstanceAwareTest extends HazelcastTestSupport {

    static ConcurrentMap<Long, Boolean> HAZELCAST_INSTANCE_INJECTION_RESULT_MAP = new ConcurrentHashMap<Long, Boolean>();

    private static final String CACHE_NAME = "MyCache";

    @AfterClass
    public static void destroy() {
        HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.clear();
        HAZELCAST_INSTANCE_INJECTION_RESULT_MAP = null;
    }

    protected Config createConfig() {
        return new Config();
    }

    protected CacheConfig<Integer, Integer> createCacheConfig(String cacheName) {
        return new CacheConfig<Integer, Integer>(cacheName);
    }

    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
    }

    protected HazelcastInstance createInstance() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        Config config = createConfig();
        return instanceFactory.newHazelcastInstance(config);
    }

    private long generateUniqueHazelcastInjectionId() {
        long id;
        for (; ; ) {
            id = System.nanoTime();
            if (HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.putIfAbsent(id, false) == null) {
                break;
            }
        }
        return id;
    }

    @Test
    public void test_inject_hazelcastInstance_to_cacheLoader_if_itIs_hazelcastInstanceAware() {
        long id1 = generateUniqueHazelcastInjectionId();
        long id2 = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setCacheLoaderFactory(new HazelcastInstanceAwareCacheLoaderFactory(id1, id2));

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);

        assertEquals("Hazelcast instance has not been injected into cache loader factory!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hazelcast instance has not been injected into cache loader!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_inject_hazelcastInstance_to_cacheWriter_if_itIs_hazelcastInstanceAware() {
        long id1 = generateUniqueHazelcastInjectionId();
        long id2 = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setCacheWriterFactory(new HazelcastInstanceAwareCacheWriterFactory(id1, id2));

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);

        assertEquals("Hazelcast instance has not been injected into cache writer factory!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hazelcast instance has not been injected into cache writer!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_inject_hazelcastInstance_to_expiryPolicy_if_itIs_hazelcastInstanceAware() {
        long id1 = generateUniqueHazelcastInjectionId();
        long id2 = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setExpiryPolicyFactory(new HazelcastInstanceAwareExpiryPolicyFactory(id1, id2));

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);

        assertEquals("Hazelcast instance has not been injected into expiry policy factory!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hazelcast instance has not been injected into expiry policy!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_inject_hazelcastInstance_to_entryProcessor_if_itIs_hazelcastInstanceAware() {
        long id = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.invoke(1, new HazelcastInstanceAwareEntryProcessor(id));

        assertEquals("Hazelcast instance has not been injected into entry processor!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id));
    }

    @Test
    public void test_inject_hazelcastInstance_to_completionListener_if_itIs_hazelcastInstanceAware() {
        final long id = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.loadAll(new HashSet<Integer>(), false, new HazelcastInstanceAwareCompletionListener(id));

        assertEqualsEventually(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id);
            }
        }, Boolean.TRUE);
    }

    @Test
    public void test_inject_hazelcastInstance_to_entryListener_if_itIs_hazelcastInstanceAware() {
        long id1 = generateUniqueHazelcastInjectionId();
        long id2 = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.addCacheEntryListenerConfiguration(
                new HazelcastInstanceAwareCacheEntryListenerConfiguration(id1, id2));

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        cacheManager.createCache(CACHE_NAME, cacheConfig);

        assertEquals("Hazelcast instance has not been injected into entry listener factory!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hazelcast instance has not been injected into entry listener!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_inject_hazelcastInstance_to_partitionLostListener_if_itIs_hazelcastInstanceAware() {
        long id = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.addPartitionLostListener(new HazelcastInstanceAwareCachePartitionLostListener(id));

        assertEquals("Hazelcast instance has not been injected into partition lost listener!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id));
    }

    public static class HazelcastInstanceAwareCacheLoaderFactory
            implements Factory<CacheLoader<Integer, Integer>>, HazelcastInstanceAware {

        private final long id1;
        private final long id2;

        HazelcastInstanceAwareCacheLoaderFactory(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheLoader<Integer, Integer> create() {
            return new HazelcastInstanceAwareCacheLoader(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class HazelcastInstanceAwareCacheLoader implements CacheLoader<Integer, Integer>, HazelcastInstanceAware {

        private final long id2;

        HazelcastInstanceAwareCacheLoader(long id2) {
            this.id2 = id2;
        }

        @Override
        public Integer load(Integer key) throws CacheLoaderException {
            return null;
        }

        @Override
        public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys)
                throws CacheLoaderException {
            return null;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id2, true);
        }

    }

    public static class HazelcastInstanceAwareCacheWriterFactory
            implements Factory<CacheWriter<Integer, Integer>>, HazelcastInstanceAware {

        private final long id1;
        private final long id2;

        HazelcastInstanceAwareCacheWriterFactory(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheWriter<Integer, Integer> create() {
            return new HazelcastInstanceAwareCacheWriter(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class HazelcastInstanceAwareCacheWriter implements CacheWriter<Integer, Integer>, HazelcastInstanceAware {

        private final long id2;

        HazelcastInstanceAwareCacheWriter(long id2) {
            this.id2 = id2;
        }

        @Override
        public void write(Cache.Entry<? extends Integer, ? extends Integer> entry)
                throws CacheWriterException {
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries)
                throws CacheWriterException {
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id2, true);
        }

    }

    public static class HazelcastInstanceAwareExpiryPolicyFactory implements Factory<ExpiryPolicy>, HazelcastInstanceAware {

        private final long id1;
        private final long id2;

        HazelcastInstanceAwareExpiryPolicyFactory(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public ExpiryPolicy create() {
            return new HazelcastInstanceAwareExpiryPolicy(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class HazelcastInstanceAwareExpiryPolicy implements ExpiryPolicy, HazelcastInstanceAware {

        private final long id2;

        HazelcastInstanceAwareExpiryPolicy(long id2) {
            this.id2 = id2;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id2, true);
        }

        @Override
        public Duration getExpiryForCreation() {
            return Duration.ETERNAL;
        }

        @Override
        public Duration getExpiryForAccess() {
            return Duration.ETERNAL;
        }

        @Override
        public Duration getExpiryForUpdate() {
            return Duration.ETERNAL;
        }
    }

    public static class HazelcastInstanceAwareEntryProcessor
            implements EntryProcessor<Integer, Integer, Integer>, HazelcastInstanceAware, Serializable {

        private final long id;

        HazelcastInstanceAwareEntryProcessor(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
                throws EntryProcessorException {
            return null;
        }
    }

    public static class HazelcastInstanceAwareCompletionListener implements CompletionListener, HazelcastInstanceAware {

        private final long id;

        HazelcastInstanceAwareCompletionListener(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void onCompletion() {
        }

        @Override
        public void onException(Exception e) {
        }
    }

    public static class HazelcastInstanceAwareCacheEntryListenerConfiguration
            implements CacheEntryListenerConfiguration<Integer, Integer> {

        private final long id1;
        private final long id2;

        HazelcastInstanceAwareCacheEntryListenerConfiguration(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public Factory<CacheEntryListener<? super Integer, ? super Integer>> getCacheEntryListenerFactory() {
            return new HazelcastInstanceAwareCacheEntryListenerFactory(id1, id2);
        }

        @Override
        public boolean isOldValueRequired() {
            return false;
        }

        @Override
        public Factory<CacheEntryEventFilter<? super Integer, ? super Integer>> getCacheEntryEventFilterFactory() {
            return null;
        }

        @Override
        public boolean isSynchronous() {
            return false;
        }
    }

    public static class HazelcastInstanceAwareCacheEntryListenerFactory
            implements Factory<CacheEntryListener<? super Integer, ? super Integer>>, HazelcastInstanceAware, Serializable {

        private final long id1;
        private final long id2;

        HazelcastInstanceAwareCacheEntryListenerFactory(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheEntryListener<? super Integer, ? super Integer> create() {
            return new HazelcastInstanceAwareCacheEntryListener(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class HazelcastInstanceAwareCacheEntryListener
            implements CacheEntryListener<Integer, Integer>, HazelcastInstanceAware, Serializable {

        private final long id2;

        HazelcastInstanceAwareCacheEntryListener(long id2) {
            this.id2 = id2;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id2, true);
        }
    }

    public static class HazelcastInstanceAwareCachePartitionLostListener
            implements CachePartitionLostListener, HazelcastInstanceAware, Serializable {

        private final long id;

        HazelcastInstanceAwareCachePartitionLostListener(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
        }
    }
}
