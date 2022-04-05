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

package com.hazelcast.cache.instance;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.services.NodeAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;

/**
 * Test Node & HazelcastInstance are injected to {@link NodeAware} and {@link HazelcastInstanceAware} cache resources.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheHazelcastInstanceAwareTest extends HazelcastTestSupport {

    private static final ConcurrentMap<Long, Boolean> HAZELCAST_INSTANCE_INJECTION_RESULT_MAP = new ConcurrentHashMap<Long, Boolean>();
    private static final ConcurrentMap<Long, Boolean> NODE_INJECTION_RESULT_MAP = new ConcurrentHashMap<Long, Boolean>();

    private static final String CACHE_NAME = "MyCache";

    @AfterClass
    public static void destroy() {
        HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.clear();
    }

    protected Config createConfig() {
        return new Config();
    }

    protected CacheConfig<Integer, Integer> createCacheConfig(String cacheName) {
        return new CacheConfig<Integer, Integer>(cacheName);
    }

    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return createServerCachingProvider(instance);
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
    public void test_injectDependenciesTo_cacheLoader() {
        long id1 = generateUniqueHazelcastInjectionId();
        long id2 = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setCacheLoaderFactory(new CacheLoaderFactoryWithDependencies(id1, id2));
        cacheConfig.setReadThrough(true);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);
        cache.get(2);

        assertEquals("Hazelcast instance has not been injected into cache loader factory!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hazelcast instance has not been injected into cache loader!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id2));
        assertEquals("Node instance has not been injected into cache loader factory!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Node instance has not been injected into cache loader!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_injectDependenciesTo_cacheWriter() {
        long id1 = generateUniqueHazelcastInjectionId();
        long id2 = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setCacheWriterFactory(new CacheWriterFactoryWithDependencies(id1, id2));
        cacheConfig.setWriteThrough(true);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);

        assertEquals("Hazelcast instance has not been injected into cache writer factory!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hazelcast instance has not been injected into cache writer!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id2));
        assertEquals("Node instance has not been injected into cache writer factory!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Node instance has not been injected into cache writer!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_injectDependenciesTo_expiryPolicy() {
        long id1 = generateUniqueHazelcastInjectionId();
        long id2 = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);
        cacheConfig.setExpiryPolicyFactory(new ExpiryPolicyFactoryWithDependencies(id1, id2));

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.put(1, 1);

        assertEquals("Hazelcast instance has not been injected into expiry policy factory!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Hazelcast instance has not been injected into expiry policy!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id2));
        assertEquals("Node instance has not been injected into expiry policy factory!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id1));
        assertEquals("Node instance has not been injected into expiry policy!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id2));
    }

    @Test
    public void test_injectDependenciesTo__entryProcessor() {
        long id = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.invoke(1, new EntryProcessorWithDependencies(id));

        assertEquals("Hazelcast instance has not been injected into entry processor!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id));

        assertEquals("Node instance has not been injected into entry processor!",
                Boolean.TRUE, NODE_INJECTION_RESULT_MAP.get(id));
    }

    @Test
    public void test_injectDependenciesTo_completionListener() {
        final long id = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.loadAll(new HashSet<Integer>(), false, new CompletionListenerWithDependencies(id));

        assertEqualsEventually(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id);
            }
        }, Boolean.TRUE);
        // Node is only injected on member side completion listener
        if (!ClassLoaderUtil.isClassAvailable(null, "com.hazelcast.client.HazelcastClient")) {
            assertEqualsEventually(new Callable<Boolean>() {
                @Override
                public Boolean call()
                        throws Exception {
                    return NODE_INJECTION_RESULT_MAP.get(id);
                }
            }, Boolean.TRUE);
        }
    }

    @Test
    public void test_injectDependenciesTo__entryListener() {
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
        // Node is only injected on member side entry listeners
        if (!ClassLoaderUtil.isClassAvailable(null, "com.hazelcast.client.HazelcastClient")) {
            assertEquals("Node instance has not been injected into entry listener factory!", Boolean.TRUE,
                    NODE_INJECTION_RESULT_MAP.get(id1));
            assertEquals("Node instance has not been injected into entry listener!", Boolean.TRUE,
                    NODE_INJECTION_RESULT_MAP.get(id2));
        }
    }

    @Test
    public void test_injectDependenciesTo_partitionLostListener() {
        long id = generateUniqueHazelcastInjectionId();
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME);

        HazelcastInstance hazelcastInstance = createInstance();
        CacheManager cacheManager = createCachingProvider(hazelcastInstance).getCacheManager();
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.addPartitionLostListener(new CachePartitionLostListenerWithDependencies(id));

        assertEquals("Hazelcast instance has not been injected into partition lost listener!",
                Boolean.TRUE, HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.get(id));
        // Node is only injected on member side listeners
        if (!ClassLoaderUtil.isClassAvailable(null, "com.hazelcast.client.HazelcastClient")) {
            assertEquals("Node instance has not been injected into partition lost listener!", Boolean.TRUE,
                    NODE_INJECTION_RESULT_MAP.get(id));
        }
    }

    public static class CacheLoaderFactoryWithDependencies
            implements Factory<CacheLoader<Integer, Integer>>, HazelcastInstanceAware, NodeAware {

        private final long id1;
        private final long id2;

        CacheLoaderFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheLoader<Integer, Integer> create() {
            return new CacheLoaderWithDependencies(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class CacheLoaderWithDependencies implements CacheLoader<Integer, Integer>, HazelcastInstanceAware, NodeAware {

        private final long id2;

        CacheLoaderWithDependencies(long id2) {
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

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id2, true);
        }
    }

    public static class CacheWriterFactoryWithDependencies
            implements Factory<CacheWriter<Integer, Integer>>, HazelcastInstanceAware, NodeAware {

        private final long id1;
        private final long id2;

        CacheWriterFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheWriter<Integer, Integer> create() {
            return new CacheWriterWithDependencies(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class CacheWriterWithDependencies
            implements CacheWriter<Integer, Integer>, HazelcastInstanceAware, NodeAware {

        private final long id;

        CacheWriterWithDependencies(long id) {
            this.id = id;
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
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
        }
    }

    public static class ExpiryPolicyFactoryWithDependencies implements Factory<ExpiryPolicy>, HazelcastInstanceAware,
            NodeAware {

        private final long id1;
        private final long id2;

        ExpiryPolicyFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public ExpiryPolicy create() {
            return new ExpiryPolicyWithDependencies(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id1, true);
        }
    }

    public static class ExpiryPolicyWithDependencies implements ExpiryPolicy, HazelcastInstanceAware, NodeAware {

        private final long id;

        ExpiryPolicyWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
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

    public static class EntryProcessorWithDependencies
            implements EntryProcessor<Integer, Integer, Integer>, HazelcastInstanceAware, NodeAware, Serializable {

        private final long id;

        EntryProcessorWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
                throws EntryProcessorException {
            return null;
        }
    }

    public static class CompletionListenerWithDependencies implements CompletionListener, HazelcastInstanceAware, NodeAware {

        private final long id;

        CompletionListenerWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.put(id, true);
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
            return new CacheEntryListenerFactoryWithDependencies(id1, id2);
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

    public static class CacheEntryListenerFactoryWithDependencies
            implements Factory<CacheEntryListener<? super Integer, ? super Integer>>, HazelcastInstanceAware,
            NodeAware, Serializable {

        private final long id1;
        private final long id2;

        CacheEntryListenerFactoryWithDependencies(long id1, long id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        @Override
        public CacheEntryListener<? super Integer, ? super Integer> create() {
            return new CacheEntryListenerWithDependencies(id2);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id1, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.putIfAbsent(id1, true);
        }
    }

    public static class CacheEntryListenerWithDependencies
            implements CacheEntryListener<Integer, Integer>, HazelcastInstanceAware, NodeAware, Serializable {

        private final long id;

        CacheEntryListenerWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.putIfAbsent(id, true);
        }
    }

    public static class CachePartitionLostListenerWithDependencies
            implements CachePartitionLostListener, HazelcastInstanceAware, NodeAware, Serializable {

        private final long id;

        CachePartitionLostListenerWithDependencies(long id) {
            this.id = id;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            HAZELCAST_INSTANCE_INJECTION_RESULT_MAP.put(id, true);
        }

        @Override
        public void setNode(Node node) {
            NODE_INJECTION_RESULT_MAP.putIfAbsent(id, true);
        }

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
        }
    }
}
