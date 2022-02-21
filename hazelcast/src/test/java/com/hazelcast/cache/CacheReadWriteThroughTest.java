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

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
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
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheReadWriteThroughTest extends HazelcastTestSupport {

    protected TestHazelcastInstanceFactory factory;
    protected HazelcastInstance hz;
    protected CachingProvider cachingProvider;

    protected Config createConfig() {
        return new Config();
    }

    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return createServerCachingProvider(instance);
    }

    protected TestHazelcastInstanceFactory createInstanceFactory(int instanceCount) {
        return createHazelcastInstanceFactory(instanceCount);
    }

    protected HazelcastInstance getInstance() {
        // Create master instance
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        // Create second instance
        factory.newHazelcastInstance(createConfig());
        return instance;
    }

    /**
     * Hook for adding additional setup steps in child classes.
     */
    protected void onSetup() {
    }

    @Before
    public void setup() {
        onSetup();
        factory = createInstanceFactory(2);
        hz = getInstance();
        cachingProvider = createCachingProvider(hz);
    }

    /**
     * Hook for adding additional teardown steps in child classes.
     */
    protected void onTearDown() {
    }

    @After
    public void tearDown() {
        onTearDown();
        cachingProvider.close();
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

        CacheManager cacheManager = cachingProvider.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, Integer> config = createCacheConfig();
        config.setReadThrough(true);
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(new GetAllAsyncCacheLoader(false)));

        Cache<Integer, Integer> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 150; i++) {
            keys.add(i);
        }

        Map<Integer, Integer> loaded = cache.getAll(keys);
        assertEquals(100, loaded.size());
    }

    private void loadAll_readThrough(boolean throwError) throws Exception {
        final String cacheName = randomName();

        CacheManager cacheManager = cachingProvider.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, Integer> config = createCacheConfig();
        config.setReadThrough(true);
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(new GetAllAsyncCacheLoader(throwError)));

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

        assertOpenEventually(latch);

        if (!throwError) {
            assertEquals(100, cache.unwrap(ICache.class).size());
        }
    }

    @Test
    public void test_loadAll_readThrough() throws Exception {
        loadAll_readThrough(false);
    }

    @Test
    public void test_loadAll_readThrough_whenThereIsAnThrowableButNotAnException() throws Exception {
        loadAll_readThrough(true);
    }

    public static class GetAllAsyncCacheLoader implements CacheLoader<Integer, Integer>, Serializable {

        private final boolean throwError;

        private GetAllAsyncCacheLoader(boolean throwError) {
            this.throwError = throwError;
        }

        @Override
        public Integer load(Integer key) {
            return key != null && key < 100 ? key : null;
        }

        @Override
        public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            if (throwError) {
                return new HashMap<Integer, Integer>() {
                    @Override
                    public Integer get(Object key) {
                        throw new IllegalAccessError("Bazinga !!!");
                    }
                };
            }
            Map<Integer, Integer> result = new HashMap<Integer, Integer>();
            for (Integer key : keys) {
                Integer value = load(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }
    }

    private void do_putAsAdd_writeThrough(boolean acceptAll) {
        final int ENTRY_COUNT = 100;
        final String cacheName = randomName();

        CacheManager cacheManager = cachingProvider.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        PutCacheWriter putCacheWriter;
        if (acceptAll) {
            putCacheWriter = new PutCacheWriter();
        } else {
            putCacheWriter = new PutCacheWriter(new ModValueChecker(ENTRY_COUNT / 10));
        }
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
    public void test_putAsAdd_writeThrough_allKeysAccepted() {
        do_putAsAdd_writeThrough(true);
    }

    @Test
    public void test_putAsAdd_writeThrough_someKeysBanned() {
        do_putAsAdd_writeThrough(false);
    }

    private void do_putAsUpdate_writeThrough(boolean acceptAll) {
        final int ENTRY_COUNT = 100;
        final String cacheName = randomName();

        CacheManager cacheManager = cachingProvider.getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        PutCacheWriter putCacheWriter;
        if (acceptAll) {
            putCacheWriter = new PutCacheWriter();
        } else {
            putCacheWriter = new PutCacheWriter(new MaxValueChecker(ENTRY_COUNT));
        }
        CacheConfig<Integer, Integer> config = createCacheConfig();
        config.setWriteThrough(true);
        config.setCacheWriterFactory(FactoryBuilder.factoryOf(putCacheWriter));

        ICache<Integer, Integer> cache = cacheManager.createCache(cacheName, config).unwrap(ICache.class);
        assertNotNull(cache);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, i);
        }
        assertEquals(ENTRY_COUNT, cache.size());

        Map<Integer, Integer> keysAndValues = new HashMap<Integer, Integer>();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            int oldValue = cache.get(i);
            int newValue = ENTRY_COUNT + i;
            try {
                cache.put(i, newValue);
                keysAndValues.put(i, newValue);
            } catch (CacheWriterException e) {
                keysAndValues.put(i, oldValue);
            }
        }
        assertEquals(ENTRY_COUNT, cache.size());
        for (int i = 0; i < ENTRY_COUNT; i++) {
            Integer expectedValue = keysAndValues.get(i);
            assertNotNull(expectedValue);

            Integer actualValue = cache.get(i);
            assertNotNull(actualValue);

            assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    public void test_putAsUpdate_writeThrough_allKeysAccepted() {
        do_putAsUpdate_writeThrough(true);
    }

    @Test
    public void test_putAsUpdate_writeThrough_someKeysBanned() {
        do_putAsUpdate_writeThrough(false);
    }

    public static class PutCacheWriter implements CacheWriter<Integer, Integer>, Serializable {

        private final ValueChecker valueChecker;

        private PutCacheWriter() {
            this.valueChecker = null;
        }

        private PutCacheWriter(ValueChecker valueChecker) {
            this.valueChecker = valueChecker;
        }

        private boolean isAcceptableValue(int value) {
            if (valueChecker == null) {
                return true;
            }
            return valueChecker.isAcceptableValue(value);
        }

        @Override
        public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            Integer value = entry.getValue();
            if (!isAcceptableValue(value)) {
                throw new CacheWriterException("Value is invalid: " + value);
            }
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) throws CacheWriterException {
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
        }
    }

    public interface ValueChecker extends Serializable {

        boolean isAcceptableValue(int value);
    }

    public static class ModValueChecker implements ValueChecker {

        private final int mod;

        private ModValueChecker(int mod) {
            this.mod = mod;
        }

        @Override
        public boolean isAcceptableValue(int value) {
            return value % mod != 0;
        }
    }

    public static class MaxValueChecker implements ValueChecker {

        private final int maxValue;

        private MaxValueChecker(int maxValue) {
            this.maxValue = maxValue;
        }

        @Override
        public boolean isAcceptableValue(int value) {
            return value < maxValue;
        }
    }
}
