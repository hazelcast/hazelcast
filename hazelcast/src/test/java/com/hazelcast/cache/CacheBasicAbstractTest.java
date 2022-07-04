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
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.SampleableConcurrentHashMap;
import org.junit.Assert;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.logging.Logger.getLogger;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class CacheBasicAbstractTest extends CacheTestSupport {

    @Test
    public void testPutGetRemoveReplace() {
        ICache<String, String> cache = createCache();

        cache.put("key1", "value1");
        assertEquals("value1", cache.get("key1"));

        assertEquals("value1", cache.getAndPut("key1", "value2"));
        assertEquals(1, cache.size());

        assertTrue(cache.remove("key1"));

        cache.put("key1", "value3");
        assertFalse(cache.remove("key1", "xx"));
        assertTrue(cache.remove("key1", "value3"));
        assertNull(cache.get("key1"));

        assertTrue(cache.putIfAbsent("key1", "value1"));
        assertFalse(cache.putIfAbsent("key1", "value1"));
        assertEquals("value1", cache.getAndRemove("key1"));
        assertNull(cache.get("key1"));

        cache.put("key1", "value1");
        assertTrue(cache.containsKey("key1"));

        assertFalse(cache.replace("key2", "value2"));
        assertTrue(cache.replace("key1", "value2"));
        assertEquals("value2", cache.get("key1"));

        assertFalse(cache.replace("key1", "xx", "value3"));
        assertTrue(cache.replace("key1", "value2", "value3"));
        assertEquals("value3", cache.get("key1"));

        assertEquals("value3", cache.getAndReplace("key1", "value4"));
        assertEquals("value4", cache.get("key1"));
    }

    @Test
    public void testAsyncGetPutRemove() throws Exception {
        final ICache<String, String> cache = createCache();
        final String key = "key";

        cache.put(key, "value1");
        CompletionStage<?> future = cache.getAsync(key);
        assertEquals("value1", future.toCompletableFuture().get());

        cache.putAsync(key, "value2");
        assertTrueEventually(() -> assertEquals("value2", cache.get(key)));

        future = cache.getAndPutAsync(key, "value3");
        assertEquals("value2", future.toCompletableFuture().get());
        assertEquals("value3", cache.get(key));

        future = cache.removeAsync("key2");
        assertFalse((Boolean) future.toCompletableFuture().get());
        future = cache.removeAsync(key);
        assertTrue((Boolean) future.toCompletableFuture().get());

        cache.put(key, "value4");
        future = cache.getAndRemoveAsync("key2");
        assertNull(future.toCompletableFuture().get());
        future = cache.getAndRemoveAsync(key);
        assertEquals("value4", future.toCompletableFuture().get());
    }

    @Test
    public void testPutIfAbsentAsync_success() throws Exception {
        ICache<String, String> cache = createCache();
        String key = randomString();

        CompletionStage<Boolean> stage = cache.putIfAbsentAsync(key, randomString());
        assertTrue(stage.toCompletableFuture().get());
    }

    @Test
    public void testPutIfAbsentAsync_fail() throws Exception {
        ICache<String, String> cache = createCache();
        String key = randomString();
        cache.put(key, randomString());

        CompletionStage<Boolean> stage = cache.putIfAbsentAsync(key, randomString());
        assertFalse(stage.toCompletableFuture().get());
    }

    @Test
    public void testPutIfAbsentAsync_withExpiryPolicy() {
        final ICache<String, String> cache = createCache();
        final String key = randomString();
        HazelcastExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1);

        cache.putIfAbsentAsync(key, randomString(), expiryPolicy);
        assertTrueEventually(() -> assertNull(cache.get(key)));
    }

    @Test
    public void testGetAndReplaceAsync() throws Exception {
        ICache<String, String> cache = createCache();
        String key = randomString();
        String oldValue = randomString();
        String newValue = randomString();
        cache.put(key, oldValue);

        CompletionStage<String> stage = cache.getAndReplaceAsync(key, newValue);
        assertEquals(stage.toCompletableFuture().get(), oldValue);
        assertEquals(cache.get(key), newValue);
    }

    @Test
    public void testGetAll_withEmptySet() {
        ICache<String, String> cache = createCache();
        Map<String, String> map = cache.getAll(Collections.emptySet());
        assertEquals(0, map.size());
    }

    @Test
    public void testClear() {
        ICache<String, String> cache = createCache();
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, "value" + i);
        }
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void testRemoveAll() {
        ICache<String, String> cache = createCache();
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, "value" + i);
        }
        cache.removeAll();
        assertEquals(0, cache.size());
    }

    @Test
    public void testPutWithTtl() throws Exception {
        final ICache<String, String> cache = createCache();
        final String key = "key";
        cache.put(key, "value1", ttlToExpiryPolicy(1, TimeUnit.SECONDS));

        assertTrueEventually(() -> assertNull(cache.get(key)));
        assertEquals(0, cache.size());

        cache.putAsync(key, "value1", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertTrueEventually(() -> assertNull(cache.get(key)));
        assertEquals(0, cache.size());

        cache.put(key, "value2");
        String value = cache.getAndPut(key, "value3", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertEquals("value2", value);
        assertTrueEventually(() -> assertNull(cache.get(key)));
        assertEquals(0, cache.size());

        cache.put(key, "value4");
        CompletionStage<String> stage = cache.getAndPutAsync(key, "value5", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertEquals("value4", stage.toCompletableFuture().get());

        assertTrueEventually(() -> assertNull(cache.get(key)));
        assertEquals(0, cache.size());
    }

    @SuppressWarnings("SameParameterValue")
    private ExpiryPolicy ttlToExpiryPolicy(long ttl, TimeUnit timeUnit) {
        return new ModifiedExpiryPolicy(new Duration(timeUnit, ttl));
    }

    @Test
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testIterator() {
        ICache<Integer, Integer> cache = createCache();
        int size = 1111;
        int multiplier = 11;
        for (int i = 0; i < size; i++) {
            cache.put(i, i * multiplier);
        }

        int[] keys = new int[size];
        int keyIndex = 0;
        Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
        while (iterator.hasNext()) {
            Cache.Entry<Integer, Integer> e = iterator.next();
            int key = e.getKey();
            int value = e.getValue();
            assertEquals(key * multiplier, value);
            keys[keyIndex++] = key;
        }
        assertEquals(size, keyIndex);

        Arrays.sort(keys);
        for (int i = 0; i < size; i++) {
            assertEquals(i, keys[i]);
        }
    }

    @Test
    public void testExpiration() {
        CacheConfig<Integer, String> config = new CacheConfig<>();
        final CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String> listener =
                new CacheFromDifferentNodesTest.SimpleEntryListener<>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<>(
                        FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);
        config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new HazelcastExpiryPolicy(100, 100, 100)));

        Cache<Integer, String> instanceCache = createCache(config);

        instanceCache.put(1, "value");

        assertTrueEventually(() -> assertEquals(1, listener.expired.get()));
    }

    @Test
    public void testExpiration_entryWithOwnTtl() {
        CacheConfig<Integer, String> config = new CacheConfig<>();
        final CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String> listener
                = new CacheFromDifferentNodesTest.SimpleEntryListener<>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration
                = new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);
        config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new HazelcastExpiryPolicy(Duration.ETERNAL, Duration.ETERNAL,
                Duration.ETERNAL)));

        ICache<Integer, String> instanceCache = createCache(config);

        instanceCache.put(1, "value", ttlToExpiryPolicy(1, TimeUnit.SECONDS));

        assertTrueEventually(() -> assertEquals(1, listener.expired.get()));
    }

    @Test
    public void testIteratorRemove() {
        ICache<Integer, Integer> cache = createCache();
        int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        assertEquals(0, cache.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testIteratorIllegalRemove() {
        ICache<Integer, Integer> cache = createCache();
        int size = 10;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
        if (iterator.hasNext()) {
            iterator.remove();
        }
    }

    @Test
    public void testIteratorDuringInsertion_withoutEviction() {
        testIteratorDuringInsertion(true);
    }

    @Test
    public void testIteratorDuringInsertion_withEviction() {
        testIteratorDuringInsertion(false);
    }

    @SuppressWarnings("WhileLoopReplaceableByForEach")
    private void testIteratorDuringInsertion(boolean withoutEviction) {
        final int MAX_SIZE = withoutEviction ? 1000000 : 1000;
        final CacheConfig<Integer, Integer> config = getCacheConfigWithMaxSize(MAX_SIZE);
        final ICache<Integer, Integer> cache = createCache(config);
        final int maxSize = withoutEviction
                ? getMaxCacheSizeWithoutEviction(config)
                : getMaxCacheSizeWithEviction(config);

        // we prefill the cache with half of the max size, so there will be new inserts from the AbstractCacheWorker
        int prefillSize = maxSize / 2;
        for (int i = 0; i < prefillSize; i++) {
            cache.put(i, i);
        }

        AbstractCacheWorker worker = new AbstractCacheWorker() {
            @Override
            void doRun(Random random) {
                int i = random.nextInt(maxSize);
                cache.put(i, i);
            }
        };
        worker.awaitFirstIteration();

        try {
            int i = 0;
            Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
            while (iterator.hasNext()) {
                Cache.Entry<Integer, Integer> e = iterator.next();
                Integer key = e.getKey();
                Integer value = e.getValue();
                if (value != null) {
                    // value can be null in case prefetchValues is false
                    // and the entry expired between the time the key was fetched
                    // and the value was fetched
                    assertEquals(key, value);
                }
                i++;
            }
            if (withoutEviction) {
                assertTrue("should have iterated over at least " + prefillSize + " entries, but was " + i, i >= prefillSize);
                assertThatNoCacheEvictionHappened(cache);
            }
        } catch (NoSuchElementException e) {
            if (withoutEviction) {
                fail("Without eviction, there should not be `NoSuchElementException`: " + e);
            }
        } finally {
            worker.shutdown();
        }
    }

    @Test
    public void testIteratorDuringUpdate_withoutEviction() {
        testIteratorDuringUpdate(true);
    }

    @Test
    public void testIteratorDuringUpdate_withEviction() {
        testIteratorDuringUpdate(false);
    }

    @SuppressWarnings("WhileLoopReplaceableByForEach")
    private void testIteratorDuringUpdate(boolean withoutEviction) {
        final int MAX_SIZE = withoutEviction ? 1000000 : 1000;
        final CacheConfig<Integer, Integer> config = getCacheConfigWithMaxSize(MAX_SIZE);
        final ICache<Integer, Integer> cache = createCache(config);
        final int maxSize = withoutEviction
                ? getMaxCacheSizeWithoutEviction(config)
                : getMaxCacheSizeWithEviction(config);

        for (int i = 0; i < maxSize; i++) {
            cache.put(i, i);
        }

        AbstractCacheWorker worker = new AbstractCacheWorker() {
            @Override
            void doRun(Random random) {
                int i = random.nextInt(maxSize);
                cache.put(i, -i);
            }
        };
        worker.awaitFirstIteration();

        try {
            int i = 0;
            Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
            while (iterator.hasNext()) {
                Cache.Entry<Integer, Integer> e = iterator.next();
                int key = e.getKey();
                Integer value = e.getValue();
                if (value != null) {
                    // value can be null in case prefetchValues is false
                    // and the entry expired between the time the key was fetched
                    // and the value was fetched
                    assertEquals("key: " + key + ", value: " + value, key, Math.abs(value));
                }
                i++;
            }
            if (withoutEviction) {
                assertEquals("should have iterated over all " + maxSize + " entries", maxSize, i);
                assertThatNoCacheEvictionHappened(cache);
            }
        } catch (NoSuchElementException e) {
            if (withoutEviction) {
                fail("Without eviction, there should not be `NoSuchElementException`: " + e);
            }
        } finally {
            worker.shutdown();
        }
    }

    @Test
    public void testIteratorDuringRemoval_withoutEviction() {
        testIteratorDuringRemoval(true);
    }

    @Test
    public void testIteratorDuringRemoval_withEviction() {
        testIteratorDuringRemoval(false);
    }

    @SuppressWarnings("WhileLoopReplaceableByForEach")
    private void testIteratorDuringRemoval(boolean withoutEviction) {
        final int MAX_SIZE = withoutEviction ? 1000000 : 1000;
        final CacheConfig<Integer, Integer> config = getCacheConfigWithMaxSize(MAX_SIZE);
        final ICache<Integer, Integer> cache = createCache(config);
        final int maxSize = withoutEviction
                ? getMaxCacheSizeWithoutEviction(config)
                : getMaxCacheSizeWithEviction(config);

        for (int i = 0; i < maxSize; i++) {
            cache.put(i, i);
        }

        AbstractCacheWorker worker = new AbstractCacheWorker() {
            @Override
            void doRun(Random random) {
                int i = random.nextInt(maxSize);
                cache.remove(i);
            }
        };
        worker.awaitFirstIteration();

        try {
            int i = 0;
            Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
            while (iterator.hasNext()) {
                Cache.Entry<Integer, Integer> e = iterator.next();
                Integer key = e.getKey();
                Integer value = e.getValue();
                if (value != null) {
                    // value can be null in case prefetchValues is false
                    // and the entry expired between the time the key was fetched
                    // and the value was fetched
                    assertEquals(key, value);
                }
                i++;
            }
            if (withoutEviction) {
                assertTrue("should have iterated over at most " + maxSize + " entries, but was " + i, i <= maxSize);
                assertThatNoCacheEvictionHappened(cache);
            }
        } catch (NoSuchElementException e) {
            // `NoSuchElementException` is expected because while iterating over entries,
            // last element might be removed by worker thread in the test and since there is no more element,
            // `NoSuchElementException` is thrown which is normal behaviour.
        } finally {
            worker.shutdown();
        }
    }

    @Test
    public void testRemoveAsync() {
        ICache<String, String> cache = createCache();
        String key = randomString();
        String value = randomString();

        cache.put(key, value);
        CompletionStage<Boolean> stage = cache.removeAsync(key);
        stage.thenAccept(Assert::assertTrue);

        stage.toCompletableFuture().join();
    }

    @Test
    public void testRemoveAsyncWhenEntryNotFound() {
        ICache<String, String> cache = createCache();

        cache.removeAsync(randomString()).thenAccept(Assert::assertFalse).toCompletableFuture().join();
    }

    @Test
    public void testRemoveAsync_withOldValue() {
        ICache<String, String> cache = createCache();
        String key = randomString();
        String value = randomString();

        cache.put(key, value);
        cache.removeAsync(key, value)
             .thenAccept(Assert::assertTrue)
             .toCompletableFuture().join();
    }

    @Test
    public void testRemoveAsyncWhenEntryNotFound_withOldValue() {
        ICache<String, String> cache = createCache();

        cache.removeAsync(randomString(), randomString())
             .thenAccept(Assert::assertFalse)
             .toCompletableFuture().join();
    }

    @Test
    public void testGetCacheWithNullName() {
        assertNull(cacheManager.getCache(null));
    }

    @Test
    public void testRemovingSameEntryTwiceShouldTriggerEntryListenerOnlyOnce() {
        String cacheName = randomString();

        CacheConfig<Integer, String> config = createCacheConfig();
        final CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String> listener
                = new CacheFromDifferentNodesTest.SimpleEntryListener<>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration
                = new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key = 1;
        String value = "value";
        cache.put(key, value);
        assertTrueEventually(() -> assertEquals(1, listener.created.get()));

        cache.removeAll();
        cache.removeAll();

        assertTrueEventually(() -> assertEquals(1, listener.removed.get()));
    }

    @Test
    public void testCompletionEvent() {
        String cacheName = randomString();

        CacheConfig<Integer, String> config = createCacheConfig();
        final CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String> listener
                = new CacheFromDifferentNodesTest.SimpleEntryListener<>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration
                = new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key1 = 1;
        String value1 = "value1";
        cache.put(key1, value1);
        assertTrueEventually(() -> assertEquals(1, listener.created.get()));

        Integer key2 = 2;
        String value2 = "value2";
        cache.put(key2, value2);
        assertTrueEventually(() -> assertEquals(2, listener.created.get()));

        Set<Integer> keys = new HashSet<>();
        keys.add(key1);
        keys.add(key2);
        cache.removeAll(keys);
        assertTrueEventually(() -> assertEquals(2, listener.removed.get()));
    }

    @Test
    public void testJSRCreateDestroyCreate() {
        String cacheName = randomString();

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer, String> config = new CacheConfig<>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key = 1;
        String value1 = "value";
        cache.put(key, value1);

        String value2 = cache.get(key);
        assertEquals(value1, value2);
        cache.remove(key);
        assertNull(cache.get(key));

        cacheManager.destroyCache(cacheName);
        assertNull(cacheManager.getCache(cacheName));

        Cache<Integer, String> cacheAfterDestroy = cacheManager.createCache(cacheName, config);
        assertNotNull(cacheAfterDestroy);
    }

    @Test
    public void testCaches_NotEmpty() {
        ArrayList<String> expected = new ArrayList<>();
        ArrayList<String> real = new ArrayList<>();
        cacheManager.createCache("c1", new MutableConfiguration());
        cacheManager.createCache("c2", new MutableConfiguration());
        cacheManager.createCache("c3", new MutableConfiguration());
        expected.add(cacheManager.getCache("c1").getName());
        expected.add(cacheManager.getCache("c2").getName());
        expected.add(cacheManager.getCache("c3").getName());

        Iterable<String> cacheNames = cacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            real.add(cacheName);
        }
        expected.removeAll(real);
        assertTrue(expected.isEmpty());
    }

    @Test
    public void testInitableIterator() {
        int testSize = 3007;
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        for (int fetchSize = 1; fetchSize < 102; fetchSize++) {
            SampleableConcurrentHashMap<Data, String> map = new SampleableConcurrentHashMap<>(1000);
            for (int i = 0; i < testSize; i++) {
                Integer key = i;
                Data data = serializationService.toData(key);
                String value1 = "value" + i;
                map.put(data, value1);
            }

            IterationPointer[] pointers = {new IterationPointer(Integer.MAX_VALUE, -1)};
            int total = 0;
            int remaining = testSize;
            while (remaining > 0 && pointers[pointers.length - 1].getIndex() > 0) {
                int size = (Math.min(remaining, fetchSize));
                List<Data> keys = new ArrayList<>(size);
                pointers = map.fetchKeys(pointers, size, keys);
                remaining -= keys.size();
                total += keys.size();
            }
            assertEquals(testSize, total);
        }
    }

    @Test
    public void testCachesTypedConfig() {
        CacheConfig<Integer, Long> config = createCacheConfig();
        String cacheName = randomString();
        config.setName(cacheName);
        config.setTypes(Integer.class, Long.class);

        Cache<Integer, Long> cache = cacheManager.createCache(cacheName, config);
        Cache<Integer, Long> cache2 = cacheManager.getCache(cacheName, config.getKeyType(), config.getValueType());

        assertNotNull(cache);
        assertNotNull(cache2);
    }

    @Test
    public void getAndOperateOnCacheAfterClose() {
        String cacheName = randomString();
        ICache<Integer, Integer> cache = createCache(cacheName);

        cache.close();
        assertTrue("The cache should be closed", cache.isClosed());
        assertFalse("The cache should be destroyed", cache.isDestroyed());

        Cache<Object, Object> cacheAfterClose = cacheManager.getCache(cacheName);
        assertNotNull("Expected cacheAfterClose not to be null", cacheAfterClose);
        assertFalse("The cacheAfterClose should not be closed", cacheAfterClose.isClosed());

        cache.put(1, 1);
    }

    @Test
    public void getButCantOperateOnCacheAfterDestroy() {
        String cacheName = randomString();
        ICache<Integer, Integer> cache = createCache(cacheName);

        cache.destroy();
        assertTrue("The cache should be closed", cache.isClosed());
        assertTrue("The cache should be destroyed", cache.isDestroyed());

        Cache<Object, Object> cacheAfterDestroy = cacheManager.getCache(cacheName);
        assertNull("Expected cacheAfterDestroy to be null", cacheAfterDestroy);

        try {
            cache.put(1, 1);
            fail("Since the cache is destroyed, an operation on the cache has to fail with 'IllegalStateException'");
        } catch (IllegalStateException expected) {
            // expect this exception since cache is closed and destroyed
        } catch (Throwable t) {
            t.printStackTrace();
            fail("Since the cache is destroyed, an operation on the cache has to fail with 'IllegalStateException',"
                    + " but failed with " + t.getMessage());
        }

        assertTrue("The existing cache should still be closed", cache.isClosed());
        assertTrue("The existing cache should still be destroyed", cache.isDestroyed());
    }

    @Test
    public void testEntryProcessor_invoke() {
        ICache<String, String> cache = createCache();
        String value = randomString();
        String key = randomString();
        String postFix = randomString();
        cache.put(key, value);
        String result = cache.invoke(key, new AppendEntryProcessor(), postFix);
        String expectedResult = value + postFix;
        assertEquals(expectedResult, result);
        assertEquals(expectedResult, cache.get(key));
    }

    @Test
    public void testEntryProcessor_invokeAll() {
        ICache<String, String> cache = createCache();
        int entryCount = 10;
        Map<String, String> localMap = new HashMap<>();
        for (int i = 0; i < entryCount; i++) {
            localMap.put(randomString(), randomString());
        }
        cache.putAll(localMap);
        String postFix = randomString();
        Map<String, EntryProcessorResult<String>> resultMap
                = cache.invokeAll(localMap.keySet(), new AppendEntryProcessor(), postFix);
        for (Map.Entry<String, String> localEntry : localMap.entrySet()) {
            EntryProcessorResult<String> entryProcessorResult = resultMap.get(localEntry.getKey());
            assertEquals(localEntry.getValue() + postFix, entryProcessorResult.get());
            assertEquals(localEntry.getValue() + postFix, cache.get(localEntry.getKey()));
        }
    }

    public static class AppendEntryProcessor implements EntryProcessor<String, String, String>, Serializable {

        private static final long serialVersionUID = -396575576353368113L;

        @Override
        public String process(MutableEntry<String, String> entry, Object... arguments) throws EntryProcessorException {
            String postFix = (String) arguments[0];
            String value = entry.getValue();
            String result = value + postFix;
            entry.setValue(result);
            return result;
        }
    }

    private abstract static class AbstractCacheWorker {

        private final Random random = new Random();
        private final CountDownLatch firstIterationDone = new CountDownLatch(1);
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final CacheWorkerThread thread = new CacheWorkerThread();

        AbstractCacheWorker() {
            thread.start();
        }

        void awaitFirstIteration() {
            try {
                firstIterationDone.await();
            } catch (InterruptedException e) {
                fail("CacheWorkerThread did not start! " + e.getMessage());
            }
        }

        public void shutdown() {
            isRunning.set(false);
            try {
                thread.join();
            } catch (InterruptedException e) {
                fail("CacheWorkerThread did not stop! " + e.getMessage());
            }
        }

        private class CacheWorkerThread extends Thread {

            public void run() {
                try {
                    doRun(random);
                    LockSupport.parkNanos(1);
                } catch (Exception e) {
                    ignore(e);
                }
                firstIterationDone.countDown();

                while (isRunning.get()) {
                    try {
                        doRun(random);
                        LockSupport.parkNanos(1);
                    } catch (Exception e) {
                        ignore(e);
                    }
                }
            }
        }

        abstract void doRun(Random random);
    }

    // https://github.com/hazelcast/hazelcast/issues/7236
    @Test
    public void expiryTimeShouldNotBeChangedOnUpdateWhenCreatedExpiryPolicyIsUsed() {
        final int createdExpiryTimeMillis = 100;

        Duration duration = new Duration(TimeUnit.MILLISECONDS, createdExpiryTimeMillis);
        CacheConfig<Integer, String> cacheConfig = new CacheConfig<>();
        cacheConfig.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(duration));

        Cache<Integer, String> cache = createCache(cacheConfig);
        cache.put(1, "value");
        cache.put(1, "value");

        sleepAtLeastMillis(createdExpiryTimeMillis + 1);

        assertNull(cache.get(1));
    }

    @Test
    public void testSetExpiryPolicyReturnsTrue() {
        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        assertTrue(cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.FIVE_MINUTES)));
    }

    @Test
    public void testSetExpiryPolicyReturnsFalse_whenKeyDoesNotExist() {
        ICache<Integer, String> cache = createCache();
        assertFalse(cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.FIVE_MINUTES)));
    }

    @Test
    public void testSetExpiryPolicyReturnsFalse_whenKeyIsAlreadyExpired() {
        ICache<Integer, String> cache = createCache();
        cache.put(1, "value", new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1)));
        sleepAtLeastSeconds(2);
        assertFalse(cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.FIVE_MINUTES)));
    }

    @Test
    public void testRecordExpiryPolicyTakesPrecedenceOverCachePolicy() {
        final int updatedTtlMillis = 1000;

        CacheConfig<Integer, String> cacheConfig = new CacheConfig<>();
        cacheConfig.setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(Duration.ONE_DAY));

        ICache<Integer, String> cache = createCache(cacheConfig);
        cache.put(1, "value");
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, updatedTtlMillis)));

        sleepAtLeastMillis(updatedTtlMillis + 1);

        assertNull(cache.get(1));
    }

    @Test
    public void testRecordExpiryPolicyTakesPrecedenceOverPolicyAtCreation() {
        final int updatedTtlMillis = 1000;

        ICache<Integer, String> cache = createCache();
        cache.put(1, "value", new TouchedExpiryPolicy(Duration.ONE_DAY));
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, updatedTtlMillis)));

        sleepAtLeastMillis(updatedTtlMillis + 1);

        assertNull(cache.get(1));
    }

    @Test
    public void testRecordExpiryPolicyTakesPrecedence() {
        final int ttlMillis = 1000;
        Duration modifiedDuration = new Duration(TimeUnit.MILLISECONDS, ttlMillis);

        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        cache.setExpiryPolicy(1, TouchedExpiryPolicy.factoryOf(modifiedDuration).create());

        sleepAtLeastMillis(ttlMillis + 1);

        assertNull(cache.get(1));
    }

    @Test
    public void test_whenExpiryPolicyIsOverridden_thenNewPolicyIsInEffect() {
        final int ttlMillis = 1000;

        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttlMillis)));
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.ETERNAL));

        sleepAtLeastMillis(ttlMillis + 1);

        assertEquals("value", cache.get(1));
    }

    @Test
    public void test_CustomExpiryPolicyIsUsedWhenEntryIsUpdated() {
        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        cache.setExpiryPolicy(1, new HazelcastExpiryPolicy(10000, 10000, 10000));
        sleepAtLeastSeconds(5);
        cache.put(1, "value2");
        sleepAtLeastSeconds(5);

        assertEquals("value2", cache.get(1));
    }

    @Test
    public void removeCacheFromOwnerCacheManagerWhenCacheIsDestroyed() {
        ICache<?, ?> cache = createCache();
        assertTrue("Expected the cache to be found in the CacheManager", cacheExistsInCacheManager(cache));

        cache.destroy();
        assertFalse("Expected the cache not to be found in the CacheManager", cacheExistsInCacheManager(cache));
    }

    private boolean cacheExistsInCacheManager(ICache<?, ?> cache) {
        for (String cacheName : cacheManager.getCacheNames()) {
            if (cacheName.equals(cache.getName())) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void entryShouldNotBeExpiredWhenTimeUnitIsNullAndDurationIsZero() {
        Duration duration = new Duration(null, 0);
        CacheConfig<Integer, String> cacheConfig = new CacheConfig<>();
        cacheConfig.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(duration));

        Cache<Integer, String> cache = createCache(cacheConfig);
        cache.put(1, "value");

        sleepAtLeastMillis(1);

        assertNotNull(cache.get(1));
    }

    @Test(expected = IllegalStateException.class)
    public void cacheManagerOfCache_cannotBeOverwritten() throws Exception {
        Properties properties = HazelcastCachingProvider.propertiesByInstanceItself(getHazelcastInstance());
        CacheManager cacheManagerFooURI = cachingProvider.getCacheManager(new URI("foo"), null, properties);
        CacheManager cacheManagerFooClassLoader = cachingProvider.getCacheManager(null,
                new MaliciousClassLoader(CacheBasicAbstractTest.class.getClassLoader()), properties);
        CacheConfig<?, ?> cacheConfig = new CacheConfig<>("the-cache");
        Cache<?, ?> cache1 = cacheManagerFooURI.createCache("the-cache", cacheConfig);
        // cache1.cacheManager is cacheManagerFooURI
        assertEquals(cacheManagerFooURI, cache1.getCacheManager());

        // assert one can still get the cache
        Cache<?, ?> cache2 = cacheManagerFooURI.getCache("the-cache");
        assertEquals(cache1, cache2);

        // attempt to overwrite existing cache1's CacheManager -> fails with IllegalStateException
        cacheManagerFooClassLoader.getCache("the-cache");
    }

    @Test
    public void cacheManagerOfCache_cannotBeOverwrittenConcurrently() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger illegalStateExceptionCount = new AtomicInteger();

        Properties properties = HazelcastCachingProvider.propertiesByInstanceItself(getHazelcastInstance());
        final CacheManager cacheManagerFooURI = cachingProvider.getCacheManager(new URI("foo"), null, properties);
        final CacheManager cacheManagerFooClassLoader = cachingProvider.getCacheManager(null,
                new MaliciousClassLoader(CacheBasicAbstractTest.class.getClassLoader()), properties);
        Future<?> createCacheFromFooURI = spawn(
                () -> createCacheConcurrently(latch, cacheManagerFooURI, illegalStateExceptionCount));

        Future<?> createCacheFromFooClassLoader = spawn(
                () -> createCacheConcurrently(latch, cacheManagerFooClassLoader, illegalStateExceptionCount));

        latch.countDown();

        try {
            createCacheFromFooURI.get();
            createCacheFromFooClassLoader.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof IllegalStateException)) {
                getLogger(CacheBasicAbstractTest.class).severe("Unexpected exception", cause);
                throw new AssertionError(format("Unexpected exception thrown: %s", cause.getMessage()));
            }
        }

        // one of two failed
        assertEquals(1, illegalStateExceptionCount.get());
    }

    private static void createCacheConcurrently(CountDownLatch latch, CacheManager cacheManager,
                                                AtomicInteger illegalStateExceptionCount) {
        try {
            latch.await();
            CacheConfig<?, ?> cacheConfig = new CacheConfig<>("the-cache");
            cacheManager.createCache("the-cache", cacheConfig);
        } catch (IllegalStateException e) {
            illegalStateExceptionCount.incrementAndGet();
            throw rethrow(e);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public static class MaliciousClassLoader extends ClassLoader {

        MaliciousClassLoader(ClassLoader parent) {
            super(parent);
        }

        @Override
        public String toString() {
            return "foo";
        }
    }
}
