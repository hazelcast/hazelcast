package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.util.CacheConcurrentHashMap;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class CacheBasicAbstractTest extends CacheTestSupport {

    @Test
    public void testPutGetRemoveReplace() throws InterruptedException, ExecutionException {
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
    public void testAsyncGetPutRemove() throws InterruptedException, ExecutionException {
        final ICache<String, String> cache = createCache();
        final String key = "key";
        cache.put(key, "value1");
        Future future = cache.getAsync(key);
        assertEquals("value1", future.get());

        cache.putAsync(key, "value2");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("value2", cache.get(key));
            }
        });

        future = cache.getAndPutAsync(key, "value3");
        assertEquals("value2", future.get());
        assertEquals("value3", cache.get(key));

        future = cache.removeAsync("key2");
        assertFalse((Boolean) future.get());
        future = cache.removeAsync(key);
        assertTrue((Boolean) future.get());

        cache.put(key, "value4");
        future = cache.getAndRemoveAsync("key2");
        assertNull(future.get());
        future = cache.getAndRemoveAsync(key);
        assertEquals("value4", future.get());
    }

    @Test
    public void testPutIfAbsentAsync_success() throws InterruptedException, ExecutionException {
        ICache<String, String> cache = createCache();
        String key = randomString();
        ICompletableFuture<Boolean> iCompletableFuture = cache.putIfAbsentAsync(key, randomString());
        assertTrue(iCompletableFuture.get());
    }

    @Test
    public void testPutIfAbsentAsync_fail() throws InterruptedException, ExecutionException {
        ICache<String, String> cache = createCache();
        String key = randomString();
        cache.put(key, randomString());
        ICompletableFuture<Boolean> iCompletableFuture = cache.putIfAbsentAsync(key, randomString());
        assertFalse(iCompletableFuture.get());
    }

    @Test
    public void testPutIfAbsentAsync_withExpiryPolicy() throws InterruptedException, ExecutionException {
        final ICache<String, String> cache = createCache();
        final String key = randomString();
        HazelcastExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1);
        cache.putIfAbsentAsync(key, randomString(), expiryPolicy);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
    }

    @Test
    public void testGetAndReplaceAsync() throws InterruptedException, ExecutionException {
        ICache<String, String> cache = createCache();
        String key = randomString();
        String oldValue = randomString();
        String newValue = randomString();
        cache.put(key, oldValue);

        ICompletableFuture<String> iCompletableFuture = cache.getAndReplaceAsync(key, newValue);

        assertEquals(iCompletableFuture.get(), oldValue);
        assertEquals(cache.get(key), newValue);
    }

    @Test
    public void testGetAll_withEmptySet() throws InterruptedException, ExecutionException {
        ICache<String, String> cache = createCache();
        Map<String, String> map = cache.getAll(Collections.<String>emptySet());
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

    protected ExpiryPolicy ttlToExpiryPolicy(long ttl, TimeUnit timeUnit) {
        return new ModifiedExpiryPolicy(new Duration(timeUnit, ttl));
    }

    @Test
    public void testPutWithTtl() throws ExecutionException, InterruptedException {
        final ICache<String, String> cache = createCache();
        final String key = "key";
        cache.put(key, "value1", ttlToExpiryPolicy(1, TimeUnit.SECONDS));

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());

        cache.putAsync(key, "value1", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());

        cache.put(key, "value2");
        String value = cache.getAndPut(key, "value3", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertEquals("value2", value);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());

        cache.put(key, "value4");
        Future future = cache.getAndPutAsync(key, "value5", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertEquals("value4", future.get());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());
    }

    @Test
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
    public void testIteratorDuringInsertion() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache<Integer, Integer> cache = createCache();
        int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Thread thread = new Thread() {
            public void run() {
                Random rand = new Random();
                while (!stop.get()) {
                    int i = rand.nextInt();
                    try {
                        cache.put(i, i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

        // Give chance to thread for starting
        sleepSeconds(1);

        try {
            int i = 0;
            Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
            while (iterator.hasNext()) {
                Cache.Entry<Integer, Integer> e = iterator.next();
                int key = e.getKey();
                int value = e.getValue();
                assertEquals(key, value);
                i++;
            }
            assertTrue(i >= size);
        } finally {
            stop.set(true);
            thread.join();
        }
    }

    @Test
    public void testIteratorDuringUpdate() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache<Integer, Integer> cache = createCache();
        final int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Thread thread = new Thread() {
            public void run() {
                Random rand = new Random();
                while (!stop.get()) {
                    int i = rand.nextInt(size);
                    try {
                        cache.put(i, -i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

        // Give chance to thread for starting
        sleepSeconds(1);

        try {
            int i = 0;
            Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
            while (iterator.hasNext()) {
                Cache.Entry<Integer, Integer> e = iterator.next();
                int key = e.getKey();
                int value = e.getValue();
                assertTrue("Key: " + key + ", Value: " + value, key == Math.abs(value));
                i++;
            }
            assertEquals(size, i);
        } finally {
            stop.set(true);
            thread.join();
        }
    }

    @Test
    public void testIteratorDuringRemoval() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache<Integer, Integer> cache = createCache();
        final int size = 2222;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Thread thread = new Thread() {
            public void run() {
                Random rand = new Random();
                while (!stop.get()) {
                    int i = rand.nextInt(size);
                    try {
                        cache.remove(i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

        // Give chance to thread for starting
        sleepSeconds(1);

        try {
            int i = 0;
            Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator();
            while (iterator.hasNext()) {
                Cache.Entry<Integer, Integer> e = iterator.next();
                int key = e.getKey();
                Integer value = e.getValue();
                if (value != null) {
                    assertEquals(key, value.intValue());
                }
                i++;
            }
            assertTrue(i <= size);
        } finally {
            stop.set(true);
            thread.join();
        }
    }

    @Test
    public void testRemoveAsync() throws ExecutionException, InterruptedException {
        ICache<String, String> cache = createCache();
        String key = randomString();
        String value = randomString();

        cache.put(key, value);
        ICompletableFuture<Boolean> future = cache.removeAsync(key);
        assertTrue(future.get());
    }

    @Test
    public void testRemoveAsyncWhenEntryNotFound() throws ExecutionException, InterruptedException {
        ICache<String, String> cache = createCache();

        ICompletableFuture<Boolean> future = cache.removeAsync(randomString());
        assertFalse(future.get());
    }

    @Test
    public void testRemoveAsync_withOldValue() throws ExecutionException, InterruptedException {
        ICache<String, String> cache = createCache();
        String key = randomString();
        String value = randomString();

        cache.put(key, value);
        ICompletableFuture<Boolean> future = cache.removeAsync(key, value);
        assertTrue(future.get());
    }

    @Test
    public void testRemoveAsyncWhenEntryNotFound_withOldValue() throws ExecutionException, InterruptedException {
        ICache<String, String> cache = createCache();

        ICompletableFuture<Boolean> future = cache.removeAsync(randomString(), randomString());
        assertFalse(future.get());
    }

    @Test
    public void testCompletionEvent() {
        String cacheName = randomString();

        CacheConfig<Integer, String> config = createCacheConfig();
        final CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String> listener =
                new CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Integer, String>(
                        FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key1 = 1;
        String value1 = "value1";
        cache.put(key1, value1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.created.get());
            }
        });

        Integer key2 = 2;
        String value2 = "value2";
        cache.put(key2, value2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, listener.created.get());
            }
        });

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(key1);
        keys.add(key2);
        cache.removeAll(keys);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, listener.removed.get());
            }
        });
    }

    @Test
    public void testJSRCreateDestroyCreate() throws InterruptedException {
        String cacheName = randomString();

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
        ArrayList<String> expected = new ArrayList<String>();
        ArrayList<String> real = new ArrayList<String>();
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
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        for (int fetchSize = 1; fetchSize < 102; fetchSize++) {
            CacheConcurrentHashMap<Data, String> map = new CacheConcurrentHashMap<Data, String>(1000);
            for (int i = 0; i < testSize; i++) {
                Integer key = i;
                Data data = ss.toData(key);
                String value1 = "value" + i;
                map.put(data, value1);
            }

            int nti = Integer.MAX_VALUE;
            int total = 0;
            int remaining = testSize;
            while (remaining > 0 && nti > 0) {
                int size = (remaining > fetchSize ? fetchSize : remaining);
                CacheKeyIteratorResult iteratorResult = map.fetchNext(nti, size);
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
        assertTrue(cache.isClosed());
        assertFalse(cache.isDestroyed());

        Cache<Object, Object> cacheAfterClose = cacheManager.getCache(cacheName);
        assertNotNull(cacheAfterClose);
        assertFalse(cacheAfterClose.isClosed());

        cache.put(1, 1);
    }

    @Test
    public void getButCantOperateOnCacheAfterDestroy() {
        String cacheName = randomString();
        ICache<Integer, Integer> cache = createCache(cacheName);
        cache.destroy();
        assertTrue(cache.isClosed());
        assertTrue(cache.isDestroyed());

        Cache<Object, Object> cacheAfterClose = cacheManager.getCache(cacheName);
        assertNotNull(cacheAfterClose);
        assertTrue(cache.isClosed());
        assertTrue(cache.isDestroyed());

        try {
            cache.put(1, 1);
            fail("Since cache is destroyed, operation on cache must with failed with 'IllegalStateException'");
        } catch (IllegalStateException e) {
            // Expect this exception since cache is closed and destroyed
        } catch (Throwable t) {
            t.printStackTrace();
            fail("Since cache is destroyed, operation on cache must with failed with 'IllegalStateException', "
                    + "not with " + t.getMessage());
        }
    }

    @Test
    public void testEntryProcessor_invoke() throws ExecutionException, InterruptedException {
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
    public void testEntryProcessor_invokeAll() throws ExecutionException, InterruptedException {
        ICache<String, String> cache = createCache();
        int entryCount = 10;
        Map<String, String> localMap = new HashMap<String, String>();
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
}
