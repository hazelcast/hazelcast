package com.hazelcast.client.cache;

import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Arrays;
import java.util.Iterator;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCachePartitionIteratorTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean prefetchValues;

    @Parameterized.Parameters(name = "prefetchValues:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    private TestHazelcastFactory factory;
    private CachingProvider cachingProvider;
    private HazelcastInstance server;

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
        server = factory.newHazelcastInstance();
        cachingProvider = createCachingProvider();
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    protected CachingProvider createCachingProvider() {
        HazelcastInstance client = factory.newHazelcastClient();
        return HazelcastClientCachingProvider.createCachingProvider(client);
    }

    private <K, V> ClientCacheProxy<K, V> getCacheProxy() {
        String cacheName = randomString();
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<K, V> config = new CacheConfig<K, V>();
        config.getEvictionConfig().setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT).setSize(10000000);
        return (ClientCacheProxy<K, V>) cacheManager.createCache(cacheName, config);

    }

    @Test
    public void test_HasNext_Returns_False_On_EmptyPartition() throws Exception {
        ClientCacheProxy<Integer, Integer> cache = getCacheProxy();
        Iterator<Cache.Entry<Integer, Integer>> iterator = cache.iterator(10, 1, prefetchValues);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_HasNext_Returns_True_On_NonEmptyPartition() throws Exception {
        ClientCacheProxy<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = cache.iterator(10, 1, prefetchValues);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void test_Next_Returns_Value_On_NonEmptyPartition() throws Exception {
        ClientCacheProxy<String, String> cache = getCacheProxy();

        String key = generateKeyForPartition(server, 1);
        String value = randomString();
        cache.put(key, value);

        Iterator<Cache.Entry<String, String>> iterator = cache.iterator(10, 1, prefetchValues);
        Cache.Entry entry = iterator.next();
        assertEquals(value, entry.getValue());
    }

    @Test
    public void test_Next_Returns_Values_When_FetchSizeExceeds_On_NonEmptyPartition() throws Exception {
        ClientCacheProxy<String, String> cache = getCacheProxy();
        String value = randomString();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(server, 42);
            cache.put(key, value);
        }
        Iterator<Cache.Entry<String, String>> iterator = cache.iterator(10, 42, prefetchValues);
        for (int i = 0; i < count; i++) {
            Cache.Entry entry = iterator.next();
            assertEquals(value, entry.getValue());

        }
    }
}
