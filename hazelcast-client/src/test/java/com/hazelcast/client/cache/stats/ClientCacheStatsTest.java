package com.hazelcast.client.cache.stats;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.stats.CacheStatsTest;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ClientCacheStatsTest extends CacheStatsTest {

    private final TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Override
    protected void onSetup() {
        super.onSetup();
        instanceFactory.newHazelcastInstance(createConfig());
        ClientConfig clientConfig = createClientConfig();
        client = instanceFactory.newHazelcastClient(clientConfig);
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        instanceFactory.shutdownAll();
    }

    @Override
    protected CachingProvider getCachingProvider() {
        return HazelcastClientCachingProvider.createCachingProvider(client);
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testOwnedEntryCount() {
        super.testOwnedEntryCount();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExpiries() {
        super.testExpiries();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEvictions() {
        super.testEvictions();
    }

    @Test
    public void testNearCacheStatsWhenNearCacheEnabled() {
        String cacheName = randomName();
        CacheConfig cacheConfig = createCacheConfig();
        cacheConfig.setName(cacheName);
        ClientConfig clientConfig = ((HazelcastClientProxy) client).getClientConfig();
        clientConfig.addNearCacheConfig(new NearCacheConfig().setName(cacheName));
        ICache<Integer, String> cache = createCache(cacheName, cacheConfig);
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertNotNull(stats.getNearCacheStatistics());
    }

}
