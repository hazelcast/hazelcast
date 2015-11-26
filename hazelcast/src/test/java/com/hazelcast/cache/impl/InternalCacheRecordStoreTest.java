package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheFromDifferentNodesTest;
import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InternalCacheRecordStoreTest extends CacheTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    /**
     * Test for issue: https://github.com/hazelcast/hazelcast/issues/6618
     */
    @Test
    public void testBatchEventMapShouldBeCleanedAfterRemoveAll() {
        String cacheName = randomString();

        CacheConfig<Integer, String> config = createCacheConfig();
        CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String> listener =
                new CacheFromDifferentNodesTest.SimpleEntryListener<Integer, String>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Integer, String>(
                        FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Integer key = 1;
        String value = "value";
        cache.put(key, value);

        HazelcastInstance instance = ((HazelcastCacheManager) cacheManager).getHazelcastInstance();
        int partitionId = instance.getPartitionService().getPartition(key).getPartitionId();

        cache.removeAll();

        Node node = getNode(instance);
        assertNotNull(node);

        ICacheService cacheService = node.getNodeEngine().getService(ICacheService.SERVICE_NAME);
        AbstractCacheRecordStore recordStore = (AbstractCacheRecordStore) cacheService
                .getRecordStore("/hz/" + cacheName, partitionId);
        assertEquals(0, recordStore.batchEvent.size());
    }

    @Override
    protected void onSetup() {
    }

    @Override
    protected void onTearDown() {
        factory.terminateAll();
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return factory.newHazelcastInstance(createConfig());
    }
}
