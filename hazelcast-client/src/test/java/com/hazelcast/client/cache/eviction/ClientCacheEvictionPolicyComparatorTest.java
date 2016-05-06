package com.hazelcast.client.cache.eviction;

import com.hazelcast.cache.eviction.CacheEvictionPolicyComparatorTest;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheEvictionPolicyComparatorTest extends CacheEvictionPolicyComparatorTest {

    private final TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    private HazelcastInstance instance;

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return HazelcastClientCachingProvider.createCachingProvider(instance);
    }

    @Override
    protected HazelcastInstance createInstance(Config config) {
        instance = instanceFactory.newHazelcastInstance(config);
        return instanceFactory.newHazelcastClient();
    }

    @Override
    protected ConcurrentMap getUserContext(HazelcastInstance hazelcastInstance) {
        return instance.getUserContext();
    }

    @After
    public void tearDown() {
        instanceFactory.shutdownAll();
    }

}
