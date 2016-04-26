package com.hazelcast.client.cache.instance;

import com.hazelcast.cache.instance.CacheHazelcastInstanceAwareTest;
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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheHazelcastInstanceAwareTest extends CacheHazelcastInstanceAwareTest {

    private TestHazelcastFactory instanceFactory;

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return HazelcastClientCachingProvider.createCachingProvider(instance);
    }

    @Override
    protected HazelcastInstance createInstance() {
        if (instanceFactory != null) {
            throw new IllegalStateException("There should not be multiple creation of TestHazelcastFactory!");
        }
        instanceFactory = new TestHazelcastFactory();
        Config config = createConfig();
        instanceFactory.newHazelcastInstance(config);
        return instanceFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        if (instanceFactory != null) {
            instanceFactory.shutdownAll();
            instanceFactory = null;
        }
    }

}
