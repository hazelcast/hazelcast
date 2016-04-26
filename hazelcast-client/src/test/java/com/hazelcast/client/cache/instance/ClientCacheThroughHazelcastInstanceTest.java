package com.hazelcast.client.cache.instance;

import com.hazelcast.cache.instance.CacheThroughHazelcastInstanceTest;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
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
public class ClientCacheThroughHazelcastInstanceTest extends CacheThroughHazelcastInstanceTest {

    private TestHazelcastFactory instanceFactory;
    private HazelcastInstance ownerInstance;

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
        ownerInstance = instanceFactory.newHazelcastInstance();
        return instanceFactory.newHazelcastClient();
    }

    @Override
    protected HazelcastInstance createInstance(Config config) {
        if (instanceFactory != null) {
            throw new IllegalStateException("There should not be multiple creation of TestHazelcastFactory!");
        }
        instanceFactory = new TestHazelcastFactory();
        ownerInstance = instanceFactory.newHazelcastInstance(config);
        if (config.getClassLoader() != null) {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setClassLoader(config.getClassLoader());
            return instanceFactory.newHazelcastClient(clientConfig);
        } else {
            return instanceFactory.newHazelcastClient();
        }
    }

    @Override
    protected void shutdownOwnerInstance(HazelcastInstance instance) {
        if (ownerInstance != null) {
            ownerInstance.shutdown();
        } else {
            throw new IllegalStateException("");
        }
    }

    @Override
    protected Class<? extends Exception> getInstanceNotActiveExceptionType() {
        return HazelcastClientNotActiveException.class;
    }

    @After
    public void tearDown() {
        if (instanceFactory != null) {
            ownerInstance = null;
            instanceFactory.shutdownAll();
            instanceFactory = null;
        }
    }

}
