package com.hazelcast.client.cache;

import com.hazelcast.cache.CachingProviderTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCachingProviderTest extends CachingProviderTest {

    private final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    @Override
    protected TestHazelcastInstanceFactory createTestHazelcastInstanceFactory(int count) {
        // Since `HazelcastClient.getHazelcastClientByName(instanceName);` doesn't work on mock client,
        // we are using real instances.
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        instances.add(instance);
        // Since we are using real instances, no need to mock instance factory.
        return null;
    }

    @Override
    protected HazelcastInstance createCacheInstance() {
        // Since `HazelcastClient.getHazelcastClientByName(instanceName);` doesn't work on mock client,
        // we are using real instances.
        HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        instances.add(instance);
        return instance;
    }

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance defaultInstance) {
        return HazelcastClientCachingProvider.createCachingProvider(defaultInstance);
    }

    @After
    public void tearDown() {
        Iterator<HazelcastInstance> iter = instances.iterator();
        while (iter.hasNext()) {
            HazelcastInstance instance = iter.next();
            try {
                instance.shutdown();
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                iter.remove();
            }
        }
    }

}
