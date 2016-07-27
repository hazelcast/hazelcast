package com.hazelcast.client.cache;

import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCachePartitionIteratorTest extends AbstractClientCachePartitionIteratorTest {

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        server = factory.newHazelcastInstance();

        HazelcastInstance client = factory.newHazelcastClient();
        cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
    }
}
