package com.hazelcast.client.cache;

import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
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
public class DummyClientCachePartitionIteratorTest extends AbstractClientCachePartitionIteratorTest {

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();

        Config config = getConfig();
        server = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient(getClientConfig(server));
        cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
    }

    private static ClientConfig getClientConfig(HazelcastInstance instance) {
        Address address = instance.getCluster().getLocalMember().getAddress();
        String addressString = address.getHost() + ":" + address.getPort();
        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        networkConfig.setSmartRouting(false);
        networkConfig.addAddress(addressString);
        clientConfig.setNetworkConfig(networkConfig);
        return clientConfig;
    }
}
