package com.hazelcast.client;

import com.hazelcast.client.impl.DefaultClientExtension;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientExtensionTest extends HazelcastTestSupport {

    @Test
    public void test_createServiceProxyFactory() throws Exception {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertInstanceOf(ClientProxyFactory.class, clientExtension.createServiceProxyFactory(MapService.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_createServiceProxyFactory_whenUnknownServicePassed() throws Exception {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertEquals(ClientProxyFactory.class, clientExtension.createServiceProxyFactory(TestService.class));
    }

    private class TestService {

    }
}
