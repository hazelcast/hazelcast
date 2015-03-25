package com.hazelcast.client;

import com.hazelcast.client.impl.DefaultClientExtension;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientExtensionTest {

    @Test
    public void testGetServiceProxy() throws Exception {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertEquals(ClientMapProxy.class, clientExtension.getServiceProxy(MapService.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetServiceProxy_whenUnknownServicePassed() throws Exception {
        ClientExtension clientExtension = new DefaultClientExtension();
        clientExtension.getServiceProxy(TestService.class);
    }

    private class TestService {

    }
}