package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.Ignore;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class ClientTimeoutTest {

    @After
    @Before
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 20000, expected = IllegalStateException.class)
    public void testTimeoutToOutsideNetwork() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
        clientConfig.getNetworkConfig().addAddress("8.8.8.8:5701");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IList<Object> list = client.getList("test");
    }

    @Test
    public void testConnectionTimeout_withIntMax() {
        testConnectionTimeout(Integer.MAX_VALUE);
    }

    @Test
    public void testConnectionTimeout_withZeroValue() {
        testConnectionTimeout(0);
    }

    public void testConnectionTimeout(int timeoutInMillis) {
        //Should work without throwing exception.
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(timeoutInMillis);
        Hazelcast.newHazelcastInstance();
        HazelcastClient.newHazelcastClient(clientConfig);
    }
}
