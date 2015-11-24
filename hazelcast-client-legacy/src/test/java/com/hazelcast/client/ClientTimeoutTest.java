package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientTimeoutTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }


    @Test(timeout = 20000, expected = IllegalStateException.class)
    public void testTimeoutToOutsideNetwork() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
        clientConfig.getNetworkConfig().addAddress("8.8.8.8:5701");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
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
        hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastClient(clientConfig);
    }
}
