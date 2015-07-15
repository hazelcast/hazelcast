package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientAuthenticationTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }


    @Test(expected = IllegalStateException.class)
    public void testFailedAuthentication() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1);
        clientConfig.getGroupConfig().setPassword("InvalidPassword");
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoClusterFound() throws Exception {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1);
        hazelcastFactory.newHazelcastClient(clientConfig);

    }
}
