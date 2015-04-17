package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientAuthenticationTest extends HazelcastTestSupport {

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }


    @Test(expected = IllegalStateException.class)
    public void testFailedAuthentication() throws Exception {
        Hazelcast.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setPassword("InvalidPassword");
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoClusterFound() throws Exception {
        HazelcastClient.newHazelcastClient();

    }
}
