package com.hazelcast.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientConnectionListenerTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testClientListener() {
        Config config = new Config();
        final CountDownLatch latch = new CountDownLatch(2);
        ListenerConfig listenerConfig = new ListenerConfig(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                latch.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                latch.countDown();
            }
        });

        config.addListenerConfig(listenerConfig);

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        client.shutdown();
        assertOpenEventually(latch);
        instance.shutdown();
    }
}
