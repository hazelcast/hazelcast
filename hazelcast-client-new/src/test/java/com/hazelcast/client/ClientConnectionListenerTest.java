package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientConnectionListenerTest extends HazelcastTestSupport {

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

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        client.shutdown();
        assertOpenEventually(latch);
        instance.shutdown();
    }
}
