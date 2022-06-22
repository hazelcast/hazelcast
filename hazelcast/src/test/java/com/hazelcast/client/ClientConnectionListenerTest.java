package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.management.ClientConnectionProcessListener;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ClientConnectionListenerTest extends HazelcastTestSupport {


    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void successfulConnection()
            throws Exception {
        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
        String clusterName = getTestMethodName();
        ClientConfig clientConfig = new ClientConfig()
                .setClusterName(clusterName)
                .addListenerConfig(new ListenerConfig().setImplementation(listener));

        Hazelcast.newHazelcastInstance(new Config().setClusterName(clusterName));

        HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println("itt");

        verify(listener).possibleAddressesCollected(asList(
                new Address("127.0.0.1", 5701),
                new Address("127.0.0.1", 5702),
                new Address("127.0.0.1", 5703)
        ));
        verify(listener).attemptingToConnectToAddress(new Address("127.0.0.1", 5701));
        verify(listener).authenticationSuccess(null);
        verify(listener).clusterConnectionSucceeded(clusterName);
        verifyNoMoreInteractions(listener);
    }

}
