/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.management;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientConnectionProcessListenerTest
        extends HazelcastTestSupport {

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    private static ClientConfig newClientConfig() {
        ClientConfig config = new ClientConfig();
        ListenerConfig listenerConfig = new ListenerConfig(new ExceptionThrowingListener());
        config.setClusterName(getTestMethodName())
                .addListenerConfig(listenerConfig)
                .getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(5_000);
        return config;
    }

    @Test
    public void successfulConnection() {
        String clusterName = getTestMethodName();
        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
        ClientConfig clientConfig = newClientConfig()
                .addListenerConfig(new ListenerConfig(listener));

        Hazelcast.newHazelcastInstance(new Config().setClusterName(clusterName));

        HazelcastClient.newHazelcastClient(clientConfig);

        // Events are fired in a separate executor, it might take a while for
        // the listeners to be notified.
        assertTrueEventually(() -> {
            verify(listener).possibleAddressesCollected(asList(
                    new Address("127.0.0.1", 5701),
                    new Address("127.0.0.1", 5702),
                    new Address("127.0.0.1", 5703)
            ));
            verify(listener, atLeastOnce()).attemptingToConnectToAddress(new Address("127.0.0.1", 5701));
            verify(listener).authenticationSuccess(new Address("127.0.0.1", 5701));
            verify(listener).clusterConnectionSucceeded(clusterName);
            verifyNoMoreInteractions(listener);
        });
    }

    @Test
    public void hostResolutionFailure_portFailure() throws Exception {
        String clusterName = getTestMethodName();
        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig(listener))
                .getNetworkConfig()
                .addAddress("nowhere.in.neverland:5701") // address not found
                .addAddress("localhost:6000"); // port failure

        Hazelcast.newHazelcastInstance(new Config().setClusterName(clusterName));

        try {
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("unexpectedly successful client startup");
        } catch (IllegalStateException e) {
            Address addr = new Address("localhost", 6000);

            // Events are fired in a separate executor, it might take a while for
            // the listeners to be notified.
            assertTrueEventually(() -> {
                verify(listener, atLeastOnce()).hostNotFound("nowhere.in.neverland");
                verify(listener, atLeastOnce()).possibleAddressesCollected(singletonList(addr));
                verify(listener, atLeastOnce()).attemptingToConnectToAddress(addr);
                verify(listener, atLeastOnce()).connectionAttemptFailed(addr);
                verify(listener).clusterConnectionFailed(clusterName);
                verifyNoMoreInteractions(listener);
            });
        }
    }

    @Test
    public void remoteClosesConnection() throws Exception {
        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
        ServerSocket server = new ServerSocket(5701);
        try {
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    while (true) {
                        Socket clientSocket = server.accept();
                        Thread.sleep(500);
                        OutputStream os = clientSocket.getOutputStream();
                        os.write("junk response".getBytes(StandardCharsets.UTF_8));
                        os.flush();
                        os.close();
                    }
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            ClientConfig clientConfig = newClientConfig();
            clientConfig.addListenerConfig(new ListenerConfig(listener))
                    .getNetworkConfig().addAddress("localhost:5701");
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("unexpectedly successful client startup");
        } catch (IllegalStateException e) {
            // Events are fired in a separate executor, it might take a while for
            // the listeners to be notified.
            assertTrueEventually(() -> {
                verify(listener, atLeastOnce()).possibleAddressesCollected(singletonList(new Address("localhost", 5701)));
                verify(listener, atLeastOnce()).attemptingToConnectToAddress(new Address("localhost", 5701));
                verify(listener, atLeastOnce()).remoteClosedConnection(new Address("localhost", 5701));
                verify(listener).clusterConnectionFailed(getTestMethodName());
                verifyNoMoreInteractions(listener);
            });
        } finally {
            do {
                server.close();
            } while (!server.isClosed());
        }
    }

    @Test
    public void clientNotAllowedInCluster() throws Exception {
        String clusterName = getTestMethodName();
        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);

        ClientConfig clientConfig = newClientConfig()
                .addListenerConfig(new ListenerConfig(listener));
        ClientFailoverConfig failoverConfig = new ClientFailoverConfig().setTryCount(0).addClientConfig(clientConfig);

        Hazelcast.newHazelcastInstance(new Config().setClusterName(clusterName));

        try {
            HazelcastClient.newHazelcastFailoverClient(failoverConfig);
            fail("unexpectedly successful client startup");
        } catch (IllegalStateException e) {
            Address addr = new Address("127.0.0.1", 5701);

            // Events are fired in a separate executor, it might take a while for
            // the listeners to be notified.
            assertTrueEventually(() -> {
                verify(listener).possibleAddressesCollected(asList(
                        new Address("127.0.0.1", 5701),
                        new Address("127.0.0.1", 5702),
                        new Address("127.0.0.1", 5703)
                ));
                verify(listener).attemptingToConnectToAddress(addr);
                verify(listener).clientNotAllowedInCluster(addr);
                verify(listener).clusterConnectionFailed(clusterName);
                verifyNoMoreInteractions(listener);
            });
        }
    }

    @Test
    public void wrongClusterName() throws Exception {
        String clusterName = getTestMethodName();
        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setClusterName("incorrect-name")
                .addListenerConfig(new ListenerConfig(listener))
                .getNetworkConfig()
                .addAddress("localhost:5701");

        Hazelcast.newHazelcastInstance(new Config().setClusterName(clusterName));

        try {
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("unexpectedly successful client startup");
        } catch (IllegalStateException e) {
            Address addr = new Address("localhost", 5701);

            // Events are fired in a separate executor, it might take a while for
            // the listeners to be notified.
            assertTrueEventually(() -> {
                verify(listener, atLeastOnce()).possibleAddressesCollected(singletonList(addr));
                verify(listener, atLeastOnce()).attemptingToConnectToAddress(addr);
                verify(listener, atLeastOnce()).credentialsFailed(addr);
                verify(listener, atLeastOnce()).connectionAttemptFailed(addr);
                verify(listener, atLeastOnce()).clusterConnectionFailed("incorrect-name");
                verifyNoMoreInteractions(listener);
            });
        }
    }

    private static class ExceptionThrowingListener implements ClientConnectionProcessListener {

        @Override
        public void attemptingToConnectToAddress(Address address) {
            throw new RuntimeException();
        }

        @Override
        public void connectionAttemptFailed(Address target) {
            throw new RuntimeException();
        }

        @Override
        public void hostNotFound(String host) {
            throw new RuntimeException();
        }

        @Override
        public void possibleAddressesCollected(List<Address> addresses) {
            throw new RuntimeException();
        }

        @Override
        public void authenticationSuccess(Address remoteAddress) {
            throw new RuntimeException();
        }

        @Override
        public void credentialsFailed(Address remoteAddress) {
            throw new RuntimeException();
        }

        @Override
        public void clientNotAllowedInCluster(Address remoteAddress) {
            throw new RuntimeException();
        }

        @Override
        public void clusterConnectionFailed(String clusterName) {
            throw new RuntimeException();
        }

        @Override
        public void clusterConnectionSucceeded(String clusterName) {
            throw new RuntimeException();
        }

        @Override
        public void remoteClosedConnection(Address address) {
            throw new RuntimeException();
        }
    }
}
