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
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.nio.Protocols;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import static com.hazelcast.instance.impl.TestUtil.getHazelcastInstanceImpl;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.util.Collections.singletonList;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientConnectionProcessListenerTest extends HazelcastTestSupport {

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
                .setClusterConnectTimeoutMillis(0);
        return config;
    }

    @Test
    public void successfulConnection() {
        Config config = new Config();
        config.setClusterName(getTestMethodName());
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Address address = instance.getCluster().getLocalMember().getAddress();

        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig(listener))
                .getNetworkConfig()
                .addAddress(address.getHost() + ":" + address.getPort());

        HazelcastClient.newHazelcastClient(clientConfig);

        // Events are fired in a separate executor, it might take a while for
        // the listeners to be notified.
        assertTrueEventually(() -> {
            verify(listener).possibleAddressesCollected(singletonList(address));
            verify(listener, atLeastOnce()).attemptingToConnectToAddress(address);
            verify(listener).authenticationSuccess(address);
            verify(listener).clusterConnectionSucceeded(getTestMethodName());
            verifyNoMoreInteractions(listener);
        });
    }

    @Test
    public void hostResolutionFailure_portFailure() throws Exception {
        Address address = new Address("localhost", 6000);
        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig(listener))
                .getNetworkConfig()
                .addAddress("nowhere.in.neverland:5701") // address not found
                .addAddress(address.getHost() + ":" + address.getPort()); // port failure

        try {
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("unexpectedly successful client startup");
        } catch (IllegalStateException e) {
            // Events are fired in a separate executor, it might take a while for
            // the listeners to be notified.
            assertTrueEventually(() -> {
                verify(listener, atLeastOnce()).hostNotFound("nowhere.in.neverland");
                verify(listener, atLeastOnce()).possibleAddressesCollected(singletonList(address));
                verify(listener, atLeastOnce()).attemptingToConnectToAddress(address);
                verify(listener, atLeastOnce()).connectionAttemptFailed(address);
                verify(listener).clusterConnectionFailed(getTestMethodName());
                verifyNoMoreInteractions(listener);
            });
        }
    }

    @Test
    public void remoteClosesConnection() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            spawn(() -> {
                try {
                    while (!server.isClosed()) {
                        Socket clientSocket = server.accept();
                        InputStream is = clientSocket.getInputStream();
                        for (int i = 0; i < Protocols.PROTOCOL_LENGTH * 2; i++) {
                            // Read the protocol bytes + at least some part
                            // of the auth message before closing the connection
                            // so we will be sure that the auth invocation
                            // will fail with the TargetDisconnectedException
                            if (is.read() == -1) {
                                fail("EOF before the auth request");
                            }
                        }
                        OutputStream os = clientSocket.getOutputStream();
                        os.close();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            Address address = new Address("localhost", server.getLocalPort());
            ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
            ClientConfig clientConfig = newClientConfig();
            clientConfig.addListenerConfig(new ListenerConfig(listener))
                    .getNetworkConfig()
                    .addAddress(address.getHost() + ":" + address.getPort());

            try {
                HazelcastClient.newHazelcastClient(clientConfig);
                fail("unexpectedly successful client startup");
            } catch (IllegalStateException ignored) {
                // Events are fired in a separate executor, it might take a while for
                // the listeners to be notified.
                assertTrueEventually(() -> {
                    verify(listener, atLeastOnce()).possibleAddressesCollected(singletonList(address));
                    verify(listener, atLeastOnce()).attemptingToConnectToAddress(address);
                    verify(listener, atLeastOnce()).remoteClosedConnection(address);
                    verify(listener).clusterConnectionFailed(getTestMethodName());
                    verifyNoMoreInteractions(listener);
                });
            }
        }
    }

    @Test
    public void clientNotAllowedInCluster() {
        Config config = new Config();
        config.setClusterName(getTestMethodName());
        HazelcastInstanceImpl instance = getHazelcastInstanceImpl(Hazelcast.newHazelcastInstance(config));
        instance.node.getClientEngine().applySelector(ClientSelectors.none());
        Address address = instance.getCluster().getLocalMember().getAddress();

        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig(listener))
                .getNetworkConfig()
                .addAddress(address.getHost() + ":" + address.getPort());

        try {
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("unexpectedly successful client startup");
        } catch (IllegalStateException e) {
            // Events are fired in a separate executor, it might take a while for
            // the listeners to be notified.
            assertTrueEventually(() -> {
                verify(listener).possibleAddressesCollected(singletonList(address));
                verify(listener).attemptingToConnectToAddress(address);
                verify(listener).clientNotAllowedInCluster(address);
                verify(listener).clusterConnectionFailed(getTestMethodName());
                verifyNoMoreInteractions(listener);
            });
        }
    }

    @Test
    public void wrongClusterName() {
        Config config = new Config();
        config.setClusterName(getTestMethodName());
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Address address = instance.getCluster().getLocalMember().getAddress();

        ClientConnectionProcessListener listener = mock(ClientConnectionProcessListener.class);

        ClientConfig clientConfig = newClientConfig();
        clientConfig.setClusterName("incorrect-name")
                .addListenerConfig(new ListenerConfig(listener))
                .getNetworkConfig()
                .addAddress(address.getHost() + ":" + address.getPort());

        try {
            HazelcastClient.newHazelcastClient(clientConfig);
            fail("unexpectedly successful client startup");
        } catch (IllegalStateException e) {

            // Events are fired in a separate executor, it might take a while for
            // the listeners to be notified.
            assertTrueEventually(() -> {
                verify(listener, atLeastOnce()).possibleAddressesCollected(singletonList(address));
                verify(listener, atLeastOnce()).attemptingToConnectToAddress(address);
                verify(listener, atLeastOnce()).credentialsFailed(address);
                verify(listener, atLeastOnce()).connectionAttemptFailed(address);
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
