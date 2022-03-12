/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.ClusterDiscoveryService;
import com.hazelcast.client.impl.clientside.LifecycleServiceImpl;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.spi.ClientMemberListProvider;
import com.hazelcast.client.impl.spi.impl.DefaultAddressProvider;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.logging.Level;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTest {

    public static class ConnectionManagerBuilder {

        Runnable onClientGracefulShutdown = () -> {
        };
        Runnable shutdownClientFunc = () -> {
        };
        Runnable onClusterRestart = () -> {
        };
        Consumer<TcpClientConnection> onConnectionClose = connection -> {
        };
        Consumer<String> onClusterChange = nextClusterName -> {
        };

        private TcpClientConnectionManager build() {
            ConnectionManagerStateCallbacks callbacks = new ConnectionManagerStateCallbacks() {

                @Override
                public void onConnectionClose(TcpClientConnection connection) {
                    onConnectionClose.accept(connection);
                }

                @Override
                public void onClusterChange(String nextClusterName) {
                    onClusterChange.accept(nextClusterName);
                }

                @Override
                public void waitForInitialMembershipEvents() {
                }

                @Override
                public void onClusterRestart() {
                    onClusterRestart.run();
                }

                @Override
                public void sendStateToCluster() {

                }

                @Override
                public int getAndSetPartitionCount(int partitionCount) {
                    return partitionCount;
                }

            };
            LoggingService loggingService = new LoggingService() {
                @Override
                public void addLogListener(@Nonnull Level level, @Nonnull LogListener logListener) {

                }

                @Override
                public void removeLogListener(@Nonnull LogListener logListener) {

                }

                @Nonnull
                @Override
                public ILogger getLogger(@Nonnull String name) {
                    return Mockito.mock(ILogger.class);
                }

                @Nonnull
                @Override
                public ILogger getLogger(@Nonnull Class type) {
                    return Mockito.mock(ILogger.class);
                }
            };
            Networking networking = mock(Networking.class);
            ClientConfig clientConfig = new ClientConfig();
            HazelcastProperties properties = new HazelcastProperties(new Properties());
            return new TcpClientConnectionManager(loggingService, clientConfig, properties,
                    clusterDiscoveryService(clientConfig.getNetworkConfig()), "test", networking,
                    lifecycleServiceImpl(onClientGracefulShutdown, shutdownClientFunc),
                    memberListProvider(), callbacks, authenticator());
        }

        private static ClusterDiscoveryService clusterDiscoveryService(ClientNetworkConfig clientNetworkConfig) {
            return new ClusterDiscoveryService() {
                @Override
                public boolean tryNextCluster(BiPredicate<CandidateClusterContext, CandidateClusterContext> function) {
                    return false;
                }

                @Override
                public CandidateClusterContext current() {
                    return new CandidateClusterContext("dev", new DefaultAddressProvider(clientNetworkConfig),
                            null, null, null, null);
                }

                @Override
                public boolean failoverEnabled() {
                    return false;
                }
            };
        }

        private static LifecycleServiceImpl lifecycleServiceImpl(Runnable onClientGracefulShutdownFunc,
                                                                 Runnable shutdownClientFunc) {
            LifecycleServiceImpl lifecycleService = new LifecycleServiceImpl("test", new ClientConfig(),
                    mock(ILogger.class), onClientGracefulShutdownFunc, shutdownClientFunc);
            lifecycleService.start();
            return lifecycleService;
        }

        private static Authenticator authenticator() {
            return connection -> {
                CompletableFuture<ClientMessage> future = new CompletableFuture<>();
                ClientMessage message = null;
                try {
                    message = ClientAuthenticationCodec.encodeResponse(AuthenticationStatus.AUTHENTICATED.getId(),
                            new Address("127.0.0.1", 5701), UUID.randomUUID(), (byte) 1,
                            "1.0.0", 271, UUID.randomUUID(), true);
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
                future.complete(message);
                return future;
            };
        }

        private static ClientMemberListProvider memberListProvider() {
            ConcurrentHashMap<UUID, Member> map = new ConcurrentHashMap<>();
            map.put(UUID.randomUUID(), new MemberImpl());
            return new ClientMemberListProvider() {
                @Override
                public Member getMember(@Nonnull UUID uuid) {
                    return map.get(uuid);
                }

                @Override
                public Collection<Member> getMemberList() {
                    return map.values();
                }

                @Override
                public boolean translateToPublicAddress() {
                    return false;
                }
            };
        }

        @Test
        public void test() {
            TcpClientConnectionManager connectionManager = new ConnectionManagerBuilder().build();

            connectionManager.start();
            connectionManager.tryConnectToAllClusterMembers();
            connectionManager.startClusterThread();

            connectionManager.shutdown();
        }
    }
}
