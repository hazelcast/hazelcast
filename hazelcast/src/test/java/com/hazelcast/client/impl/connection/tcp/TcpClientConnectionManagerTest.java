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
import com.hazelcast.client.impl.clientside.ClusterDiscoveryService;
import com.hazelcast.client.impl.clientside.LifecycleServiceImpl;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.spi.ClientMemberListProvider;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTest {

    private static TcpClientConnectionManager createConnectionManager(ConnectionManagerStateCallbacks callbacks,
                                                                      Runnable onClientGracefulShutdownFunc,
                                                                      Runnable shutdownClientFunc) {
        LoggingService loggingService = mock(LoggingService.class);
        ClusterDiscoveryService clusterDiscoveryService = mock(ClusterDiscoveryService.class);
        Networking networking = mock(Networking.class);
        ClientConfig clientConfig = new ClientConfig();
        HazelcastProperties properties = new HazelcastProperties(new Properties());
        return new TcpClientConnectionManager(loggingService, clientConfig, properties,
                clusterDiscoveryService, "test", networking,
                lifecycleServiceImpl(onClientGracefulShutdownFunc, shutdownClientFunc),
                memberListProvider(), callbacks, authenticator());
    }

    private static LifecycleServiceImpl lifecycleServiceImpl(Runnable onClientGracefulShutdownFunc,
                                                             Runnable shutdownClientFunc) {
        return new LifecycleServiceImpl("test", new ClientConfig(),
                mock(ILogger.class), onClientGracefulShutdownFunc, shutdownClientFunc);
    }

    private static Authenticator authenticator() {
        return  connection -> {
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
        return new ClientMemberListProvider() {
            @Override
            public Member getMember(@NotNull UUID uuid) {
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
        createConnectionManager(new ConnectionManagerStateCallbacks() {

            @Override
            public void onConnectionClose(TcpClientConnection connection) {

            }

            @Override
            public void onClusterChange(String nextClusterName) {

            }

            @Override
            public void waitForInitialMembershipEvents() {

            }

            @Override
            public void onClusterRestart() {

            }

            @Override
            public void sendStateToCluster() {

            }

            @Override
            public int getAndSetPartitionCount(int partitionCount) {
                return 0;
            }
        }, () -> {

        }, () -> {

        });
    }
}
