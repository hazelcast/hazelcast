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
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

public class ConnectionManagerTestUtil {

    static class ClusterContext {
        Runnable onClusterConnect = () -> {

        };
        ConcurrentHashMap<UUID, MemberContext> memberMap = new ConcurrentHashMap<>();
        UUID defaultClusterId = UUID.randomUUID();

        UUID addMember(String host, int port, UUID clusterId) {
            UUID uuid = UUID.randomUUID();
            Member member = member(uuid, host, port);
            memberMap.put(uuid, new MemberContext(member, clusterId));
            return uuid;
        }

        UUID addMember(String host, int port) {
            UUID uuid = UUID.randomUUID();
            Member member = member(uuid, host, port);
            memberMap.put(uuid, new MemberContext(member, defaultClusterId));
            return uuid;
        }

        void removeMember(UUID uuid) {
            memberMap.remove(uuid);
        }
    }

    static class MemberContext {
        Member member;
        UUID clusterId;

        MemberContext(Member member, UUID clusterId) {
            this.member = member;
            this.clusterId = clusterId;
        }
    }

    static TcpClientConnectionManager newConnectionManager(ClientConfig clientConfig) {
        ClusterContext context = new ClusterContext();
        context.addMember("127.0.0.1", 5701, UUID.randomUUID());
        return newConnectionManager(clientConfig, context);
    }


    static Member member(UUID uuid, String host, int port) {
        try {
            return new MemberImpl.Builder(new Address(host, port)).uuid(uuid).build();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    static TcpClientConnectionManager newConnectionManager(ClientConfig clientConfig, ClusterContext context) {
        AtomicInteger connectionIdGenerator = new AtomicInteger();
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

        HazelcastProperties properties = new HazelcastProperties(new Properties());
        LifecycleServiceImpl lifecycleService = lifecycleServiceImpl();
        return new TcpClientConnectionManager(loggingService, clientConfig, properties,
                clusterDiscoveryService(clientConfig.getNetworkConfig()), "test", networking,
                lifecycleService, memberListProvider(context.memberMap), null, authenticator(context.memberMap
        )) {
            @Override
            protected void connectionClosed(TcpClientConnection connection) {
            }

            @Override
            protected void onClusterChange(String newClusterName) {
            }

            @Override
            protected void onClusterConnect() {
                context.onClusterConnect.run();
            }

            @Override
            protected int getAndSetPartitionCount(int partitionCount) {
                return partitionCount;
            }

            @Override
            protected void sendStateToCluster() {
            }

            @Override
            protected TcpClientConnection createSocketConnection(Address target) {
                return new TcpClientConnection(this, lifecycleService, loggingService,
                        connectionIdGenerator.incrementAndGet(), null) {
                    @Override
                    public InetSocketAddress getLocalSocketAddress() {
                        return null;
                    }

                    @Override
                    public Address getInitAddress() {
                        return target;
                    }

                    @Override
                    public long lastReadTimeMillis() {
                        return System.currentTimeMillis();
                    }

                    @Override
                    public long lastWriteTimeMillis() {
                        return System.currentTimeMillis();
                    }
                };
            }
        };
    }

    private static ClusterDiscoveryService clusterDiscoveryService(ClientNetworkConfig clientNetworkConfig) {
        return new ClusterDiscoveryService() {
            @Override
            public boolean tryNextCluster(BiPredicate<CandidateClusterContext, CandidateClusterContext> function) {
                return false;
            }

            @Override
            public CandidateClusterContext current() {
                return new CandidateClusterContext("dev", new DefaultAddressProvider(clientNetworkConfig,
                        () -> false),
                        null, null, null, null);
            }

            @Override
            public boolean failoverEnabled() {
                return false;
            }
        };
    }

    private static LifecycleServiceImpl lifecycleServiceImpl() {
        LifecycleServiceImpl lifecycleService = new LifecycleServiceImpl(null, "test",
                new ClientConfig(), mock(ILogger.class)) {
            @Override
            protected void onGracefulShutdown() {

            }

            @Override
            protected void onClientShutdown() {
            }
        };
        lifecycleService.start();
        return lifecycleService;
    }

    private static Authenticator authenticator(ConcurrentHashMap<UUID, MemberContext> members) {
        return connection -> {
            CompletableFuture<ClientMessage> future = new CompletableFuture<>();
            Address initAddress = connection.getInitAddress();
            MemberContext memberContext = members.values().stream().
                    filter(m -> m.member.getAddress().equals(initAddress)).
                    findFirst().get();
            ClientMessage message = ClientAuthenticationCodec.encodeResponse(AuthenticationStatus.AUTHENTICATED.getId(),
                    initAddress, memberContext.member.getUuid(), (byte) 1, "1.0.0", 271,
                    memberContext.clusterId, true);

            future.complete(message);
            return future;
        };
    }

    private static ClientMemberListProvider memberListProvider(ConcurrentHashMap<UUID, MemberContext> contextMap) {
        return new ClientMemberListProvider() {
            @Override
            public Member getMember(@Nonnull UUID uuid) {
                return contextMap.get(uuid).member;
            }

            @Override
            public Collection<Member> getMemberList() {
                return contextMap.values().stream().map(context -> context.member).collect(Collectors.toList());
            }

            @Override
            public void waitInitialMemberListFetched() {

            }
        };
    }
}
