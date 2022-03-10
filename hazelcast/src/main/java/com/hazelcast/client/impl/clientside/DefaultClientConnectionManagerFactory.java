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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.connection.tcp.Authenticator;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.PasswordCredentials;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.EOFException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;
import static com.hazelcast.client.properties.ClientProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.client.properties.ClientProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_OUTPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_WRITE_THROUGH_ENABLED;
import static com.hazelcast.internal.util.ThreadAffinity.newSystemThreadAffinity;

public class DefaultClientConnectionManagerFactory implements ClientConnectionManagerFactory {

    private static final int DEFAULT_SMART_CLIENT_THREAD_COUNT = 3;
    private static final int SMALL_MACHINE_PROCESSOR_COUNT = 8;

    public DefaultClientConnectionManagerFactory() {
    }

    @Override
    public ClientConnectionManager createConnectionManager(HazelcastClientInstanceImpl client) {
        ClientConfig clientConfig = client.getClientConfig();
        HazelcastProperties properties = client.getProperties();
        boolean failoverEnabled = client.getFailoverConfig() != null;
        ClusterDiscoveryService clusterDiscoveryService = client.getClusterDiscoveryService();
        NioNetworking networking = createNetworking(properties, clientConfig,
                client.getLoggingService(), client.getMetricsRegistry(),
                client.getName(), client.getConcurrencyDetection());
        return new TcpClientConnectionManager(client.getLoggingService(), clientConfig, properties, failoverEnabled,
                clusterDiscoveryService, client.getName(), networking,
                (LifecycleServiceImpl) client.getLifecycleService(),
                client.getClientClusterService(), client, createAuthenticator(client));
    }

    public static Authenticator createAuthenticator(HazelcastClientInstanceImpl client) {
        Set<String> labels = Collections.unmodifiableSet(client.getClientConfig().getLabels());
        String connectionType = client.getProperties().getBoolean(MC_CLIENT_MODE_PROP)
                ? ConnectionType.MC_JAVA_CLIENT : ConnectionType.JAVA_CLIENT;
        UUID clientUUID = client.getClientUUID();
        return connection -> {
            Address memberAddress = connection.getInitAddress();
            ClientMessage request = encodeAuthenticationRequest(client, memberAddress, labels, connectionType,
                    clientUUID);
            return new ClientInvocation(client, request, null, connection).invokeUrgent();
        };
    }

    private static ClientMessage encodeAuthenticationRequest(HazelcastClientInstanceImpl client, Address toAddress,
                                                             Set<String> labels, String connectionType, UUID clientUUID) {
        InternalSerializationService ss = client.getSerializationService();
        byte serializationVersion = ss.getVersion();

        CandidateClusterContext currentContext = client.getClusterDiscoveryService().current();
        Credentials credentials = currentContext.getCredentialsFactory().newCredentials(toAddress);
        String clusterName = currentContext.getClusterName();
        AtomicReference<Credentials> credentialsReference = client.getCredentialsReference();
        credentialsReference.set(credentials);

        if (credentials instanceof PasswordCredentials) {
            PasswordCredentials cr = (PasswordCredentials) credentials;
            return ClientAuthenticationCodec
                    .encodeRequest(clusterName, cr.getName(), cr.getPassword(), clientUUID, connectionType,
                            serializationVersion, BuildInfoProvider.getBuildInfo().getVersion(), client.getName(), labels);
        } else {
            byte[] secretBytes;
            if (credentials instanceof TokenCredentials) {
                secretBytes = ((TokenCredentials) credentials).getToken();
            } else {
                secretBytes = ss.toData(credentials).toByteArray();
            }
            return ClientAuthenticationCustomCodec.encodeRequest(clusterName, secretBytes, clientUUID, connectionType,
                    serializationVersion, BuildInfoProvider.getBuildInfo().getVersion(), client.getName(), labels);
        }
    }


    private NioNetworking createNetworking(HazelcastProperties properties, ClientConfig config,
                                           LoggingService loggingService, MetricsRegistry metricsRegistry,
                                           String clientName, ConcurrencyDetection concurrencyDetection) {
        boolean isSmartRoutingEnabled = config.getNetworkConfig().isSmartRouting();
        int configuredInputThreads = properties.getInteger(IO_INPUT_THREAD_COUNT);
        int configuredOutputThreads = properties.getInteger(IO_OUTPUT_THREAD_COUNT);

        int inputThreads;
        if (configuredInputThreads == -1) {
            if (isSmartRoutingEnabled && RuntimeAvailableProcessors.get() > SMALL_MACHINE_PROCESSOR_COUNT) {
                inputThreads = DEFAULT_SMART_CLIENT_THREAD_COUNT;
            } else {
                inputThreads = 1;
            }
        } else {
            inputThreads = configuredInputThreads;
        }

        int outputThreads;
        if (configuredOutputThreads == -1) {
            if (isSmartRoutingEnabled && RuntimeAvailableProcessors.get() > SMALL_MACHINE_PROCESSOR_COUNT) {
                outputThreads = DEFAULT_SMART_CLIENT_THREAD_COUNT;
            } else {
                outputThreads = 1;
            }
        } else {
            outputThreads = configuredOutputThreads;
        }

        ILogger logger = loggingService.getLogger(ClientConnectionManager.class);
        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(loggingService)
                        .metricsRegistry(metricsRegistry)
                        .threadNamePrefix(clientName)
                        .errorHandler(new ClientConnectionChannelErrorHandler(logger))
                        .inputThreadCount(inputThreads)
                        .inputThreadAffinity(newSystemThreadAffinity("hazelcast.client.io.input.thread.affinity"))
                        .outputThreadCount(outputThreads)
                        .outputThreadAffinity(newSystemThreadAffinity("hazelcast.client.io.output.thread.affinity"))
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .writeThroughEnabled(properties.getBoolean(IO_WRITE_THROUGH_ENABLED))
                        .concurrencyDetection(concurrencyDetection)
        );
    }

    private static final class ClientConnectionChannelErrorHandler implements ChannelErrorHandler {

        private final ILogger logger;

        private ClientConnectionChannelErrorHandler(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void onError(Channel channel, Throwable cause) {
            if (channel == null) {
                logger.severe(cause);
            } else {
                if (cause instanceof OutOfMemoryError) {
                    logger.severe(cause);
                }

                Connection connection = (Connection) channel.attributeMap().get(TcpClientConnection.class);
                if (cause instanceof EOFException) {
                    connection.close("Connection closed by the other side", cause);
                } else {
                    connection.close("Exception in " + connection + ", thread=" + Thread.currentThread().getName(), cause);
                }
            }
        }
    }

}
