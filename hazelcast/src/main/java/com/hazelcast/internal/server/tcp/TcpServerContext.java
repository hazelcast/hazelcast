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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.net.Socket;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.instance.EndpointQualifier.REST;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;

@SuppressWarnings({"checkstyle:methodcount"})
public class TcpServerContext implements ServerContext {

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final RestApiConfig restApiConfig;
    private final MemcacheProtocolConfig memcacheProtocolConfig;

    public TcpServerContext(Node node, NodeEngineImpl nodeEngine) {
        this.node = node;
        this.nodeEngine = nodeEngine;
        this.restApiConfig = initRestApiConfig(node.getConfig());
        this.memcacheProtocolConfig = initMemcacheProtocolConfig(node.getConfig());
    }

    private static RestApiConfig initRestApiConfig(Config config) {
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        boolean isAdvancedNetwork = advancedNetworkConfig.isEnabled();
        RestApiConfig restApiConfig = config.getNetworkConfig().getRestApiConfig();

        if (isAdvancedNetwork && advancedNetworkConfig.getEndpointConfigs().get(REST) != null) {
            RestServerEndpointConfig restServerEndpointConfig = advancedNetworkConfig.getRestEndpointConfig();
            restApiConfig.setEnabled(true).setEnabledGroups(restServerEndpointConfig.getEnabledGroups());
        }

        return restApiConfig;
    }

    private static MemcacheProtocolConfig initMemcacheProtocolConfig(Config config) {
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        boolean isAdvancedNetwork = advancedNetworkConfig.isEnabled();

        if (isAdvancedNetwork && config.getAdvancedNetworkConfig().getEndpointConfigs().get(MEMCACHE) != null) {
            return new MemcacheProtocolConfig().setEnabled(true);
        }

        return config.getNetworkConfig().getMemcacheProtocolConfig();
    }

    @Override
    public HazelcastProperties properties() {
        return node.getProperties();
    }

    @Override
    public String getHazelcastName() {
        return node.hazelcastInstance.getName();
    }

    @Override
    public LoggingService getLoggingService() {
        return nodeEngine.getLoggingService();
    }

    @Override
    public boolean isNodeActive() {
        return node.getState() != NodeState.SHUT_DOWN;
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    public UUID getThisUuid() {
        return node.getThisUuid();
    }

    @Override
    public Map<EndpointQualifier, Address> getThisAddresses() {
        return nodeEngine.getLocalMember().getAddressMap();
    }

    @Override
    public void onFatalError(Exception e) {
        String hzName = nodeEngine.getHazelcastInstance().getName();
        Thread thread = new Thread(createThreadName(hzName, "io.error.shutdown")) {
            public void run() {
                node.shutdown(false);
            }
        };
        thread.start();
    }

    public SocketInterceptorConfig getSocketInterceptorConfig(EndpointQualifier endpointQualifier) {
        final AdvancedNetworkConfig advancedNetworkConfig = node.getConfig().getAdvancedNetworkConfig();
        if (advancedNetworkConfig.isEnabled()) {
            EndpointConfig config = advancedNetworkConfig.getEndpointConfigs().get(endpointQualifier);
            return config != null ? config.getSocketInterceptorConfig() : null;
        }

        return node.getConfig().getNetworkConfig().getSocketInterceptorConfig();
    }

    @Override
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig(EndpointQualifier endpointQualifier) {
        final AdvancedNetworkConfig advancedNetworkConfig = node.getConfig().getAdvancedNetworkConfig();
        if (advancedNetworkConfig.isEnabled()) {
            EndpointConfig config = advancedNetworkConfig.getEndpointConfigs().get(endpointQualifier);
            return config != null ? config.getSymmetricEncryptionConfig() : null;
        }

        return node.getConfig().getNetworkConfig().getSymmetricEncryptionConfig();
    }

    @Override
    public ClientEngine getClientEngine() {
        return node.clientEngine;
    }

    @Override
    public TextCommandService getTextCommandService() {
        return node.getTextCommandService();
    }

    @Override
    public void removeEndpoint(Address endpointAddress) {
        nodeEngine.getExecutionService().execute(ExecutionService.IO_EXECUTOR,
                () -> node.clusterService.suspectAddressIfNotConnected(endpointAddress));
    }

    @Override
    public void onDisconnect(final Address endpoint, Throwable cause) {
        if (cause == null) {
            // connection is closed explicitly. we should not attempt to reconnect
            return;
        }

        if (node.clusterService.getMember(endpoint) != null) {
            nodeEngine.getExecutionService().execute(ExecutionService.IO_EXECUTOR, new ReconnectionTask(endpoint));
        }
    }

    @Override
    public void onSuccessfulConnection(Address address) {
        if (!node.getClusterService().isJoined()) {
            node.getJoiner().unblacklist(address);
        }
    }

    @Override
    public void onFailedConnection(final Address address) {
        ClusterService clusterService = node.clusterService;
        if (clusterService.isJoined()) {
            if (clusterService.getMember(address) != null) {
                nodeEngine.getExecutionService().schedule(ExecutionService.IO_EXECUTOR, new ReconnectionTask(address),
                        getConnectionMonitorInterval(), TimeUnit.MILLISECONDS);
            }
        } else {
            node.getJoiner().blacklist(address, false);
        }
    }

    @Override
    public void shouldConnectTo(Address address) {
        UUID memberUuid = node.getLocalAddressRegistry().uuidOf(address);
        if (memberUuid != null && memberUuid.equals(node.getThisUuid())) {
            throw new RuntimeException("Connecting to self! " + address);
        }
    }

    @Override
    public void interceptSocket(EndpointQualifier endpointQualifier, Socket socket, boolean onAccept) throws IOException {
        socket.getChannel().configureBlocking(true);

        if (!isSocketInterceptorEnabled(endpointQualifier)) {
            return;
        }

        MemberSocketInterceptor memberSocketInterceptor = getSocketInterceptor(endpointQualifier);
        if (memberSocketInterceptor == null) {
            return;
        }

        if (onAccept) {
            memberSocketInterceptor.onAccept(socket);
        } else {
            memberSocketInterceptor.onConnect(socket);
        }
    }

    @Override
    public boolean isSocketInterceptorEnabled(EndpointQualifier endpointQualifier) {
        final SocketInterceptorConfig socketInterceptorConfig = getSocketInterceptorConfig(endpointQualifier);
        return socketInterceptorConfig != null && socketInterceptorConfig.isEnabled();
    }

    @Override
    public int getSocketConnectTimeoutSeconds(EndpointQualifier endpointQualifier) {
        final AdvancedNetworkConfig advancedNetworkConfig = node.getConfig().getAdvancedNetworkConfig();
        if (advancedNetworkConfig.isEnabled()) {
            EndpointConfig config = advancedNetworkConfig.getEndpointConfigs().get(endpointQualifier);
            return config != null ? config.getSocketConnectTimeoutSeconds() : 0;
        }

        return node.getProperties().getSeconds(ClusterProperty.SOCKET_CONNECT_TIMEOUT_SECONDS);
    }

    @Override
    public long getConnectionMonitorInterval() {
        return node.getProperties().getMillis(ClusterProperty.CONNECTION_MONITOR_INTERVAL);
    }

    @Override
    public int getConnectionMonitorMaxFaults() {
        return node.getProperties().getInteger(ClusterProperty.CONNECTION_MONITOR_MAX_FAULTS);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Void> submitAsync(final Runnable runnable) {
        return (Future<Void>) nodeEngine.getExecutionService().submit(ExecutionService.IO_EXECUTOR, runnable);
    }

    @Override
    public EventService getEventService() {
        return nodeEngine.getEventService();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return node.getSerializationService();
    }

    @Override
    public MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier) {
        return node.getNodeExtension().getSocketInterceptor(endpointQualifier);
    }

    @Override
    public InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, ServerConnection connection) {
        return node.getNodeExtension().createInboundHandlers(qualifier, connection, this);
    }

    @Override
    public OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, ServerConnection connection) {
        return node.getNodeExtension().createOutboundHandlers(qualifier, connection, this);
    }

    @Override
    public Collection<Integer> getOutboundPorts(EndpointQualifier endpointQualifier) {
        final AdvancedNetworkConfig advancedNetworkConfig = node.getConfig().getAdvancedNetworkConfig();
        if (advancedNetworkConfig.isEnabled()) {
            EndpointConfig endpointConfig = advancedNetworkConfig.getEndpointConfigs().get(endpointQualifier);
            final Collection<Integer> outboundPorts = endpointConfig != null
                    ? endpointConfig.getOutboundPorts() : Collections.<Integer>emptyList();
            final Collection<String> outboundPortDefinitions = endpointConfig != null
                    ? endpointConfig.getOutboundPortDefinitions() : Collections.<String>emptyList();
            return AddressUtil.getOutboundPorts(outboundPorts, outboundPortDefinitions);
        }

        final NetworkConfig networkConfig = node.getConfig().getNetworkConfig();
        final Collection<Integer> outboundPorts = networkConfig.getOutboundPorts();
        final Collection<String> outboundPortDefinitions = networkConfig.getOutboundPortDefinitions();
        return AddressUtil.getOutboundPorts(outboundPorts, outboundPortDefinitions);
    }

    private class ReconnectionTask implements Runnable {
        private final Address endpoint;

        ReconnectionTask(Address endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void run() {
            ClusterServiceImpl clusterService = node.clusterService;
            if (clusterService.getMember(endpoint) != null) {
                node.getServer().getConnectionManager(MEMBER).getOrConnect(endpoint);
            }
        }
    }

    @Override
    public RestApiConfig getRestApiConfig() {
        return restApiConfig;
    }

    @Override
    public MemcacheProtocolConfig getMemcacheProtocolConfig() {
        return memcacheProtocolConfig;
    }

    @Override
    public AuditlogService getAuditLogService() {
        return node.getNodeExtension().getAuditlogService();
    }
}
