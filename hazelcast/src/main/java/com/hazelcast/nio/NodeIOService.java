/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.net.Socket;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ThreadUtil.createThreadName;

@PrivateApi
public class NodeIOService implements IOService {

    private static final ILogger LOGGER = Logger.getLogger(NodeIOService.class);

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final RestApiConfig restApiConfig;
    private final MemcacheProtocolConfig memcacheProtocolConfig;

    public NodeIOService(Node node, NodeEngineImpl nodeEngine) {
        this.node = node;
        this.nodeEngine = nodeEngine;
        restApiConfig = initRestApiConfig(node.getProperties(), node.getConfig());
        memcacheProtocolConfig = initMemcacheProtocolConfig(node.getProperties(), node.getConfig());
    }

    /**
     * Initializes {@link RestApiConfig} if not provided based on legacy group properties. Also checks (fails fast)
     * if both the {@link RestApiConfig} and system properties are used.
     */
    @SuppressWarnings("deprecation")
    private static RestApiConfig initRestApiConfig(HazelcastProperties properties, Config config) {
        RestApiConfig restApiConfig = config.getRestApiConfig();
        if (restApiConfig != null) {
            // ensure the legacy Hazelcast group properties are not provided
            ensurePropertyNotConfigured(properties, GroupProperty.REST_ENABLED);
            ensurePropertyNotConfigured(properties, GroupProperty.HTTP_HEALTHCHECK_ENABLED);
        } else {
            restApiConfig = new RestApiConfig();
            if (checkDeprecatedProperty(properties, GroupProperty.REST_ENABLED)) {
                restApiConfig.setEnabled(true);
                restApiConfig.enableAllGroups();
            }
            if (checkDeprecatedProperty(properties, GroupProperty.HTTP_HEALTHCHECK_ENABLED)) {
                restApiConfig.setEnabled(true);
                restApiConfig.enableGroups(RestEndpointGroup.HEALTH_CHECK);
            }
        }
        return restApiConfig;
    }

    @SuppressWarnings("deprecation")
    private static MemcacheProtocolConfig initMemcacheProtocolConfig(HazelcastProperties properties, Config config) {
        MemcacheProtocolConfig memcacheProtocolConfig = config.getMemcacheProtocolConfig();
        if (memcacheProtocolConfig != null) {
            // ensure the legacy Hazelcast group property is not provided
            ensurePropertyNotConfigured(properties, GroupProperty.MEMCACHE_ENABLED);
        } else {
            memcacheProtocolConfig = new MemcacheProtocolConfig();
            if (checkDeprecatedProperty(properties, GroupProperty.MEMCACHE_ENABLED)) {
                memcacheProtocolConfig.setEnabled(true);
            }
        }
        return memcacheProtocolConfig;
    }

    private static void ensurePropertyNotConfigured(HazelcastProperties properties, HazelcastProperty hazelcastProperty)
            throws ConfigurationException {
        if (properties.containsKey(hazelcastProperty)) {
            throw new ConfigurationException("Service start failed. The legacy property " + hazelcastProperty.getName()
                    + " is provided together with new Config object. "
                    + "Remove the property from your configuration to fix this issue.");
        }
    }

    private static boolean checkDeprecatedProperty(HazelcastProperties properties, HazelcastProperty hazelcastProperty)
            throws ConfigurationException {
        if (properties.containsKey(hazelcastProperty)) {
            LOGGER.warning("Property " + hazelcastProperty.getName()
                    + " is deprecated. Use configuration object/element instead.");
            return properties.getBoolean(hazelcastProperty);
        }
        return false;
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
    public boolean isActive() {
        return node.getState() != NodeState.SHUT_DOWN;
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
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

    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return node.getConfig().getNetworkConfig().getSocketInterceptorConfig();
    }

    @Override
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return node.getConfig().getNetworkConfig().getSymmetricEncryptionConfig();
    }

    @Override
    public SSLConfig getSSLConfig() {
        return node.getConfig().getNetworkConfig().getSSLConfig();
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
    public void removeEndpoint(final Address endPoint) {
        nodeEngine.getExecutionService().execute(ExecutionService.IO_EXECUTOR, new Runnable() {
            @Override
            public void run() {
                node.clusterService.suspectAddressIfNotConnected(endPoint);
            }
        });
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
        if (!clusterService.isJoined()) {
            node.getJoiner().blacklist(address, false);
        } else {
            if (clusterService.getMember(address) != null) {
                nodeEngine.getExecutionService().schedule(ExecutionService.IO_EXECUTOR, new ReconnectionTask(address),
                        getConnectionMonitorInterval(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public void shouldConnectTo(Address address) {
        if (node.getThisAddress().equals(address)) {
            throw new RuntimeException("Connecting to self! " + address);
        }
    }

    @Override
    public boolean isSocketBind() {
        return node.getProperties().getBoolean(GroupProperty.SOCKET_CLIENT_BIND);
    }

    @Override
    public boolean isSocketBindAny() {
        return node.getProperties().getBoolean(GroupProperty.SOCKET_CLIENT_BIND_ANY);
    }

    @Override
    public void interceptSocket(Socket socket, boolean onAccept) throws IOException {
        socket.getChannel().configureBlocking(true);

        if (!isSocketInterceptorEnabled()) {
            return;
        }

        MemberSocketInterceptor memberSocketInterceptor = getMemberSocketInterceptor();
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
    public boolean isSocketInterceptorEnabled() {
        final SocketInterceptorConfig socketInterceptorConfig = getSocketInterceptorConfig();
        return socketInterceptorConfig != null && socketInterceptorConfig.isEnabled();
    }

    @Override
    public int getSocketConnectTimeoutSeconds() {
        return node.getProperties().getSeconds(GroupProperty.SOCKET_CONNECT_TIMEOUT_SECONDS);
    }

    @Override
    public long getConnectionMonitorInterval() {
        return node.getProperties().getMillis(GroupProperty.CONNECTION_MONITOR_INTERVAL);
    }

    @Override
    public int getConnectionMonitorMaxFaults() {
        return node.getProperties().getInteger(GroupProperty.CONNECTION_MONITOR_MAX_FAULTS);
    }

    @Override
    public void executeAsync(final Runnable runnable) {
        nodeEngine.getExecutionService().execute(ExecutionService.IO_EXECUTOR, runnable);
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
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return node.getNodeExtension().getMemberSocketInterceptor();
    }

    @Override
    public InboundHandler[] createMemberInboundHandlers(TcpIpConnection connection) {
        return node.getNodeExtension().createInboundHandlers(connection, this);
    }

    @Override
    public OutboundHandler[] createMemberOutboundHandlers(TcpIpConnection connection) {
        return node.getNodeExtension().createOutboundHandlers(connection, this);
    }

    @Override
    public Collection<Integer> getOutboundPorts() {
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
                node.connectionManager.getOrConnect(endpoint);
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
}
