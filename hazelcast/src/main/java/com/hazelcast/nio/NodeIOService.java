/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.networking.IOOutOfMemoryHandler;
import com.hazelcast.internal.networking.ReadHandler;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;
import com.hazelcast.internal.networking.WriteHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@PrivateApi
public class NodeIOService implements IOService {

    private final Node node;
    private final NodeEngineImpl nodeEngine;

    public NodeIOService(Node node, NodeEngineImpl nodeEngine) {
        this.node = node;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public HazelcastThreadGroup getHazelcastThreadGroup() {
        return nodeEngine.getHazelcastThreadGroup();
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
    public IOOutOfMemoryHandler getIoOutOfMemoryHandler() {
        return new IOOutOfMemoryHandler() {
            @Override
            public void handle(OutOfMemoryError error) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(error);
            }
        };
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    public void onFatalError(Exception e) {
        HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();
        Thread thread = new Thread(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("io.error.shutdown")) {
            public void run() {
                node.shutdown(false);
            }
        };
        thread.start();
    }

    @Override
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
    public void handleClientMessage(ClientMessage cm, Connection connection) {
        node.clientEngine.handleClientMessage(cm, connection);
    }

    @Override
    public TextCommandService getTextCommandService() {
        return node.getTextCommandService();
    }

    @Override
    public boolean isMemcacheEnabled() {
        return node.getProperties().getBoolean(GroupProperty.MEMCACHE_ENABLED);
    }

    @Override
    public boolean isRestEnabled() {
        return node.getProperties().getBoolean(GroupProperty.REST_ENABLED);
    }

    @Override
    public boolean isHealthcheckEnabled() {
        return node.getProperties().getBoolean(GroupProperty.HTTP_HEALTHCHECK_ENABLED);
    }

    @Override
    public void removeEndpoint(final Address endPoint) {
        nodeEngine.getExecutionService().execute(ExecutionService.IO_EXECUTOR, new Runnable() {
            @Override
            public void run() {
                // we can safely pass null because removeEndpoint is triggered from the connectionManager after
                // the connection is closed. So a reason is already set.
                node.clusterService.removeAddress(endPoint, null);
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
        if (!node.joined()) {
            node.getJoiner().unblacklist(address);
        }
    }

    @Override
    public void onFailedConnection(final Address address) {
        if (!node.joined()) {
            node.getJoiner().blacklist(address, false);
        } else {
            if (node.clusterService.getMember(address) != null) {
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
    public int getSocketReceiveBufferSize() {
        return node.getProperties().getInteger(GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE);
    }

    @Override
    public int getSocketSendBufferSize() {
        return node.getProperties().getInteger(GroupProperty.SOCKET_SEND_BUFFER_SIZE);
    }

    @Override
    public boolean isSocketBufferDirect() {
        return node.getProperties().getBoolean(GroupProperty.SOCKET_BUFFER_DIRECT);
    }

    @Override
    public int getSocketClientReceiveBufferSize() {
        int clientSendBuffer = node.getProperties().getInteger(GroupProperty.SOCKET_CLIENT_RECEIVE_BUFFER_SIZE);
        return clientSendBuffer != -1 ? clientSendBuffer : getSocketReceiveBufferSize();
    }

    @Override
    public int getSocketClientSendBufferSize() {
        int clientReceiveBuffer = node.getProperties().getInteger(GroupProperty.SOCKET_CLIENT_SEND_BUFFER_SIZE);
        return clientReceiveBuffer != -1 ? clientReceiveBuffer : getSocketReceiveBufferSize();
    }

    @Override
    public int getSocketLingerSeconds() {
        return node.getProperties().getSeconds(GroupProperty.SOCKET_LINGER_SECONDS);
    }

    @Override
    public int getSocketConnectTimeoutSeconds() {
        return node.getProperties().getSeconds(GroupProperty.SOCKET_CONNECT_TIMEOUT_SECONDS);
    }

    @Override
    public boolean getSocketKeepAlive() {
        return node.getProperties().getBoolean(GroupProperty.SOCKET_KEEP_ALIVE);
    }

    @Override
    public boolean getSocketNoDelay() {
        return node.getProperties().getBoolean(GroupProperty.SOCKET_NO_DELAY);
    }

    @Override
    public int getInputSelectorThreadCount() {
        return node.getProperties().getInteger(GroupProperty.IO_INPUT_THREAD_COUNT);
    }

    @Override
    public int getOutputSelectorThreadCount() {
        return node.getProperties().getInteger(GroupProperty.IO_OUTPUT_THREAD_COUNT);
    }

    @Override
    public boolean isClient() {
        return false;
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
    public int getBalancerIntervalSeconds() {
        return node.getProperties().getSeconds(GroupProperty.IO_BALANCER_INTERVAL_SECONDS);
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
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        return node.getNodeExtension().getSocketChannelWrapperFactory();
    }

    @Override
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return node.getNodeExtension().getMemberSocketInterceptor();
    }

    @Override
    public ReadHandler createReadHandler(TcpIpConnection connection) {
        return node.getNodeExtension().createReadHandler(connection, this);
    }

    @Override
    public WriteHandler createWriteHandler(TcpIpConnection connection) {
        return node.getNodeExtension().createWriteHandler(connection, this);
    }

    @Override
    public Collection<Integer> getOutboundPorts() {
        final NetworkConfig networkConfig = node.getConfig().getNetworkConfig();
        final Collection<String> portDefinitions = getPortDefinitions(networkConfig);
        final Set<Integer> ports = getPorts(networkConfig);
        if (portDefinitions.isEmpty() && ports.isEmpty()) {
            // means any port
            return Collections.emptySet();
        }
        if (portDefinitions.contains("*") || portDefinitions.contains("0")) {
            // means any port
            return Collections.emptySet();
        }
        transformPortDefinitionsToPorts(portDefinitions, ports);
        if (ports.contains(0)) {
            // means any port
            return Collections.emptySet();
        }
        return ports;
    }

    private void transformPortDefinitionsToPorts(Collection<String> portDefinitions, Set<Integer> ports) {
        // not checking port ranges...
        for (String portDef : portDefinitions) {
            String[] portDefs = portDef.split("[,; ]");
            for (String def : portDefs) {
                def = def.trim();
                if (def.isEmpty()) {
                    continue;
                }
                final int dashPos = def.indexOf('-');
                if (dashPos > 0) {
                    final int start = Integer.parseInt(def.substring(0, dashPos));
                    final int end = Integer.parseInt(def.substring(dashPos + 1));
                    for (int port = start; port <= end; port++) {
                        ports.add(port);
                    }
                } else {
                    ports.add(Integer.parseInt(def));
                }
            }
        }
    }

    private Set<Integer> getPorts(NetworkConfig networkConfig) {
        return networkConfig.getOutboundPorts() == null
                ? new HashSet<Integer>() : new HashSet<Integer>(networkConfig.getOutboundPorts());
    }

    private Collection<String> getPortDefinitions(NetworkConfig networkConfig) {
        return networkConfig.getOutboundPortDefinitions() == null
                ? Collections.<String>emptySet() : networkConfig.getOutboundPortDefinitions();
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
}

