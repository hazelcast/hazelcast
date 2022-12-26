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

package com.hazelcast.internal.bootstrap;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.tpc.TPCSocketConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.TpcEngine;
import com.hazelcast.internal.tpc.nio.NioAsyncReadHandler;
import com.hazelcast.internal.tpc.nio.NioAsyncServerSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.AddressUtil.transformPortDefinitionsToPorts;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("checkstyle:MagicNumber, checkstyle:")
public class TpcServerBootstrap {
    private static final int TERMINATE_TIMEOUT_SECONDS = 5;

    public volatile boolean shutdown;

    private final NodeEngineImpl nodeEngine;
    private final InternalSerializationService ss;
    private final ILogger logger;
    private final Address thisAddress;
    private final TpcEngine tpcEngine;
    private final boolean writeThrough;
    private final boolean regularSchedule;
    private final boolean tcpNoDelay = true;
    private final boolean enabled;
    private final Map<Eventloop, Supplier<? extends ReadHandler>> readHandlerSuppliers = new HashMap<>();
    private final List<AsyncServerSocket> serverSockets = new ArrayList<>();
    private volatile List<Integer> clientPorts;

    public TpcServerBootstrap(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(TpcServerBootstrap.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.enabled = Boolean.parseBoolean(getProperty("hazelcast.tpc.enabled", "false"))
                || nodeEngine.getConfig().getTpcEngineConfig().isEnabled();
        logger.info("TPC: " + (enabled ? "enabled" : "disabled"));
        this.writeThrough = Boolean.parseBoolean(getProperty("hazelcast.tpc.write-through", "false"));
        this.regularSchedule = Boolean.parseBoolean(getProperty("hazelcast.tpc.regular-schedule", "true"));
        this.thisAddress = nodeEngine.getThisAddress();
        this.tpcEngine = newTpcEngine();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TpcEngine getTpcEngine() {
        return tpcEngine;
    }

    public List<Integer> getClientPorts() {
        return clientPorts;
    }

    private TpcEngine newTpcEngine() {
        if (!enabled) {
            return null;
        }

        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        NioEventloop.NioConfiguration eventloopConfiguration = new NioEventloop.NioConfiguration();
        eventloopConfiguration.setThreadFactory(AltoEventloopThread::new);
        configuration.setEventloopConfiguration(eventloopConfiguration);
        configuration.setEventloopCount(nodeEngine.getConfig().getTpcEngineConfig().getEventloopCount());
        return new TpcEngine(configuration);
    }

    public void start() {
        if (!enabled) {
            return;
        }

        logger.info("Starting TpcServerBootstrap");
        tpcEngine.start();

        Eventloop.Type eventloopType = tpcEngine.eventloopType();
        switch (eventloopType) {
            case NIO:
                startNio();
                break;
            default:
                throw new IllegalStateException("Unknown eventloopType:" + eventloopType);
        }

        clientPorts = serverSockets.stream().map(AsyncServerSocket::localPort).collect(Collectors.toList());
    }

    private void startNio() {
        TPCSocketConfig clientSocketConfig = getClientSocketConfig();

        if (clientSocketConfig == null) {
            // advanced network is enabled yet there is no configured server socket for clients
            return;
        }

        List<Integer> clientAllowedPorts = new ArrayList<>(
                transformPortDefinitionsToPorts(clientSocketConfig.getPortDefinitions(), new LinkedHashSet<>()));

        int clientPortIndex = 0;
        for (int k = 0; k < tpcEngine.eventloopCount(); k++) {
            NioEventloop eventloop = (NioEventloop) tpcEngine.eventloop(k);

            Supplier<NioAsyncReadHandler> readHandlerSupplier =
                    () -> new ClientNioAsyncReadHandler(nodeEngine.getNode().clientEngine);
            readHandlerSuppliers.put(eventloop, readHandlerSupplier);

            NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(eventloop);
            serverSockets.add(serverSocket);
            int receiveBufferSize = clientSocketConfig.getReceiveBufferSize();
            int sendBufferSize = clientSocketConfig.getSendBufferSize();
            serverSocket.receiveBufferSize(receiveBufferSize);
            serverSocket.reuseAddress(true);
            clientPortIndex = bind(serverSocket, clientAllowedPorts, clientPortIndex);
            serverSocket.accept(socket -> {
                socket.readHandler(readHandlerSuppliers.get(eventloop).get());
                socket.setWriteThrough(writeThrough);
                socket.setRegularSchedule(regularSchedule);
                socket.sendBufferSize(sendBufferSize);
                socket.receiveBufferSize(receiveBufferSize);
                socket.tcpNoDelay(tcpNoDelay);
                socket.keepAlive(true);
                socket.activate(eventloop);
            });
        }
    }

    // public for testing
    public TPCSocketConfig getClientSocketConfig() {
        if (nodeEngine.getConfig().getAdvancedNetworkConfig().isEnabled()) {
            ServerSocketEndpointConfig endpointConfig = (ServerSocketEndpointConfig) nodeEngine
                    .getConfig()
                    .getAdvancedNetworkConfig()
                    .getEndpointConfigs()
                    .get(EndpointQualifier.CLIENT);

            if (endpointConfig == null) {
                return null;
            }

            return endpointConfig.getTpcSocketConfig();
        }

        // unified socket
        return nodeEngine.getConfig().getNetworkConfig().getTpcSocketConfig();
    }

    private int bind(NioAsyncServerSocket serverSocket, List<Integer> allowedPorts, int index) {
        try {
            serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), allowedPorts.get(index)));
            return index + 1;
        } catch (BindException e) {
            return bind(serverSocket, allowedPorts, index + tpcEngine.eventloopCount());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (IndexOutOfBoundsException e) {
            throw new HazelcastException("Allowed TPC ports weren't enough.");
        }
    }

    public void shutdown() {
        if (!enabled) {
            return;
        }

        logger.info("TcpBootstrap shutdown");

        shutdown = true;
        tpcEngine.shutdown();

        try {
            tpcEngine.awaitTermination(TERMINATE_TIMEOUT_SECONDS, SECONDS);
        } catch (InterruptedException e) {
            logger.warning("TpcEngine failed to terminate.");
            Thread.currentThread().interrupt();
        }

        logger.info("TcpBootstrap terminated");
    }
}
