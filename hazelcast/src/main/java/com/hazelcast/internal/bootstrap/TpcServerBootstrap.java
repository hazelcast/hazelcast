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
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.alto.AltoSocketConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Configuration;
import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.TpcEngine;
import com.hazelcast.internal.tpc.nio.NioReactorBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.AltoOperationScheduler;
import com.hazelcast.spi.impl.operationexecutor.impl.AltoPartitionOperationThread;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.internal.server.ServerContext.KILO_BYTE;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_KEEPALIVE;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_REUSEPORT;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.TCP_NODELAY;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("checkstyle:MagicNumber, checkstyle:")
public class TpcServerBootstrap {
    public static final HazelcastProperty ALTO_ENABLED = new HazelcastProperty(
            "hazelcast.internal.alto.enabled");
    public static final HazelcastProperty ALTO_EVENTLOOP_COUNT = new HazelcastProperty(
            "hazelcast.internal.alto.eventloop.count");
    private static final int TERMINATE_TIMEOUT_SECONDS = 5;

    // todo: nothing is done with this.
    private volatile boolean shutdown;
    private final NodeEngineImpl nodeEngine;
    private final InternalSerializationService ss;
    private final ILogger logger;
    private final Address thisAddress;
    private TpcEngine tpcEngine;
    private final boolean tcpNoDelay = true;
    private final boolean enabled;
    private final Map<Reactor, Supplier<? extends ReadHandler>> readHandlerSuppliers = new HashMap<>();
    private final List<AsyncServerSocket> serverSockets = new ArrayList<>();
    private final Config config;
    private volatile List<Integer> clientPorts;

    public TpcServerBootstrap(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(TpcServerBootstrap.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.config = nodeEngine.getConfig();
        this.enabled = loadAltoEnabled();
        this.thisAddress = nodeEngine.getThisAddress();
    }

    private boolean loadAltoEnabled() {
        boolean enabled;
        String enabledString = nodeEngine.getProperties().getString(ALTO_ENABLED);
        if (enabledString != null) {
            enabled = Boolean.parseBoolean(enabledString);
        } else {
            enabled = config.getAltoConfig().isEnabled();
        }
        logger.info("TPC: " + (enabled ? "enabled" : "disabled"));
        return enabled;
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

        Configuration configuration = new Configuration();
        NioReactorBuilder reactorBuilder = new NioReactorBuilder();
        reactorBuilder.setThreadFactory(new ThreadFactory() {
            int index = 0;

            @Override
            public Thread newThread(Runnable eventloopRunnable) {
                OperationExecutorImpl operationExecutor = (OperationExecutorImpl) nodeEngine
                        .getOperationService()
                        .getOperationExecutor();
                AltoPartitionOperationThread operationThread = (AltoPartitionOperationThread) operationExecutor
                        .getPartitionThreads()[index++];
                operationThread.setEventloopTask(eventloopRunnable);
                return operationThread;
            }
        });

        reactorBuilder.setSchedulerSupplier(() -> new AltoOperationScheduler(1));
        configuration.setReactorBuilder(reactorBuilder);
        configuration.setReactorCount(loadEventloopCount());
        return new TpcEngine(configuration);
    }

    public int eventloopCount() {
        return loadEventloopCount();
    }

    private int loadEventloopCount() {
        int eventloopCount;
        String eventloopCountString = nodeEngine.getProperties().getString(ALTO_EVENTLOOP_COUNT);
        if (eventloopCountString != null) {
            eventloopCount = Integer.parseInt(eventloopCountString);
        } else {
            eventloopCount = config.getAltoConfig().getEventloopCount();
        }
        return eventloopCount;
    }

    public void start() {
        if (!enabled) {
            return;
        }
        this.tpcEngine = newTpcEngine();

        // The AltoPartitionOperationThread are created with the right AltoOperationQueue, but
        // the reactor isn't set yet.
        // The tpcEngine (and hence reactor.start) will create the appropriate happens-before
        // edge between the main thread and the reactor thread. So it is guaranteed to see
        // the reactor.
        OperationExecutorImpl operationExecutor = (OperationExecutorImpl) nodeEngine
                .getOperationService()
                .getOperationExecutor();
        for (int k = 0; k < operationExecutor.getPartitionThreadCount(); k++) {
            Reactor reactor = tpcEngine.reactor(k);
            AltoPartitionOperationThread partitionThread = (AltoPartitionOperationThread) operationExecutor
                    .getPartitionThreads()[k];
            partitionThread.getQueue().setReactor(reactor);
        }

        logger.info("Starting TpcServerBootstrap");
        tpcEngine.start();
        openServerSockets();
        clientPorts = serverSockets.stream().map(AsyncServerSocket::getLocalPort).collect(Collectors.toList());
    }

    private void openServerSockets() {
        AltoSocketConfig clientSocketConfig = getClientSocketConfig();

        String[] range = clientSocketConfig.getPortRange().split("-");
        int port = Integer.parseInt(range[0]);
        int limit = Integer.parseInt(range[1]);

        for (int k = 0; k < tpcEngine.reactorCount(); k++) {
            Reactor reactor = tpcEngine.reactor(k);

            Supplier<ReadHandler> readHandlerSupplier =
                    () -> new ClientAsyncReadHandler(nodeEngine.getNode().clientEngine);
            readHandlerSuppliers.put(reactor, readHandlerSupplier);

            AsyncServerSocket serverSocket = reactor.newAsyncServerSocketBuilder()
                    .set(SO_RCVBUF, clientSocketConfig.getReceiveBufferSizeKB() * KILO_BYTE)
                    .set(SO_REUSEPORT, true)
                    .setAcceptConsumer(acceptRequest -> {
                        AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                                .setReadHandler(readHandlerSuppliers.get(reactor).get())
                                .set(SO_SNDBUF, clientSocketConfig.getSendBufferSizeKB() * KILO_BYTE)
                                .set(SO_RCVBUF, clientSocketConfig.getReceiveBufferSizeKB() * KILO_BYTE)
                                .set(TCP_NODELAY, tcpNoDelay)
                                .set(SO_KEEPALIVE, true)
                                .build();
                        socket.start();
                    })
                    .build();
            serverSockets.add(serverSocket);
            port = bind(serverSocket, port, limit);
            serverSocket.start();
        }
    }

    // public for testing
    public AltoSocketConfig getClientSocketConfig() {
        validateSocketConfig();

        if (config.getAdvancedNetworkConfig().isEnabled()) {
            ServerSocketEndpointConfig endpointConfig = (ServerSocketEndpointConfig) config
                    .getAdvancedNetworkConfig()
                    .getEndpointConfigs()
                    .get(EndpointQualifier.CLIENT);

            return endpointConfig.getAltoSocketConfig();
        }

        // unified socket
        return config.getNetworkConfig().getAltoSocketConfig();
    }

    private void validateSocketConfig() {
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        if (advancedNetworkConfig.isEnabled()) {
            AltoSocketConfig defaultAltoSocketConfig = new AltoSocketConfig();
            Map<EndpointQualifier, EndpointConfig> endpointConfigs = advancedNetworkConfig.getEndpointConfigs();

            endpointConfigs.forEach(((endpointQualifier, endpointConfig) -> {
                if (endpointQualifier != EndpointQualifier.CLIENT
                        && !endpointConfig.getAltoSocketConfig().equals(defaultAltoSocketConfig)) {
                    throw new InvalidConfigurationException(
                            "Alto socket configuration is only available for clients ports for now.");
                }
            }));

            if (endpointConfigs.get(EndpointQualifier.CLIENT) == null) {
                // Advanced network is enabled yet there is no configured server socket
                // for clients. This means cluster will run but no client ports will be
                // created, so no clients can connect to the cluster.
                throw new InvalidConfigurationException("Missing client server socket configuration. "
                        + "If you have enabled Alto and advanced networking, "
                        + "please configure a client server socket.");
            }
        }
    }

    private int bind(AsyncServerSocket serverSocket, int port, int limit) {
        while (port < limit) {
            try {
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                return port + 1;
            } catch (UncheckedIOException e) {
                if (e.getCause() instanceof BindException) {
                    // this port is occupied probably by another hz member, try another one
                    port += tpcEngine.reactorCount();
                } else {
                    throw e;
                }
            } catch (UnknownHostException e) {
                throw new UncheckedIOException(e);
            }
        }

        throw new HazelcastException("Could not find a free port in the Alto socket port range.");
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
