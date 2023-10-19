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

package com.hazelcast.internal.tpc;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.TaskQueue;
import com.hazelcast.internal.tpcengine.TpcEngine;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.TpcPartitionOperationThread;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.internal.server.ServerContext.KILO_BYTE;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_KEEPALIVE;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.TCP_NODELAY;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The TpcServerBootstrap is responsible for:
 * <ol>
 *     <li>starting up the {@link TpcEngine}</li>
 *     <li>correct initialization of internal parts like the {@link  TpcPartitionOperationThread}</li>
 *     <li>starting the appropriate server ports for each TPC thread.</li>
 * </ol>
 */
public class TpcServerBootstrap {

    /**
     * If set, overrides {@link com.hazelcast.config.tpc.TpcConfig#isEnabled()}
     */
    public static final HazelcastProperty TPC_ENABLED = new HazelcastProperty(
            "hazelcast.internal.tpc.enabled");

    /**
     * If set, overrides {@link com.hazelcast.config.tpc.TpcConfig#getEventloopCount()}
     */
    public static final HazelcastProperty TPC_EVENTLOOP_COUNT = new HazelcastProperty(
            "hazelcast.internal.tpc.eventloop.count");

    public static final HazelcastProperty TPC_IO_THREAD_COUNT = new HazelcastProperty(
            "hazelcast.internal.tpc.io.thread.count");


    private static final int TERMINATE_TIMEOUT_SECONDS = 5;

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final Address thisAddress;
    private TpcEngine tpcEngine;
    @SuppressWarnings("java:S1170")
    private final boolean tcpNoDelay = true;
    private final boolean enabled;
    private final List<AsyncServerSocket> serverSockets = new ArrayList<>();
    private final Config config;
    private volatile List<Integer> clientPorts;

    public TpcServerBootstrap(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(TpcServerBootstrap.class);
        this.config = nodeEngine.getConfig();
        this.enabled = loadTpcEnabled();
        this.thisAddress = nodeEngine.getThisAddress();
    }

    private boolean loadTpcEnabled() {
        boolean enabled0;
        String enabledString = nodeEngine.getProperties().getString(TPC_ENABLED);
        if (enabledString != null) {
            enabled0 = Boolean.parseBoolean(enabledString);
        } else {
            enabled0 = config.getTpcConfig().isEnabled();
        }
        logger.info("TPC: " + (enabled0 ? "enabled" : "disabled"));
        return enabled0;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TpcEngine getTpcEngine() {
        return tpcEngine;
    }

    public List<Integer> getClientPorts() {
        System.out.println("getClientPorts: " + clientPorts);
        return clientPorts;
    }

    public int eventloopCount() {
        return loadReactorCount() - loadDedicatedIoThreadCount();
    }

    private int loadReactorCount() {
        String eventloopCountString = nodeEngine.getProperties().getString(TPC_EVENTLOOP_COUNT);
        if (eventloopCountString == null) {
            return config.getTpcConfig().getEventloopCount();
        } else {
            return Integer.parseInt(eventloopCountString);
        }
    }

    private int loadDedicatedIoThreadCount() {
        String dedicatedIOThreadCount = nodeEngine.getProperties().getString(TPC_IO_THREAD_COUNT);
        if (dedicatedIOThreadCount == null) {
            return 0;
        } else {
            return Integer.parseInt(dedicatedIOThreadCount);
        }
    }

    public void start() {
        if (!enabled) {
            return;
        }

        logger.info("Starting TpcServerBootstrap");

        int dedicatedIoThreadCount = loadDedicatedIoThreadCount();
        int reactorCount = loadReactorCount();
        logger.info("reactorThreadCount " + reactorCount);
        logger.info("dedicatedIoThreadCount " + dedicatedIoThreadCount);

        TpcEngine.Builder tpcEngineBuilder = new TpcEngine.Builder();
        // The current approach for allowing the OperationThreads to become the reactor threads
        // is done to lower the risk to introduce TPC next to the classic design. But eventually
        // the system needs be be build around the TPC engine.
        tpcEngineBuilder.reactorConfigureFn = new Consumer<>() {
            private int threadIndex;

            @Override
            public void accept(Reactor.Builder reactorBuilder) {
                if (dedicatedIoThreadCount > 0 && threadIndex < dedicatedIoThreadCount) {
                    reactorBuilder.defaultTaskQueueBuilder = new TaskQueue.Builder();
                    reactorBuilder.defaultTaskQueueBuilder.concurrent = true;
                    reactorBuilder.threadName = "ioThread-" + threadIndex;
                } else {
                    OperationExecutorImpl operationExecutor = (OperationExecutorImpl) nodeEngine
                            .getOperationService()
                            .getOperationExecutor();
                    int operationThreadIndex = threadIndex - dedicatedIoThreadCount;
                    TpcPartitionOperationThread operationThread = (TpcPartitionOperationThread) operationExecutor
                            .getPartitionThreads()[operationThreadIndex];

                    reactorBuilder.threadFactory = eventloopTask -> {
                        operationThread.setEventloopTask(eventloopTask);
                        return operationThread;
                    };

                    reactorBuilder.defaultTaskQueueBuilder = new TaskQueue.Builder();
                    reactorBuilder.defaultTaskQueueBuilder.taskRunner = operationThread;
                    reactorBuilder.defaultTaskQueueBuilder.queue = operationThread.getQueue();
                    reactorBuilder.defaultTaskQueueBuilder.concurrent = true;
                }
                threadIndex++;
            }
        };
        tpcEngineBuilder.reactorCount = reactorCount;
        tpcEngine = tpcEngineBuilder.build();
        // The TpcPartitionOperationThread are created with the right TpcOperationQueue, but
        // the reactor isn't set yet.
        // The tpcEngine (and hence reactor.start) will create the appropriate happens-before
        // edge between the main thread and the reactor thread. So it is guaranteed to see
        // the reactor.
        OperationExecutorImpl operationExecutor = (OperationExecutorImpl) nodeEngine
                .getOperationService()
                .getOperationExecutor();

        int partitionThreadIndex = 0;
        for (int k = dedicatedIoThreadCount; k < reactorCount; k++) {
            Reactor reactor = tpcEngine.reactor(k);
            TpcPartitionOperationThread partitionThread = (TpcPartitionOperationThread) operationExecutor
                    .getPartitionThreads()[partitionThreadIndex];
             partitionThread.getQueue().setTaskQueue(reactor.defaultTaskQueue());

            partitionThreadIndex++;
        }

        tpcEngine.start();
        setupServerSocketsForClients();
        clientPorts = serverSockets.stream().map(AsyncServerSocket::getLocalPort).collect(Collectors.toList());
    }

    private void setupServerSocketsForClients() {
        TpcSocketConfig socketConfig = getClientSocketConfig();

        String[] range = socketConfig.getPortRange().split("-");
        int port = Integer.parseInt(range[0]);
        int limit = Integer.parseInt(range[1]);

        int dedicatedIoThreadCount = loadDedicatedIoThreadCount();
        int sockets;
        if (dedicatedIoThreadCount == 0) {
            sockets = tpcEngine.reactorCount();
        } else {
            sockets = dedicatedIoThreadCount;
        }

        logger.info("Opening " + sockets + " sockets.");

        // Currently we only open the sockets for clients. But in the future we also need to
        // open sockets for members, WAN replication etc.
        for (int k = 0; k < sockets; k++) {
            Reactor reactor = tpcEngine.reactor(k);

            AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
            serverSocketBuilder.bindAddressGenerator = new BindAddressGenerator(port, limit);
            // for window scaling to work, this property needs to be set
            serverSocketBuilder.options.set(SO_RCVBUF, socketConfig.getReceiveBufferSizeKB() * KILO_BYTE);
            serverSocketBuilder.acceptFn = acceptRequest -> {
                AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
                socketBuilder.reader = new ClientMessageAsyncSocketReader(
                        nodeEngine.getNode().clientEngine, nodeEngine.getProperties());
                socketBuilder.writer = new ClientMessageAsyncSocketWriter();
                socketBuilder.options.set(SO_SNDBUF, socketConfig.getSendBufferSizeKB() * KILO_BYTE);
                socketBuilder.options.set(SO_RCVBUF, socketConfig.getReceiveBufferSizeKB() * KILO_BYTE);
                socketBuilder.options.set(TCP_NODELAY, tcpNoDelay);
                socketBuilder.options.set(SO_KEEPALIVE, true);
                AsyncSocket socket = socketBuilder.build();
                socket.start();
            };
            AsyncServerSocket serverSocket = serverSocketBuilder.build();
            port = serverSocket.getLocalPort();
            serverSockets.add(serverSocket);
            serverSocket.start();
        }
    }

    // public for testing
    public TpcSocketConfig getClientSocketConfig() {
        validateSocketConfig();

        if (config.getAdvancedNetworkConfig().isEnabled()) {
            ServerSocketEndpointConfig endpointConfig = (ServerSocketEndpointConfig) config
                    .getAdvancedNetworkConfig()
                    .getEndpointConfigs()
                    .get(EndpointQualifier.CLIENT);

            return endpointConfig.getTpcSocketConfig();
        }

        // unified socket
        return config.getNetworkConfig().getTpcSocketConfig();
    }

    private void validateSocketConfig() {
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        if (!advancedNetworkConfig.isEnabled()) {
            return;
        }

        TpcSocketConfig defaultTpcSocketConfig = new TpcSocketConfig();
        Map<EndpointQualifier, EndpointConfig> endpointConfigs = advancedNetworkConfig.getEndpointConfigs();

        endpointConfigs.forEach(((endpointQualifier, endpointConfig) -> {
            if (endpointQualifier != EndpointQualifier.CLIENT
                    && !endpointConfig.getTpcSocketConfig().equals(defaultTpcSocketConfig)) {
                throw new InvalidConfigurationException(
                        "TPC socket configuration is only available for clients ports for now.");
            }
        }));

        if (endpointConfigs.get(EndpointQualifier.CLIENT) == null) {
            // Advanced network is enabled yet there is no configured server socket
            // for clients. This means cluster will run but no client ports will be
            // created, so no clients can connect to the cluster.
            throw new InvalidConfigurationException("Missing client server socket configuration. "
                    + "If you have enabled TPC and advanced networking, "
                    + "please configure a client server socket.");
        }
    }

    /**
     * Finds a free port in a range of available ports.
     */
    private final class BindAddressGenerator implements Supplier<SocketAddress> {
        private int port;
        private int limit;

        private BindAddressGenerator(int port, int limit) {
            this.port = port;
            this.limit = limit;
        }

        @Override
        public SocketAddress get() {
            try {
                if (port >= limit) {
                    return null;
                }

                SocketAddress address = new InetSocketAddress(thisAddress.getInetAddress(), port);
                port++;
                return address;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public void shutdown() {
        if (!enabled) {
            return;
        }

        logger.info("TpcServerBootstrap shutdown");

        tpcEngine.shutdown();

        try {
            tpcEngine.awaitTermination(TERMINATE_TIMEOUT_SECONDS, SECONDS);
        } catch (InterruptedException e) {
            logger.warning("TpcEngine failed to terminate.");
            Thread.currentThread().interrupt();
        }

        logger.info("TpcServerBootstrap terminated");
    }
}
