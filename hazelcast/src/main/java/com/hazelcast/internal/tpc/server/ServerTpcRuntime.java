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

package com.hazelcast.internal.tpc.server;

import com.hazelcast.config.Config;
import com.hazelcast.htable.impl.HTableDataManager;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.tpc.RpcCore;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.TpcEngine;
import com.hazelcast.internal.tpcengine.TpcEngineBuilder;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.UnpooledIOBufferAllocator;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.pubsub.impl.TopicDataManager;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationProcessor;
import com.hazelcast.spi.impl.operationexecutor.impl.TpcPartitionOperationThread;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * The TpcRuntime is runtime that provides the infrastructure to build next generation data-structures.
 * For more information see:
 * https://www.micahlerner.com/2022/06/04/data-parallel-actors-a-programming-model-for-scalable-query-serving-systems.html
 * = * <p>
 * Mapping from partition to CPU is easy; just a simple mod.
 * <p>
 * RSS: How can we align:
 * - the CPU receiving data from some TCP/IP-connection.
 * - and pinning the same CPU to the RX-queue that processes that TCP/IP-connection
 * So how can we make sure that all TCP/IP-connections for that CPU are processed by the same CPU processing the IRQ.
 * <p>
 * And how can we make sure that for example we want to isolate a few CPUs for the RSS part, but then
 * forward to the CPU that owns the TCP/IP-connection
 * <p>
 * So it appears that Seastar is using the toeplitz hash
 * https://github.com/scylladb/seastar/issues/654
 * <p>
 * So we have a list of channels to some machine.
 * <p>
 * And we determine for each of the channel the toeplitz hash based on src/dst port/ip;
 * <p>
 * So this would determine which channels are mapped to some CPU.
 * <p>
 * So how do we go from partition to a channel?
 */
@SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:VisibilityModifier"})
public class ServerTpcRuntime implements TpcRuntime {

    private static final int TERMINATE_TIMEOUT_SECONDS = 5;

    public volatile boolean shuttingdown;
    public final Node node;
    public final ILogger logger;
    private final boolean poolRequests = parseBoolean(getProperty("hazelcast.alto.pool-requests", "true"));
    private final boolean poolRemoteResponses = parseBoolean(getProperty("hazelcast.alto.pool-remote-responses", "true"));
    private final int requestTimeoutMs = parseInt(getProperty("hazelcast.alto.request.timeoutMs", "23000"));
    private final int eventloopCount;
    private TpcEngine tpcEngine;
    private ServerRpcCore rpcCore;
    private ArrayList<RequestProcessor> schedulers = new ArrayList<>();
    private boolean enabled;
    private final Config config;
    private TpcClientPlane clientPlane;
    private int partitionCount = -1;

    public ServerTpcRuntime(Node node) {
        this.node = node;
        this.config = node.config;
        this.logger = node.getLogger(ServerTpcRuntime.class);
        this.enabled = isTpcEnabled0();
        this.eventloopCount = eventloopCount0();
    }

    @Override
    public int getPartitionCount() {
        return partitionCount;
    }

    @Override
    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public TpcClientPlane getClientPlane() {
        return clientPlane;
    }

    public TpcEngine getTpcEngine() {
        return tpcEngine;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public List<Integer> getClientPlaneServerPorts() {
        return clientPlane.getServerPorts();
    }

    public int eventloopCount() {
        return eventloopCount;
    }

    @Override
    public RpcCore getRpcCore() {
        return rpcCore;
    }

    private boolean isTpcEnabled0() {
        String enabledString = node.getProperties().getString(ClusterProperty.TPC_ENABLED);
        if (enabledString != null) {
            return Boolean.parseBoolean(enabledString);
        } else {
            return config.getTpcConfig().isEnabled();
        }
    }

    private int eventloopCount0() {
        String eventloopCountString = node.getProperties().getString(ClusterProperty.TPC_EVENTLOOP_COUNT);
        if (eventloopCountString == null) {
            return config.getTpcConfig().getEventloopCount();
        } else {
            return Integer.parseInt(eventloopCountString);
        }
    }

    public void start() {
        if (!enabled) {
            logger.info("MemberTpcRuntime: Disabled");
            return;
        }

        logger.info("MemberTpcRuntime: starting");

        InternalPartitionService partitionService = node.nodeEngine.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();

        ManagerRegistry managerRegistry = newManagerRegistry();

        CmdRegistry cmdRegistry = newOpRegistry();

        this.rpcCore = new ServerRpcCore(node.server.getConnectionManager(MEMBER),
                partitionService,
                node.getThisAddress(),
                node.getThisUuid(),
                new FrameDecoderConstructor());

        ReactorType reactorType = ReactorType.fromString(getProperty("hazelcast.tpc.eventloop.type", "nio"));
        ReactorBuilder reactorBuilder = ReactorBuilder.newReactorBuilder(reactorType);
        reactorBuilder.setThreadFactory(new OperationThreadFactory());
        reactorBuilder.setSchedulerSupplier(() -> {
            // remote responses will be created and released by the TPC thread.
            // So a non-concurrent allocator is good enough.
            IOBufferAllocator remoteResponseAllocator = new NonConcurrentIOBufferAllocator(128, true);
            // local responses will be created by the TPC thread, but will be released by a user thread.
            // So a concurrent allocator is needed.
            IOBufferAllocator localResponseAllocator = new ConcurrentIOBufferAllocator(128, true);

            RequestProcessor requestScheduler = new RequestProcessor(
                    cmdRegistry,
                    managerRegistry,
                    32768,
                    Integer.MAX_VALUE,
                    localResponseAllocator,
                    remoteResponseAllocator,
                    rpcCore
            );
            schedulers.add(requestScheduler);

            OperationProcessor operationScheduler = new OperationProcessor(1, node);
            return new TpcProcessor(requestScheduler, operationScheduler);
        });

        this.tpcEngine = new TpcEngineBuilder()
                .setReactorCount(eventloopCount)
                .setReactorBuilder(reactorBuilder)
                .build()
                .start();
        rpcCore.setTpcEngine(tpcEngine);
        rpcCore.setClassicServer(node.getServer());
        rpcCore.start();

        clientPlane = new TpcClientPlane(tpcEngine, node);
        clientPlane.start();

        logger.info("MemberTpcRuntime: started");
    }

    private ManagerRegistry newManagerRegistry() {
        ManagerRegistry managerRegistry = new ManagerRegistry();
        managerRegistry.register(HTableDataManager.class, new HTableDataManager(partitionCount));
        TopicDataManager topicManager = new TopicDataManager(partitionCount);
        topicManager.recover();
        managerRegistry.register(TopicDataManager.class, topicManager);
        return managerRegistry;
    }

    private static CmdRegistry newOpRegistry() {
        CmdRegistry opRegistry = new CmdRegistry(16);
        CmdLoader opHookLoader = new CmdLoader(opRegistry, ServerTpcRuntime.class.getClassLoader());
        opHookLoader.load();
        return opRegistry;
    }

    private class FrameDecoderConstructor implements Function<Reactor, AsyncSocketReader> {
        @Override
        public AsyncSocketReader apply(Reactor reactor) {
            FrameDecoder reader = new FrameDecoder();
            reader.processor = reactor.scheduler();
            reader.responseHandler = rpcCore;
            reader.requestAllocator = poolRequests
                    ? new NonConcurrentIOBufferAllocator(128, true)
                    : new UnpooledIOBufferAllocator();
            reader.responseAllocator = poolRemoteResponses
                    ? new ConcurrentIOBufferAllocator(128, true)
                    : new UnpooledIOBufferAllocator();
            reader.connectionManager = node.getServer().getConnectionManager(MEMBER);
            return reader;
        }
    }

    public void shutdown() {
        if (!enabled) {
            return;
        }

        logger.info("MemberTpcRuntime: shutdown");

        shuttingdown = true;

        if (tpcEngine != null) {
            tpcEngine.shutdown();
        }

        if (rpcCore != null) {
            rpcCore.shutdown();
        }

        try {
            if (tpcEngine != null) {
                tpcEngine.awaitTermination(TERMINATE_TIMEOUT_SECONDS, SECONDS);
            }
        } catch (InterruptedException e) {
            logger.warning("TpcEngine failed to terminate.");
            Thread.currentThread().interrupt();
        }

        long totalScheduled = 0;
        for (RequestProcessor scheduler : schedulers) {
            totalScheduled += scheduler.getScheduled();
        }

        System.out.println("----------- distribution of processed operations -----------------------------");
        for (int k = 0; k < schedulers.size(); k++) {
            RequestProcessor scheduler = schedulers.get(k);
            double percentage = (100d * scheduler.getScheduled()) / totalScheduled;
            System.out.println("OpScheduler[" + k + "] percentage:" + percentage + "%, total:" + scheduler.getScheduled());
        }
        System.out.println("----------- distribution of processed operations -----------------------------");

        logger.info("MemberTpcRuntime: terminated");
    }

    private class OperationThreadFactory implements ThreadFactory {
        int index;

        @Override
        public Thread newThread(Runnable eventloopTask) {
            OperationExecutorImpl operationExecutor = (OperationExecutorImpl) node.nodeEngine
                    .getOperationService()
                    .getOperationExecutor();
            TpcPartitionOperationThread operationThread = (TpcPartitionOperationThread) operationExecutor
                    .getPartitionThreads()[index++];
            operationThread.setEventloopTask(eventloopTask);
            return operationThread;
        }
    }
}
