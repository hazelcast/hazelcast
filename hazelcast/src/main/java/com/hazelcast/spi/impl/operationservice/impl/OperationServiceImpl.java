/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterClock;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_CALL_TIMEOUT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_DESERIALIZE_RESULT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_REPLICA_INDEX;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_COUNT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * This is the implementation of the {@link com.hazelcast.spi.impl.operationservice.InternalOperationService}.
 * <p/>
 * <h1>System Operation</h1>
 * When a {@link com.hazelcast.spi.UrgentSystemOperation} is invoked on this OperationService, it will be executed with a
 * high urgency by making use of a urgent queue. So when the system is under load, and the operation queues are
 * filled, then system operations are executed before normal operation. The advantage is that when a system is under
 * pressure, it still is able to do things like recognizing new members in the cluster and moving partitions around.
 * <p/>
 * When a UrgentSystemOperation is send to a remote machine, it is wrapped in a {@link Packet} and the packet is marked as a
 * urgent packet. When this packet is received on the remove OperationService, the urgent flag is checked and if
 * needed, the operation is set on the urgent queue. So local and remote execution of System operations will obey
 * the urgency.
 *
 * @see Invocation
 * @see InvocationBuilderImpl
 * @see PartitionInvocation
 * @see TargetInvocation
 */
public final class OperationServiceImpl implements InternalOperationService, MetricsProvider, LiveOperationsTracker {

    private static final int CORE_SIZE_CHECK = 8;
    private static final int CORE_SIZE_FACTOR = 4;
    private static final int CONCURRENCY_LEVEL = 16;
    private static final int ASYNC_QUEUE_CAPACITY = 100000;
    private static final long TERMINATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    final InvocationRegistry invocationRegistry;
    final OperationExecutor operationExecutor;

    @Probe(name = "completedCount", level = MANDATORY)
    final AtomicLong completedOperationsCount = new AtomicLong();

    @Probe(name = "operationTimeoutCount", level = MANDATORY)
    final MwCounter operationTimeoutCount = MwCounter.newMwCounter();

    @Probe(name = "callTimeoutCount", level = MANDATORY)
    final MwCounter callTimeoutCount = MwCounter.newMwCounter();

    @Probe(name = "retryCount", level = MANDATORY)
    final MwCounter retryCount = MwCounter.newMwCounter();

    final NodeEngineImpl nodeEngine;
    final Node node;
    final ILogger logger;
    final OperationBackupHandler operationBackupHandler;
    final BackpressureRegulator backpressureRegulator;
    volatile Invocation.Context invocationContext;

    private final InvocationMonitor invocationMonitor;
    private final SlowOperationDetector slowOperationDetector;
    private final AsyncResponseHandler asyncResponseHandler;
    private final InternalSerializationService serializationService;
    private final ResponseHandler responseHandler;
    private final Address thisAddress;

    public OperationServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.thisAddress = node.getThisAddress();
        this.logger = node.getLogger(OperationService.class);
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();

        this.backpressureRegulator = new BackpressureRegulator(
                node.getProperties(), node.getLogger(BackpressureRegulator.class));

        int coreSize = Runtime.getRuntime().availableProcessors();
        boolean reallyMultiCore = coreSize >= CORE_SIZE_CHECK;
        int concurrencyLevel = reallyMultiCore ? coreSize * CORE_SIZE_FACTOR : CONCURRENCY_LEVEL;

        this.invocationRegistry = new InvocationRegistry(
                node.getLogger(OperationServiceImpl.class),
                backpressureRegulator.newCallIdSequence(), concurrencyLevel);

        this.invocationMonitor = new InvocationMonitor(
                nodeEngine, thisAddress, node.getHazelcastThreadGroup(), node.getProperties(), invocationRegistry,
                node.getLogger(InvocationMonitor.class), serializationService, nodeEngine.getServiceManager());

        this.operationBackupHandler = new OperationBackupHandler(this);

        this.responseHandler = new ResponseHandler(
                node.getLogger(ResponseHandler.class), node.getSerializationService(), invocationRegistry, nodeEngine);
        this.asyncResponseHandler = new AsyncResponseHandler(
                node.getHazelcastThreadGroup(), node.getLogger(AsyncResponseHandler.class),
                responseHandler, node.getProperties());

        this.operationExecutor = new OperationExecutorImpl(
                node.getProperties(), node.loggingService, thisAddress, new OperationRunnerFactoryImpl(this),
                node.getHazelcastThreadGroup(), node.getNodeExtension());

        this.slowOperationDetector = new SlowOperationDetector(node.loggingService,
                operationExecutor.getGenericOperationRunners(), operationExecutor.getPartitionOperationRunners(),
                node.getProperties(), node.getHazelcastThreadGroup());
    }

    @Override
    public void populate(LiveOperations result) {
        operationExecutor.scan(result);
    }


    public PacketHandler getAsyncResponseHandler() {
        return asyncResponseHandler;
    }

    public InvocationMonitor getInvocationMonitor() {
        return invocationMonitor;
    }

    @Override
    public List<SlowOperationDTO> getSlowOperationDTOs() {
        return slowOperationDetector.getSlowOperationDTOs();
    }

    public InvocationRegistry getInvocationRegistry() {
        return invocationRegistry;
    }

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    @Override
    public int getPartitionThreadCount() {
        return operationExecutor.getPartitionThreadCount();
    }

    @Override
    public int getGenericThreadCount() {
        return operationExecutor.getGenericThreadCount();
    }

    @Override
    public int getRunningOperationsCount() {
        return operationExecutor.getRunningOperationCount();
    }

    @Override
    public long getExecutedOperationCount() {
        return completedOperationsCount.get();
    }

    @Override
    public int getRemoteOperationsCount() {
        return invocationRegistry.size();
    }

    @Override
    public int getOperationExecutorQueueSize() {
        return operationExecutor.getQueueSize();
    }

    @Override
    public int getPriorityOperationExecutorQueueSize() {
        return operationExecutor.getPriorityQueueSize();
    }

    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    @Override
    public int getResponseQueueSize() {
        return asyncResponseHandler.getQueueSize();
    }

    @Override
    public void execute(PartitionSpecificRunnable task) {
        operationExecutor.execute(task);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        checkNotNegative(partitionId, "Partition id cannot be negative!");
        return new InvocationBuilderImpl(invocationContext, serviceName, op, partitionId);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        checkNotNull(target, "Target cannot be null!");
        return new InvocationBuilderImpl(invocationContext, serviceName, op, target);
    }

    @Override
    public void runOperationOnCallingThread(Operation op) {
        run(op);
    }

    @Override
    public void run(Operation op) {
        operationExecutor.run(op);
    }

    @Override
    public void executeOperation(Operation op) {
        execute(op);
    }

    @Override
    public void execute(Operation op) {
        operationExecutor.execute(op);
    }

    @Override
    public boolean isRunAllowed(Operation op) {
        return operationExecutor.isRunAllowed(op);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        op.setServiceName(serviceName)
                .setPartitionId(partitionId)
                .setReplicaIndex(DEFAULT_REPLICA_INDEX);

        return new PartitionInvocation(
                invocationContext, op, DEFAULT_TRY_COUNT, DEFAULT_TRY_PAUSE_MILLIS,
                DEFAULT_CALL_TIMEOUT, DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnPartition(Operation op) {
        return new PartitionInvocation(
                invocationContext, op, DEFAULT_TRY_COUNT, DEFAULT_TRY_PAUSE_MILLIS,
                DEFAULT_CALL_TIMEOUT, DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
        op.setServiceName(serviceName);

        return new TargetInvocation(invocationContext, op, target, DEFAULT_TRY_COUNT,
                DEFAULT_TRY_PAUSE_MILLIS, DEFAULT_CALL_TIMEOUT, DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> void asyncInvokeOnPartition(String serviceName, Operation op, int partitionId, ExecutionCallback<V> callback) {
        op.setServiceName(serviceName).setPartitionId(partitionId).setReplicaIndex(DEFAULT_REPLICA_INDEX);

        InvocationFuture future = new PartitionInvocation(invocationContext, op, DEFAULT_TRY_COUNT, DEFAULT_TRY_PAUSE_MILLIS,
                DEFAULT_CALL_TIMEOUT, DEFAULT_DESERIALIZE_RESULT).invokeAsync();

        if (callback != null) {
            future.andThen(callback);
        }
    }

    // =============================== processing operation  ===============================

    @Override
    public boolean isCallTimedOut(Operation op) {
        // Join operations should not be checked for timeout
        // because caller is not member of this cluster
        // and can have a different clock.
        if (!op.returnsResponse() || isJoinOperation(op)) {
            return false;
        }

        long callTimeout = op.getCallTimeout();
        long invocationTime = op.getInvocationTime();
        long expireTime = invocationTime + callTimeout;

        if (expireTime <= 0 || expireTime == Long.MAX_VALUE) {
            return false;
        }

        ClusterClock clusterClock = nodeEngine.getClusterService().getClusterClock();
        long now = clusterClock.getClusterTime();
        if (expireTime < now) {
            return true;
        }

        return false;
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        Map<Address, List<Integer>> memberPartitions = nodeEngine.getPartitionService().getMemberPartitionsMap();
        InvokeOnPartitions invokeOnPartitions = new InvokeOnPartitions(this, serviceName, operationFactory, memberPartitions);
        return invokeOnPartitions.invoke();
    }

    @Override
    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                   Collection<Integer> partitions) throws Exception {
        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(3);
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (int partition : partitions) {
            Address owner = partitionService.getPartitionOwnerOrWait(partition);

            if (!memberPartitions.containsKey(owner)) {
                memberPartitions.put(owner, new ArrayList<Integer>());
            }

            memberPartitions.get(owner).add(partition);
        }
        InvokeOnPartitions invokeOnPartitions = new InvokeOnPartitions(this, serviceName, operationFactory, memberPartitions);
        return invokeOnPartitions.invoke();
    }

    @Override
    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory, int[] partitions)
            throws Exception {
        Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(3);
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (int partition : partitions) {
            Address owner = partitionService.getPartitionOwnerOrWait(partition);

            if (!memberPartitions.containsKey(owner)) {
                memberPartitions.put(owner, new ArrayList<Integer>());
            }

            memberPartitions.get(owner).add(partition);
        }
        InvokeOnPartitions invokeOnPartitions = new InvokeOnPartitions(this, serviceName, operationFactory, memberPartitions);
        return invokeOnPartitions.invoke();
    }

    @Override
    public boolean send(Operation op, Address target) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }

        byte[] bytes = serializationService.toBytes(op);
        int partitionId = op.getPartitionId();
        Packet packet = new Packet(bytes, partitionId)
                .setFlag(FLAG_OP);

        if (op.isUrgent()) {
            packet.setFlag(FLAG_URGENT);
        }

        ConnectionManager connectionManager = node.getConnectionManager();
        Connection connection = connectionManager.getOrConnect(target);
        return connectionManager.transmit(packet, connection);
    }

    public boolean send(Response response, Address target) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", response: " + response);
        }

        byte[] bytes = serializationService.toBytes(response);
        Packet packet = new Packet(bytes, -1)
                .setAllFlags(FLAG_OP | FLAG_RESPONSE);

        if (response.isUrgent()) {
            packet.setFlag(FLAG_URGENT);
        }

        ConnectionManager connectionManager = node.getConnectionManager();
        Connection connection = connectionManager.getOrConnect(target);
        return connectionManager.transmit(packet, connection);
    }

    public void onMemberLeft(MemberImpl member) {
        invocationMonitor.onMemberLeft(member);
    }

    public void reset() {
        invocationRegistry.reset();
    }

    @Override
    public void provideMetrics(MetricsRegistry metricsRegistry) {
        metricsRegistry.scanAndRegister(this, "operation");
        metricsRegistry.collectMetrics(invocationRegistry, invocationMonitor, responseHandler, asyncResponseHandler,
                operationExecutor);
    }

    public void start() {
        logger.finest("Starting OperationService");

        ManagedExecutorService asyncExecutor = nodeEngine.getExecutionService().register(
                ExecutionService.ASYNC_EXECUTOR, Runtime.getRuntime().availableProcessors(),
                ASYNC_QUEUE_CAPACITY, ExecutorType.CONCRETE);

        this.invocationContext = new Invocation.Context(
                asyncExecutor,
                nodeEngine.getClusterService().getClusterClock(),
                nodeEngine.getClusterService(),
                node.connectionManager,
                node.nodeEngine.getExecutionService(),
                nodeEngine.getProperties().getMillis(OPERATION_CALL_TIMEOUT_MILLIS),
                invocationRegistry,
                invocationMonitor,
                nodeEngine.getLocalMember().getUuid(),
                nodeEngine.getLogger(Invocation.class),
                node,
                nodeEngine,
                nodeEngine.getPartitionService(),
                this,
                operationExecutor,
                retryCount,
                serializationService,
                nodeEngine.getThisAddress());

        invocationMonitor.start();
        operationExecutor.start();
        asyncResponseHandler.start();
        slowOperationDetector.start();
    }

    /**
     * Shuts down invocation infrastructure.
     * New invocation requests will be rejected after shutdown and all pending invocations
     * will be notified with a failure response.
     */
    public void shutdownInvocations() {
        logger.finest("Shutting down invocations");

        invocationRegistry.shutdown();
        invocationMonitor.shutdown();
        asyncResponseHandler.shutdown();

        try {
            invocationMonitor.awaitTermination(TERMINATION_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            //restore the interrupt.
            //todo: we need a better mechanism for dealing with interruption and waiting for termination
            Thread.currentThread().interrupt();
            EmptyStatement.ignore(e);
        }
    }

    public void shutdownOperationExecutor() {
        logger.finest("Shutting down operation executors");

        operationExecutor.shutdown();
        slowOperationDetector.shutdown();
    }
}
