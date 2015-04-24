/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.classic.ClassicOperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.counters.MwCounter;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.InvocationBuilder.DEFAULT_CALL_TIMEOUT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_DESERIALIZE_RESULT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_REPLICA_INDEX;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_COUNT;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS;

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
public final class OperationServiceImpl implements InternalOperationService {

    private static final int CORE_SIZE_CHECK = 8;
    private static final int CORE_SIZE_FACTOR = 4;
    private static final int CONCURRENCY_LEVEL = 16;
    private static final int ASYNC_QUEUE_CAPACITY = 100000;
    private static final long TERMINATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    final InvocationRegistry invocationsRegistry;
    final OperationExecutor operationExecutor;
    final ILogger invocationLogger;
    final ManagedExecutorService asyncExecutor;

    @Probe(name = "completed.count")
    final AtomicLong completedOperationsCount = new AtomicLong();

    @Probe(name = "operationTimeoutCount")
    final MwCounter operationTimeoutCount = MwCounter.newMwCounter();

    @Probe(name = "callTimeoutCount")
    final MwCounter callTimeoutCount = MwCounter.newMwCounter();

    @Probe(name = "retryCount")
    final MwCounter retryCount = MwCounter.newMwCounter();

    final NodeEngineImpl nodeEngine;
    final MetricsRegistry metricsRegistry;
    final Node node;
    final ILogger logger;
    final OperationBackupHandler operationBackupHandler;
    final BackpressureRegulator backpressureRegulator;
    final long defaultCallTimeoutMillis;

    private final SlowOperationDetector slowOperationDetector;
    private final IsStillRunningService isStillRunningService;

    public OperationServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class);
        this.metricsRegistry = nodeEngine.getMetricsRegistry();

        this.invocationLogger = nodeEngine.getLogger(Invocation.class);
        GroupProperties groupProperties = node.getGroupProperties();
        this.defaultCallTimeoutMillis = groupProperties.OPERATION_CALL_TIMEOUT_MILLIS.getLong();

        this.backpressureRegulator = new BackpressureRegulator(groupProperties, logger);

        int coreSize = Runtime.getRuntime().availableProcessors();
        boolean reallyMultiCore = coreSize >= CORE_SIZE_CHECK;
        int concurrencyLevel = reallyMultiCore ? coreSize * CORE_SIZE_FACTOR : CONCURRENCY_LEVEL;

        this.invocationsRegistry = new InvocationRegistry(nodeEngine, logger, backpressureRegulator, concurrencyLevel);
        this.operationBackupHandler = new OperationBackupHandler(this);

        this.operationExecutor = new ClassicOperationExecutor(
                groupProperties,
                node.loggingService,
                node.getThisAddress(),
                new OperationRunnerFactoryImpl(this),
                new ResponsePacketHandlerImpl(this),
                node.getHazelcastThreadGroup(),
                node.getNodeExtension(),
                metricsRegistry
        );

        this.isStillRunningService = new IsStillRunningService(operationExecutor, nodeEngine, logger);

        ExecutionService executionService = nodeEngine.getExecutionService();
        this.asyncExecutor = executionService.register(ExecutionService.ASYNC_EXECUTOR, coreSize,
                ASYNC_QUEUE_CAPACITY, ExecutorType.CONCRETE);

        this.slowOperationDetector = initSlowOperationDetector();

        nodeEngine.getMetricsRegistry().scanAndRegister(this, "operation");
    }

    private SlowOperationDetector initSlowOperationDetector() {
        return new SlowOperationDetector(node.loggingService,
                operationExecutor.getGenericOperationRunners(),
                operationExecutor.getPartitionOperationRunners(),
                node.groupProperties,
                node.getHazelcastThreadGroup());
    }

    public IsStillRunningService getIsStillRunningService() {
        return isStillRunningService;
    }

    @Override
    public void dumpPerformanceMetrics(StringBuffer sb) {
    }

    @Override
    public List<SlowOperationDTO> getSlowOperationDTOs() {
        return slowOperationDetector.getSlowOperationDTOs();
    }

    public InvocationRegistry getInvocationsRegistry() {
        return invocationsRegistry;
    }

    @Override
    public int getPartitionOperationThreadCount() {
        return operationExecutor.getPartitionOperationThreadCount();
    }

    @Override
    public int getGenericOperationThreadCount() {
        return operationExecutor.getGenericOperationThreadCount();
    }

    @Probe(name = "running.count")
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
        return invocationsRegistry.size();
    }

    @Probe(name = "response-queue.size")
    @Override
    public int getResponseQueueSize() {
        return operationExecutor.getResponseQueueSize();
    }

    @Probe(name = "queue.size")
    @Override
    public int getOperationExecutorQueueSize() {
        return operationExecutor.getOperationExecutorQueueSize();
    }

    @Probe(name = "priority-queue.size")
    @Override
    public int getPriorityOperationExecutorQueueSize() {
        return operationExecutor.getPriorityOperationExecutorQueueSize();
    }

    @Override
    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    @Override
    public void execute(PartitionSpecificRunnable task) {
        operationExecutor.execute(task);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id cannot be negative!");
        }
        return new InvocationBuilderImpl(nodeEngine, serviceName, op, partitionId);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null!");
        }
        return new InvocationBuilderImpl(nodeEngine, serviceName, op, target);
    }

    @Override
    public void runOperationOnCallingThread(Operation op) {
        operationExecutor.runOnCallingThread(op);
    }

    @Override
    public void executeOperation(Operation op) {
        operationExecutor.execute(op);
    }

    @Override
    public boolean isAllowedToRunOnCallingThread(Operation op) {
        return operationExecutor.isAllowedToRunInCurrentThread(op);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        return new PartitionInvocation(
                nodeEngine, serviceName, op, partitionId, DEFAULT_REPLICA_INDEX,
                DEFAULT_TRY_COUNT, DEFAULT_TRY_PAUSE_MILLIS,
                DEFAULT_CALL_TIMEOUT, null, DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
        return new TargetInvocation(nodeEngine, serviceName, op, target, DEFAULT_TRY_COUNT,
                DEFAULT_TRY_PAUSE_MILLIS,
                DEFAULT_CALL_TIMEOUT, null, DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> void asyncInvokeOnPartition(String serviceName, Operation op, int partitionId, ExecutionCallback<V> callback) {
        new PartitionInvocation(nodeEngine, serviceName, op, partitionId, DEFAULT_REPLICA_INDEX,
                DEFAULT_TRY_COUNT, DEFAULT_TRY_PAUSE_MILLIS,
                DEFAULT_CALL_TIMEOUT, callback, DEFAULT_DESERIALIZE_RESULT).invokeAsync();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> void asyncInvokeOnTarget(String serviceName, Operation op, Address target, ExecutionCallback<V> callback) {
        new TargetInvocation(nodeEngine, serviceName, op, target, DEFAULT_TRY_COUNT,
                DEFAULT_TRY_PAUSE_MILLIS,
                DEFAULT_CALL_TIMEOUT, callback, DEFAULT_DESERIALIZE_RESULT).invokeAsync();
    }

    // =============================== processing operation  ===============================

    @Override
    public boolean isCallTimedOut(Operation op) {
        if (!op.returnsResponse() || op.getCallId() == 0) {
            return false;
        }

        long callTimeout = op.getCallTimeout();
        long invocationTime = op.getInvocationTime();
        long expireTime = invocationTime + callTimeout;

        if (expireTime <= 0 || expireTime >= Long.MAX_VALUE) {
            return false;
        }

        long now = nodeEngine.getClusterService().getClusterClock().getClusterTime();
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
    public boolean send(Operation op, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }

        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }

        Data data = nodeEngine.toData(op);
        int partitionId = op.getPartitionId();
        Packet packet = new Packet(data, partitionId);
        packet.setHeader(Packet.HEADER_OP);

        if (op instanceof UrgentSystemOperation) {
            packet.setHeader(Packet.HEADER_URGENT);
        }

        Connection connection = node.getConnectionManager().getOrConnect(target);
        return nodeEngine.getPacketTransceiver().transmit(packet, connection);
    }

    @Override
    public boolean send(Response response, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }

        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", response: " + response);
        }

        Data data = nodeEngine.toData(response);
        Packet packet = new Packet(data);
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);

        if (response.isUrgent()) {
            packet.setHeader(Packet.HEADER_URGENT);
        }

        Connection connection = node.getConnectionManager().getOrConnect(target);
        return nodeEngine.getPacketTransceiver().transmit(packet, connection);
    }

    public void onMemberLeft(MemberImpl member) {
        invocationsRegistry.onMemberLeft(member);
    }

    public void reset() {
        invocationsRegistry.reset();
    }

    public void shutdown() {
        logger.finest("Shutting down OperationService");
        invocationsRegistry.shutdown();
        operationExecutor.shutdown();
        slowOperationDetector.shutdown();

        try {
            invocationsRegistry.awaitTermination(TERMINATION_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            //restore the interrupt.
            //todo: we need a better mechanism for dealing with interruption and waiting for termination
            Thread.currentThread().interrupt();
            EmptyStatement.ignore(e);
        }
    }
}
