/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorServiceFuture;
import com.hazelcast.durableexecutor.impl.operations.DisposeResultOperation;
import com.hazelcast.durableexecutor.impl.operations.RetrieveAndDisposeResultOperation;
import com.hazelcast.durableexecutor.impl.operations.RetrieveResultOperation;
import com.hazelcast.durableexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.durableexecutor.impl.operations.TaskOperation;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class DurableExecutorServiceProxy extends AbstractDistributedObject<DistributedDurableExecutorService>
        implements DurableExecutorService {

    private final FutureUtil.ExceptionHandler shutdownExceptionHandler = new FutureUtil.ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable != null) {
                if (throwable instanceof QuorumException) {
                    sneakyThrow(throwable);
                }
                if (throwable.getCause() instanceof QuorumException) {
                    sneakyThrow(throwable.getCause());
                }
            }
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "Exception while ExecutorService shutdown", throwable);
            }
        }
    };

    private final ILogger logger;

    private final Random random = new Random();

    private final int partitionCount;

    private final String name;

    DurableExecutorServiceProxy(NodeEngine nodeEngine, DistributedDurableExecutorService service, String name) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(DurableExecutorServiceProxy.class);
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
    }

    @Override
    public <T> Future<T> retrieveResult(long uniqueId) {
        int partitionId = Bits.extractInt(uniqueId, false);
        int sequence = Bits.extractInt(uniqueId, true);
        Operation op = new RetrieveResultOperation(name, sequence).setPartitionId(partitionId);
        return invokeOnPartition(op);
    }

    @Override
    public void disposeResult(long uniqueId) {
        int partitionId = Bits.extractInt(uniqueId, false);
        int sequence = Bits.extractInt(uniqueId, true);
        Operation op = new DisposeResultOperation(name, sequence).setPartitionId(partitionId);
        InternalCompletableFuture<?> future = invokeOnPartition(op);
        future.join();
    }

    @Override
    public <T> Future<T> retrieveAndDisposeResult(long uniqueId) {
        int partitionId = Bits.extractInt(uniqueId, false);
        int sequence = Bits.extractInt(uniqueId, true);
        Operation op = new RetrieveAndDisposeResultOperation(name, sequence).setPartitionId(partitionId);
        return invokeOnPartition(op);
    }

    @Override
    public void execute(Runnable task) {
        RunnableAdapter runnableAdapter = createRunnableAdapter(task);
        int partitionId = getTaskPartitionId(runnableAdapter);
        submitToPartition(runnableAdapter, partitionId, null);
    }

    @Override
    public void executeOnKeyOwner(Runnable task, Object key) {
        RunnableAdapter runnableAdapter = createRunnableAdapter(task);
        int partitionId = getPartitionId(key);
        submitToPartition(runnableAdapter, partitionId, null);
    }

    @Override
    public <T> DurableExecutorServiceFuture<T> submit(Runnable task, T result) {
        RunnableAdapter<T> runnableAdapter = createRunnableAdapter(task);
        int partitionId = getTaskPartitionId(runnableAdapter);
        return submitToPartition(runnableAdapter, partitionId, result);
    }

    @Override
    public DurableExecutorServiceFuture<?> submit(Runnable task) {
        RunnableAdapter<?> runnableAdapter = createRunnableAdapter(task);
        int partitionId = getTaskPartitionId(runnableAdapter);
        return submitToPartition(runnableAdapter, partitionId, null);
    }

    public <T> DurableExecutorServiceFuture<T> submit(Callable<T> task) {
        int partitionId = getTaskPartitionId(task);
        return submitToPartition(task, partitionId, null);
    }

    public <T> DurableExecutorServiceFuture<T> submitToKeyOwner(Callable<T> task, Object key) {
        int partitionId = getPartitionId(key);
        return submitToPartition(task, partitionId, null);
    }

    @Override
    public DurableExecutorServiceFuture<?> submitToKeyOwner(Runnable task, Object key) {
        RunnableAdapter<?> runnableAdapter = createRunnableAdapter(task);
        int partitionId = getPartitionId(key);
        return submitToPartition(runnableAdapter, partitionId, null);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void shutdown() {
        NodeEngine nodeEngine = getNodeEngine();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> calls = new LinkedList<Future>();

        for (Member member : members) {
            ShutdownOperation op = new ShutdownOperation(name);
            Future f = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
            calls.add(f);
        }

        waitWithDeadline(calls, 3, TimeUnit.SECONDS, shutdownExceptionHandler);
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        try {
            return getService().isShutdown(name);
        } catch (HazelcastInstanceNotActiveException e) {
            return true;
        }
    }

    @Override
    public boolean isTerminated() {
        return isShutdown();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    protected void throwNotActiveException() {
        throw new RejectedExecutionException();
    }

    private <T> DurableExecutorServiceFuture<T> submitToPartition(Callable<T> task, int partitionId, T defaultValue) {
        checkNotNull(task, "task can't be null");

        SerializationService serializationService = getNodeEngine().getSerializationService();
        Data taskData = serializationService.toData(task);
        TaskOperation operation = new TaskOperation(name, taskData);
        operation.setPartitionId(partitionId);
        InternalCompletableFuture<Integer> future = invokeOnPartition(operation);
        int sequence;
        try {
            sequence = future.get();
        } catch (Throwable t) {
            CompletedFuture<T> completedFuture = new CompletedFuture<T>(serializationService, t, getAsyncExecutor());
            return new DurableExecutorServiceDelegateFuture<T>(completedFuture, serializationService, null, -1);
        }
        Operation op = new RetrieveResultOperation(name, sequence).setPartitionId(partitionId);
        InternalCompletableFuture<T> internalCompletableFuture = invokeOnPartition(op);

        long taskId = Bits.combineToLong(partitionId, sequence);
        return new DurableExecutorServiceDelegateFuture<T>(internalCompletableFuture, serializationService, defaultValue, taskId);
    }

    private ExecutorService getAsyncExecutor() {
        return getNodeEngine().getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command can't be null");

        return new RunnableAdapter<T>(command);
    }

    private <T> int getTaskPartitionId(Callable<T> task) {
        if (task instanceof PartitionAware) {
            Object partitionKey = ((PartitionAware) task).getPartitionKey();
            if (partitionKey != null) {
                return getPartitionId(partitionKey);
            }
        }
        return random.nextInt(partitionCount);
    }

    private int getPartitionId(Object key) {
        return getNodeEngine().getPartitionService().getPartitionId(key);
    }

    private static class DurableExecutorServiceDelegateFuture<T> extends DelegatingFuture<T>
            implements DurableExecutorServiceFuture<T> {

        final long taskId;

        public DurableExecutorServiceDelegateFuture(InternalCompletableFuture future,
                                                    SerializationService serializationService,
                                                    T defaultValue, long taskId) {
            super(future, serializationService, defaultValue);
            this.taskId = taskId;
        }

        @Override
        public long getTaskId() {
            return taskId;
        }
    }
}
