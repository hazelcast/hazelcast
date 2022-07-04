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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorServiceFuture;
import com.hazelcast.durableexecutor.impl.operations.DisposeResultOperation;
import com.hazelcast.durableexecutor.impl.operations.RetrieveAndDisposeResultOperation;
import com.hazelcast.durableexecutor.impl.operations.RetrieveResultOperation;
import com.hazelcast.durableexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.durableexecutor.impl.operations.TaskOperation;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.DelegatingCompletableFuture;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService.SERVICE_NAME;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.spi.impl.InternalCompletableFuture.completedExceptionally;

public class DurableExecutorServiceProxy extends AbstractDistributedObject<DistributedDurableExecutorService>
        implements DurableExecutorService {

    private final int partitionCount;
    private final String name;
    private final ILogger logger;
    private final Random random = new Random();
    private final FutureUtil.ExceptionHandler shutdownExceptionHandler = new FutureUtil.ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable != null) {
                if (throwable instanceof SplitBrainProtectionException) {
                    sneakyThrow(throwable);
                }
                if (throwable.getCause() instanceof SplitBrainProtectionException) {
                    sneakyThrow(throwable.getCause());
                }
            }
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "Exception while ExecutorService shutdown", throwable);
            }
        }
    };

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
        future.joinInternal();
    }

    @Override
    public <T> Future<T> retrieveAndDisposeResult(long uniqueId) {
        int partitionId = Bits.extractInt(uniqueId, false);
        int sequence = Bits.extractInt(uniqueId, true);
        Operation op = new RetrieveAndDisposeResultOperation(name, sequence).setPartitionId(partitionId);
        return invokeOnPartition(op);
    }

    @Override
    public void execute(@Nonnull Runnable task) {
        RunnableAdapter runnableAdapter = createRunnableAdapter(task);
        int partitionId = getTaskPartitionId(runnableAdapter);
        submitToPartition(runnableAdapter, partitionId, null);
    }

    @Override
    public void executeOnKeyOwner(@Nonnull Runnable task,
                                  @Nonnull Object key) {
        checkNotNull(key, "key must not be null");
        RunnableAdapter runnableAdapter = createRunnableAdapter(task);
        int partitionId = getPartitionId(key);
        submitToPartition(runnableAdapter, partitionId, null);
    }

    @Nonnull
    @Override
    public <T> DurableExecutorServiceFuture<T> submit(@Nonnull Runnable task, T result) {
        RunnableAdapter<T> runnableAdapter = createRunnableAdapter(task);
        int partitionId = getTaskPartitionId(runnableAdapter);
        return submitToPartition(runnableAdapter, partitionId, result);
    }

    @Nonnull
    @Override
    public DurableExecutorServiceFuture<?> submit(@Nonnull Runnable task) {
        RunnableAdapter<?> runnableAdapter = createRunnableAdapter(task);
        int partitionId = getTaskPartitionId(runnableAdapter);
        return submitToPartition(runnableAdapter, partitionId, null);
    }

    @Nonnull
    public <T> DurableExecutorServiceFuture<T> submit(@Nonnull Callable<T> task) {
        int partitionId = getTaskPartitionId(task);
        return submitToPartition(task, partitionId, null);
    }

    @Override
    public <T> DurableExecutorServiceFuture<T> submitToKeyOwner(@Nonnull Callable<T> task,
                                                                @Nonnull Object key) {
        checkNotNull(key, "key must not be null");
        int partitionId = getPartitionId(key);
        return submitToPartition(task, partitionId, null);
    }

    @Override
    public DurableExecutorServiceFuture<?> submitToKeyOwner(@Nonnull Runnable task,
                                                            @Nonnull Object key) {
        checkNotNull(key, "key must not be null");
        RunnableAdapter<?> runnableAdapter = createRunnableAdapter(task);
        int partitionId = getPartitionId(key);
        return submitToPartition(runnableAdapter, partitionId, null);
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks,
                                         long timeout, @Nonnull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks,
                           long timeout, @Nonnull TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
        checkNotNull(unit, "unit must not be null");
        return false;
    }

    @Override
    public void shutdown() {
        NodeEngine nodeEngine = getNodeEngine();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> calls = new LinkedList<>();

        for (Member member : members) {
            ShutdownOperation op = new ShutdownOperation(name);
            Future f = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
            calls.add(f);
        }

        waitWithDeadline(calls, 3, TimeUnit.SECONDS, shutdownExceptionHandler);
    }

    @Nonnull
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

    private <T> DurableExecutorServiceFuture<T> submitToPartition(@Nonnull Callable<T> task,
                                                                  int partitionId,
                                                                  T defaultValue) {
        checkNotNull(task, "task can't be null");

        SerializationService serializationService = getNodeEngine().getSerializationService();
        Data taskData = serializationService.toData(task);
        TaskOperation operation = new TaskOperation(name, taskData);
        operation.setPartitionId(partitionId);
        InternalCompletableFuture<Integer> future = invokeOnPartition(operation);
        int sequence;
        try {
            sequence = future.join();
        } catch (CompletionException t) {
            InternalCompletableFuture<T> completedFuture = completedExceptionally(t.getCause());
            return new DurableExecutorServiceDelegateFuture<T>(completedFuture, serializationService, null, -1);
        } catch (CancellationException e) {
            return new DurableExecutorServiceDelegateFuture<>(future, serializationService, null, -1);
        }
        Operation op = new RetrieveResultOperation(name, sequence).setPartitionId(partitionId);
        InternalCompletableFuture<T> internalCompletableFuture = invokeOnPartition(op);

        long taskId = Bits.combineToLong(partitionId, sequence);
        return new DurableExecutorServiceDelegateFuture<>(internalCompletableFuture, serializationService, defaultValue, taskId);
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command can't be null");

        return new RunnableAdapter<>(command);
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

    private int getPartitionId(@Nonnull Object key) {
        return getNodeEngine().getPartitionService().getPartitionId(key);
    }

    private static class DurableExecutorServiceDelegateFuture<T> extends DelegatingCompletableFuture<T>
            implements DurableExecutorServiceFuture<T> {

        final long taskId;

        DurableExecutorServiceDelegateFuture(InternalCompletableFuture future,
                                             SerializationService serializationService,
                                             T defaultValue, long taskId) {
            super(serializationService, future, defaultValue);
            this.taskId = taskId;
        }

        @Override
        public long getTaskId() {
            return taskId;
        }
    }
}
