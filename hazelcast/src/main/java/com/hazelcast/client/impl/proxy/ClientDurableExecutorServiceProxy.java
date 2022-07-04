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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorServiceFuture;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.spi.impl.DeserializingCompletableFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link DurableExecutorService}.
 */
public final class ClientDurableExecutorServiceProxy extends ClientProxy implements DurableExecutorService {

    private final Random random = new Random();

    private int partitionCount;

    public ClientDurableExecutorServiceProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    protected void onInitialize() {
        ClientPartitionService partitionService = getContext().getPartitionService();
        partitionCount = partitionService.getPartitionCount();
    }

    @Override
    public <T> Future<T> retrieveResult(long taskId) {
        int partitionId = Bits.extractInt(taskId, false);
        int sequence = Bits.extractInt(taskId, true);
        ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(name, sequence);
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(),
                DurableExecutorRetrieveResultCodec::decodeResponse);
    }

    @Override
    public void disposeResult(long taskId) {
        int partitionId = Bits.extractInt(taskId, false);
        int sequence = Bits.extractInt(taskId, true);
        ClientMessage clientMessage = DurableExecutorDisposeResultCodec.encodeRequest(name, sequence);
        invokeOnPartition(clientMessage, partitionId);
    }

    @Override
    public <T> Future<T> retrieveAndDisposeResult(long taskId) {
        int partitionId = Bits.extractInt(taskId, false);
        int sequence = Bits.extractInt(taskId, true);
        ClientMessage clientMessage = DurableExecutorRetrieveAndDisposeResultCodec.encodeRequest(name, sequence);
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(),
                DurableExecutorRetrieveResultCodec::decodeResponse);
    }

    @Override
    public void execute(@Nonnull Runnable task) {
        int partitionId = getTaskPartitionId(task);
        Callable callable = createRunnableAdapter(task);
        submitToPartition(callable, partitionId, null);
    }

    @Override
    public void executeOnKeyOwner(@Nonnull Runnable task,
                                  @Nonnull Object key) {
        checkNotNull(key, "key must not be null");
        int partitionId = getPartitionId(key);
        Callable callable = createRunnableAdapter(task);
        submitToPartition(callable, partitionId, null);
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
        int partitionId = getPartitionId(key);
        Callable callable = createRunnableAdapter(task);
        return submitToPartition(callable, partitionId, null);
    }

    @Nonnull
    @Override
    public <T> DurableExecutorServiceFuture<T> submit(@Nonnull Callable<T> task) {
        int partitionId = getTaskPartitionId(task);
        return submitToPartition(task, partitionId, null);
    }

    @Nonnull
    @Override
    public <T> DurableExecutorServiceFuture<T> submit(@Nonnull Runnable task, T result) {
        int partitionId = getTaskPartitionId(task);
        Callable<T> callable = createRunnableAdapter(task);
        return submitToPartition(callable, partitionId, result);
    }

    @Nonnull
    @Override
    public DurableExecutorServiceFuture<?> submit(@Nonnull Runnable task) {
        int partitionId = getTaskPartitionId(task);
        Callable callable = createRunnableAdapter(task);
        return submitToPartition(callable, partitionId, null);
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
        ClientMessage request = DurableExecutorShutdownCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        ClientMessage request = DurableExecutorIsShutdownCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return DurableExecutorIsShutdownCodec.decodeResponse(response);
    }

    @Override
    public boolean isTerminated() {
        return isShutdown();
    }

    private <T> DurableExecutorServiceFuture<T> submitToPartition(@Nonnull Callable<T> task,
                                                                  int partitionId,
                                                                  @Nullable T result) {
        checkNotNull(task, "task should not be null");

        ClientMessage request = DurableExecutorSubmitToPartitionCodec.encodeRequest(name, toData(task));
        int sequence;
        try {
            ClientMessage response = invokeOnPartition(request, partitionId);
            sequence = DurableExecutorSubmitToPartitionCodec.decodeResponse(response);
        } catch (Throwable t) {
            return completedExceptionally(t, ConcurrencyUtil.getDefaultAsyncExecutor());
        }
        ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(name, sequence);
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
        long taskId = Bits.combineToLong(partitionId, sequence);
        return new ClientDurableExecutorServiceDelegatingFuture<>(future, getSerializationService(),
                DurableExecutorRetrieveResultCodec::decodeResponse,
                result, taskId);
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command can't be null");
        return new RunnableAdapter<>(command);
    }

    private int getTaskPartitionId(Object task) {
        if (task instanceof PartitionAware) {
            Object partitionKey = ((PartitionAware) task).getPartitionKey();
            if (partitionKey != null) {
                return getPartitionId(partitionKey);
            }
        }
        return random.nextInt(partitionCount);
    }

    private int getPartitionId(@Nonnull Object key) {
        return getContext().getPartitionService().getPartitionId(key);
    }

    private static <T> DurableExecutorServiceFuture<T> completedExceptionally(Throwable t, Executor executor) {
        return new ClientDurableExecutorServiceCompletedFuture<>(t, executor);
    }

    private static class ClientDurableExecutorServiceDelegatingFuture<T> extends ClientDelegatingFuture<T>
            implements DurableExecutorServiceFuture<T> {

        private final long taskId;

        ClientDurableExecutorServiceDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                                     SerializationService serializationService,
                                                     ClientMessageDecoder clientMessageDecoder,
                                                     T defaultValue, long taskId) {
            super(clientInvocationFuture, serializationService, clientMessageDecoder, defaultValue);
            this.taskId = taskId;
        }

        @Override
        public long getTaskId() {
            return taskId;
        }
    }

    private static final class ClientDurableExecutorServiceCompletedFuture<T> extends DeserializingCompletableFuture<T>
            implements DurableExecutorServiceFuture<T> {

        private ClientDurableExecutorServiceCompletedFuture(Throwable throwable, Executor executor) {
            super(executor);
            super.completeExceptionally(throwable);
        }

        @Override
        public long getTaskId() {
            throw new IllegalStateException("Task failed to execute!");
        }
    }
}
