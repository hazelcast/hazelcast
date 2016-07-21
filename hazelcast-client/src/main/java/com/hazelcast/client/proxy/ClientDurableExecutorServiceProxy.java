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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorIsShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorShutdownCodec;
import com.hazelcast.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorServiceFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link DurableExecutorService}.
 */
public final class ClientDurableExecutorServiceProxy extends ClientProxy implements DurableExecutorService {

    private static final ClientMessageDecoder RETRIEVE_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) DurableExecutorRetrieveResultCodec.decodeResponse(clientMessage).response;
        }
    };

    private final Random random = new Random();

    private int partitionCount;

    public ClientDurableExecutorServiceProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    protected void onInitialize() {
        ClientContext context = getContext();
        ClientPartitionService partitionService = context.getPartitionService();
        partitionCount = partitionService.getPartitionCount();
    }

    @Override
    public <T> Future<T> retrieveResult(long taskId) {
        int partitionId = Bits.extractInt(taskId, false);
        int sequence = Bits.extractInt(taskId, true);
        ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(name, sequence);
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, partitionId).invoke();
        return new ClientDelegatingFuture<T>(future, getSerializationService(), RETRIEVE_RESPONSE_DECODER);
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
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, partitionId).invoke();
        return new ClientDelegatingFuture<T>(future, getSerializationService(), RETRIEVE_RESPONSE_DECODER);
    }

    @Override
    public void execute(Runnable task) {
        int partitionId = getTaskPartitionId(task);
        Callable callable = createRunnableAdapter(task);
        submitToPartition(callable, partitionId, null);
    }

    @Override
    public void executeOnKeyOwner(Runnable task, Object key) {
        int partitionId = getPartitionId(key);
        Callable callable = createRunnableAdapter(task);
        submitToPartition(callable, partitionId, null);
    }

    @Override
    public <T> DurableExecutorServiceFuture<T> submitToKeyOwner(Callable<T> task, Object key) {
        int partitionId = getPartitionId(key);
        return submitToPartition(task, partitionId, null);
    }

    @Override
    public DurableExecutorServiceFuture<?> submitToKeyOwner(Runnable task, Object key) {
        int partitionId = getPartitionId(key);
        Callable callable = createRunnableAdapter(task);
        return submitToPartition(callable, partitionId, null);
    }

    @Override
    public <T> DurableExecutorServiceFuture<T> submit(Callable<T> task) {
        int partitionId = getTaskPartitionId(task);
        return submitToPartition(task, partitionId, null);
    }

    @Override
    public <T> DurableExecutorServiceFuture<T> submit(Runnable task, T result) {
        int partitionId = getTaskPartitionId(task);
        Callable<T> callable = createRunnableAdapter(task);
        return submitToPartition(callable, partitionId, result);
    }

    @Override
    public DurableExecutorServiceFuture<?> submit(Runnable task) {
        int partitionId = getTaskPartitionId(task);
        Callable callable = createRunnableAdapter(task);
        return submitToPartition(callable, partitionId, null);
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
        DurableExecutorIsShutdownCodec.ResponseParameters resultParameters =
                DurableExecutorIsShutdownCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean isTerminated() {
        return isShutdown();
    }

    private <T> DurableExecutorServiceFuture<T> submitToPartition(Callable<T> task, int partitionId, T result) {
        checkNotNull(task, "task should not be null");

        SerializationService serService = getSerializationService();
        ClientMessage request = DurableExecutorSubmitToPartitionCodec.encodeRequest(name, serService.toData(task));
        int sequence;
        try {
            ClientMessage response = invokeOnPartition(request, partitionId);
            sequence = DurableExecutorSubmitToPartitionCodec.decodeResponse(response).response;
        } catch (Throwable t) {
            return new ClientDurableExecutorServiceCompletedFuture<T>(t, getAsyncExecutor());
        }
        ClientMessage clientMessage = DurableExecutorRetrieveResultCodec.encodeRequest(name, sequence);
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, partitionId).invoke();
        long taskId = Bits.combineToLong(partitionId, sequence);
        return new ClientDurableExecutorServiceDelegatingFuture<T>(future, serService, RETRIEVE_RESPONSE_DECODER, result, taskId);
    }

    private ExecutorService getAsyncExecutor() {
        return getContext().getExecutionService().getAsyncExecutor();
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        if (command == null) {
            throw new NullPointerException();
        }
        return new RunnableAdapter<T>(command);
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

    private int getPartitionId(Object key) {
        return getContext().getPartitionService().getPartitionId(key);
    }

    private static class ClientDurableExecutorServiceDelegatingFuture<T> extends ClientDelegatingFuture<T>
            implements DurableExecutorServiceFuture<T> {

        private final long taskId;

        public ClientDurableExecutorServiceDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
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

    private static final class ClientDurableExecutorServiceCompletedFuture<T> implements DurableExecutorServiceFuture<T> {

        private final Object result;
        private final Executor executor;

        private ClientDurableExecutorServiceCompletedFuture(Object result, Executor executor) {
            this.result = result;
            this.executor = executor;
        }

        @Override
        public long getTaskId() {
            throw new IllegalStateException("Task failed to execute!!!");
        }

        @Override
        public void andThen(ExecutionCallback<T> callback) {
            andThen(callback, executor);
        }

        @Override
        public void andThen(final ExecutionCallback<T> callback, Executor executor) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    if (result instanceof Throwable) {
                        callback.onFailure((Throwable) result);
                    } else {
                        callback.onResponse((T) result);
                    }
                }
            });
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            if (result instanceof Throwable) {
                if (result instanceof ExecutionException) {
                    throw (ExecutionException) result;
                }
                if (result instanceof InterruptedException) {
                    throw (InterruptedException) result;
                }
                throw new ExecutionException((Throwable) result);
            }
            return (T) result;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }
    }
}
