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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientInvocationFuture<V> implements ICompletableFuture<V> {

    static final ILogger LOGGER = Logger.getLogger(ClientInvocationFuture.class);

    private static final Object NULL_RESPONSE = new Object();

    private final ClientRequest request;

    private final ClientExecutionServiceImpl executionService;

    private final SerializationService serializationService;

    private final List<ExecutionCallbackNode> callbackNodeList = new LinkedList<ExecutionCallbackNode>();

    private final ClientInvocation invocation;

    private boolean responseDeserialized;

    private IFunction afterDeserializeFunction;

    private volatile Object response;

    public ClientInvocationFuture(ClientInvocation invocation, HazelcastClientInstanceImpl client,
                                  ClientRequest request) {
        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.serializationService = client.getSerializationService();
        this.request = request;
        this.invocation = invocation;
    }

    /**
     * By default the ClientInvocationFuture will not deserialize a response if it is in Data format.
     * <p/>
     * This can be changed setting the responseDeserialized to true.
     *
     * @param responseDeserialized
     */
    public void setResponseDeserialized(boolean responseDeserialized) {
        this.responseDeserialized = responseDeserialized;
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
        return response != null;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            throw ExceptionUtil.rethrow(exception);
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final int heartBeatInterval = invocation.getHeartBeatInterval();
        if (response == null) {
            long waitMillis = unit.toMillis(timeout);
            if (waitMillis > 0) {
                synchronized (this) {
                    while (waitMillis > 0 && response == null) {
                        long start = Clock.currentTimeMillis();
                        this.wait(Math.min(heartBeatInterval, waitMillis));
                        long elapsed = Clock.currentTimeMillis() - start;
                        waitMillis -= elapsed;
                        if (!invocation.isConnectionHealthy(elapsed)) {
                            invocation.notify(new TargetDisconnectedException());
                        }
                    }
                }
            }
        }
        return resolveResponse();
    }

    void setResponse(Object response) {
        synchronized (this) {
            if (this.response != null) {
                LOGGER.warning("The Future.set() method can only be called once. Request: " + request
                        + ", current response: " + this.response + ", new response: " + response);
                return;
            }

            this.response = response;

            this.notifyAll();
            for (ExecutionCallbackNode node : callbackNodeList) {
                runAsynchronous(node.callback, node.executor, node.deserialized);
            }
            callbackNodeList.clear();
        }
    }

    private V resolveResponse() throws ExecutionException, TimeoutException, InterruptedException {
        if (response == NULL_RESPONSE) {
            return null;
        }

        if (response instanceof Throwable) {
            return resolveException();
        }

        if (response == null) {
            throw new TimeoutException();
        }

        if (responseDeserialized) {
            Object resp = serializationService.toObject(response);

            if (afterDeserializeFunction != null) {
                resp = afterDeserializeFunction.apply(resp);
            }

            if (resp == null) {
                this.response = NULL_RESPONSE;
                return null;
            }

            this.response = resp;
        }

        return (V) response;
    }

    private V resolveException() throws ExecutionException, TimeoutException, InterruptedException {
        ExceptionUtil.fixRemoteStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
        if (response instanceof ExecutionException) {
            throw (ExecutionException) response;
        }
        if (response instanceof TimeoutException) {
            throw (TimeoutException) response;
        }
        if (response instanceof Error) {
            throw (Error) response;
        }
        if (response instanceof InterruptedException) {
            throw (InterruptedException) response;
        }
        throw new ExecutionException((Throwable) response);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, executionService.getAsyncExecutor());
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        synchronized (this) {
            if (response != null) {
                runAsynchronous(callback, executor, true);
                return;
            }
            callbackNodeList.add(new ExecutionCallbackNode(callback, executor, true));
        }
    }

    public void andThenInternal(ExecutionCallback<Data> callback) {
        ExecutorService executor = executionService.getAsyncExecutor();
        synchronized (this) {
            if (response != null) {
                runAsynchronous(callback, executor, false);
                return;
            }
            callbackNodeList.add(new ExecutionCallbackNode(callback, executor, false));
        }
    }

    public ClientRequest getRequest() {
        return request;
    }

    private void runAsynchronous(final ExecutionCallback callback, Executor executor, final boolean deserialized) {
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Object resp;
                        try {
                            resp = resolveResponse();
                        } catch (Throwable t) {
                            callback.onFailure(t);
                            return;
                        }
                        if (deserialized) {
                            resp = serializationService.toObject(resp);
                            if (afterDeserializeFunction != null) {
                                resp = afterDeserializeFunction.apply(resp);
                            }
                        }
                        callback.onResponse(resp);
                    } catch (Throwable t) {
                        LOGGER.severe("Failed to execute callback: " + callback
                                + "! Request: " + request + ", response: " + response, t);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            LOGGER.warning("Execution of callback: " + callback + " is rejected!", e);
        }
    }

    public void afterDeserialize(IFunction afterDeserializeFunction) {
        this.afterDeserializeFunction = afterDeserializeFunction;
    }

    public ClientInvocation getInvocation() {
        return invocation;
    }

    class ExecutionCallbackNode {

        final ExecutionCallback callback;
        final Executor executor;
        final boolean deserialized;

        ExecutionCallbackNode(ExecutionCallback callback, Executor executor, boolean deserialized) {
            this.callback = callback;
            this.executor = executor;
            this.deserialized = deserialized;
        }
    }
}
