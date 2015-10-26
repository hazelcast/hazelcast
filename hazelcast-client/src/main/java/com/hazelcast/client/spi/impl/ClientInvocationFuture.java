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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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

public class ClientInvocationFuture implements ICompletableFuture<ClientMessage> {

    protected static final ILogger LOGGER = Logger.getLogger(ClientInvocationFuture.class);

    protected final ClientMessage clientMessage;
    protected volatile Object response;

    private final ClientExecutionServiceImpl executionService;
    private final List<ExecutionCallbackNode> callbackNodeList = new LinkedList<ExecutionCallbackNode>();
    private final ClientInvocation invocation;

    public ClientInvocationFuture(ClientInvocation invocation, HazelcastClientInstanceImpl client,
                                  ClientMessage clientMessage) {

        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.clientMessage = clientMessage;
        this.invocation = invocation;
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
    public ClientMessage get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            throw ExceptionUtil.rethrow(exception);
        }
    }

    @Override
    public ClientMessage get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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
                            invocation.notifyException(new TargetDisconnectedException());
                        }
                    }
                }
            }
        }
        return resolveResponse();
    }

    /**
     * @param response coming from server
     * @return true if response coming from server should be set
     */
    boolean shouldSetResponse(Object response) {
        if (this.response != null) {
            LOGGER.warning("The Future.set() method can only be called once. Request: " + clientMessage
                    + ", current response: " + this.response + ", new response: " + response);
            return false;
        }
        return true;
    }

    void setResponse(Object response) {
        synchronized (this) {
            if (!shouldSetResponse(response)) {
                return;
            }

            this.response = response;
            this.notifyAll();
            for (ExecutionCallbackNode node : callbackNodeList) {
                runAsynchronous(node.callback, node.executor);
            }
            callbackNodeList.clear();
        }
    }

    private ClientMessage resolveResponse() throws ExecutionException, TimeoutException, InterruptedException {
        if (response instanceof Throwable) {
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
        if (response == null) {
            throw new TimeoutException();
        }
        return (ClientMessage) response;
    }

    @Override
    public void andThen(ExecutionCallback<ClientMessage> callback) {
        andThen(callback, executionService.getAsyncExecutor());
    }

    @Override
    public void andThen(ExecutionCallback<ClientMessage> callback, Executor executor) {
        synchronized (this) {
            if (response != null) {
                runAsynchronous(callback, executor);
                return;
            }
            callbackNodeList.add(new ExecutionCallbackNode(callback, executor));
        }
    }

    public void andThenInternal(ExecutionCallback<ClientMessage> callback) {
        ExecutorService executor = executionService.getAsyncExecutor();
        synchronized (this) {
            if (response != null) {
                runAsynchronous(callback, executor);
                return;
            }
            callbackNodeList.add(new ExecutionCallbackNode(callback, executor));
        }
    }

    private void runAsynchronous(final ExecutionCallback callback, Executor executor) {
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ClientMessage resp;
                        try {
                            resp = resolveResponse();
                        } catch (Throwable t) {
                            callback.onFailure(t);
                            return;
                        }
                        callback.onResponse(resp);
                    } catch (Throwable t) {
                        LOGGER.severe("Failed to execute callback: " + callback
                                + "! Request: " + clientMessage + ", response: " + response, t);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            LOGGER.warning("Execution of callback: " + callback + " is rejected!", e);
        }
    }

    public ClientInvocation getInvocation() {
        return invocation;
    }

    static class ExecutionCallbackNode {

        final ExecutionCallback callback;
        final Executor executor;

        ExecutionCallbackNode(ExecutionCallback callback, Executor executor) {
            this.callback = callback;
            this.executor = executor;
        }
    }
}
