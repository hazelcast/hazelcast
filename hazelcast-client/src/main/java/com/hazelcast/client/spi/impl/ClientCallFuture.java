/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientCallFuture<V> implements ICompletableFuture<V>, Callback {

    private static final int MAX_RESEND_COUNT = 20;

    private Object response;

    private final ClientRequest request;

    private final ClientExecutionServiceImpl executionService;

    private final ClientInvocationServiceImpl invocationService;

    private final SerializationService serializationService;

    private final EventHandler handler;

    private AtomicInteger reSendCount = new AtomicInteger();

    private volatile ClientConnection connection;

    private List<ExecutionCallbackNode> callbackNodeList = new LinkedList<ExecutionCallbackNode>();

    public ClientCallFuture(HazelcastClient client, ClientRequest request, EventHandler handler) {
        this.invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.serializationService = client.getSerializationService();
        this.request = request;
        this.handler = handler;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return response != null;
    }

    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (response == null) {
            long waitMillis = unit.toMillis(timeout);
            if (waitMillis > 0) {
                synchronized (this) {
                    while (waitMillis > 0 && response == null) {
                        long start = Clock.currentTimeMillis();
                        this.wait(waitMillis);
                        waitMillis -= (Clock.currentTimeMillis() - start);
                    }
                }
            }
        }
        return resolveResponse();
    }

    public void notify(Object response) {
        if (response == null) {
            throw new IllegalArgumentException("response can't be null");
        }
        if (response instanceof TargetNotMemberException) {
            if (resend()) {
                return;
            }
        }

        if (response instanceof TargetDisconnectedException || response instanceof HazelcastInstanceNotActiveException) {
            if (request instanceof RetryableRequest || invocationService.isRedoOperation()) {
                if (resend()) {
                    return;
                }
            }
        }
        setResponse(response);
    }

    private void setResponse(Object response) {
        synchronized (this) {
            if (this.response != null && handler == null) {
                throw new IllegalArgumentException("The Future.set method can only be called once");
            }
            if (this.response != null && !(response instanceof Throwable)) {
                String uuid = serializationService.toObject(this.response);
                String alias = serializationService.toObject(response);
                invocationService.reRegisterListener(uuid, alias, request.getCallId());
                return;
            }
            this.response = response;
            this.notifyAll();
        }

        for (ExecutionCallbackNode node : callbackNodeList) {
            runAsynchronous(node.callback, node.executor);
        }
        callbackNodeList.clear();
    }

    private V resolveResponse() throws ExecutionException, TimeoutException, InterruptedException {
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
        return (V) response;
    }

    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, executionService.getAsyncExecutor());
    }

    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        synchronized (this) {
            if (response != null) {
                runAsynchronous(callback, executor);
                return;
            }
            callbackNodeList.add(new ExecutionCallbackNode(callback, executor));
        }
    }

    public ClientRequest getRequest() {
        return request;
    }

    public EventHandler getHandler() {
        return handler;
    }

    public ClientConnection getConnection() {
        return connection;
    }

    public boolean resend() {
        if (handler == null && reSendCount.incrementAndGet() > MAX_RESEND_COUNT) {
            return false;
        }
        executionService.execute(new ReSendTask());
        return true;
    }

    private void runAsynchronous(final ExecutionCallback callback, Executor executor) {
        executor.execute(new Runnable() {
            public void run() {
                try {
                    callback.onResponse(serializationService.toObject(resolveResponse()));
                } catch (Throwable t) {
                    callback.onFailure(t);
                }
            }
        });
    }

    public void setConnection(ClientConnection connection) {
        this.connection = connection;
    }

    class ReSendTask implements Runnable {
        public void run() {
            try {
                invocationService.reSend(ClientCallFuture.this);
            } catch (Exception e) {
                if (handler != null) {
                    invocationService.registerFailedListener(ClientCallFuture.this);
                } else {
                    setResponse(e);
                }
            }
        }
    }

    class ExecutionCallbackNode {

        final ExecutionCallback callback;
        final Executor executor;

        ExecutionCallbackNode(ExecutionCallback callback, Executor executor) {
            this.callback = callback;
            this.executor = executor;
        }
    }
}
