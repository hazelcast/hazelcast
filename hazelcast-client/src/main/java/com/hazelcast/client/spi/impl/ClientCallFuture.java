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
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientCallFuture<V> implements CompletableFuture<V>, Callback {

    private Object response;

    private final ClientRequest request;

    private final ClientClusterServiceImpl clusterService;

    private final ClientExecutionService executionService;

    private final EventHandler handler;

    private volatile int reSendCount = 0;

    public ClientCallFuture(ClientClusterServiceImpl clusterService, ClientExecutionService executionService, ClientRequest request, EventHandler handler) {
        this.clusterService = clusterService;
        this.executionService = executionService;
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
            synchronized (this) {
                if (response == null) {
                    this.wait(unit.toMillis(timeout));
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
            if (request instanceof RetryableRequest || clusterService.isRedoOperation()) {
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
            if (this.response != null && handler != null && response instanceof String) {
                String uuid = (String) this.response;
                String alias = (String) response;
                int callId = request.getCallId();
                clusterService.reRegisterListener(uuid, alias, callId);
                return;
            }
            this.response = response;
            this.notifyAll();
        }
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
        return (V) response;
    }

    public void andThen(ExecutionCallback<V> callback) {
        //TODO
    }

    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        //TODO
    }

    public ClientRequest getRequest() {
        return request;
    }

    public EventHandler getHandler() {
        return handler;
    }

    private boolean resend() {
        reSendCount++;
        if (reSendCount > ClientClusterServiceImpl.RETRY_COUNT) {
            return false;
        }
        executionService.execute(new ResSendTask());
        return true;
    }

    class ResSendTask implements Runnable {
        public void run() {
            try {
                clusterService.reSend(ClientCallFuture.this);
            } catch (Exception e) {
                setResponse(e);
            }
        }
    }
}