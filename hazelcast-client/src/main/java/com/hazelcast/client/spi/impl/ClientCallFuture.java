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

import com.hazelcast.client.util.ErrorHandler;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientCallFuture<V> implements CompletableFuture<V> {

    private Object response;

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

    public void setResponse(Object response) {
        if (response == null) {
            throw new IllegalArgumentException("response can't be null");
        }
        synchronized (this) {
            if (this.response != null) {
                throw new IllegalArgumentException("The Future.set method can only be called once");
            }
            this.response = response;
            this.notifyAll();
        }
    }

    private V resolveResponse() {
        return (V) ErrorHandler.returnResultOrThrowException(response);
    }

    public void andThen(ExecutionCallback<V> callback) {
        //TODO
    }

    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        //TODO
    }
}