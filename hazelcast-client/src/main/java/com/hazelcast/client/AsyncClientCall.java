/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncClientCall<V> implements Future<V> {
    protected static final Object NULL = new Object();
    protected Object result = null;
    protected final Call remoteCall;
    protected boolean cancelled = false;

    public AsyncClientCall(Call remoteCall) {
        this.remoteCall = remoteCall;
    }

    public void setResult(Object obj) {
        result = (obj == null) ? NULL : obj;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean isDone() {
        return cancelled || result != null;
    }

    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ExecutionException(e);
        }
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (isDone()) {
            return getResult();
        }
        Object response;
        try {
            response = remoteCall.getResponse(timeout, unit);
        } catch (RuntimeException e) {
            response = e.getCause();
        }
        processResult(response);
        return getResult();
    }

    void processResult(Object result) throws InterruptedException, ExecutionException, TimeoutException {
        if (result == null) {
            throw new TimeoutException();
        } else if (result instanceof Throwable) {
            if (result instanceof InterruptedException) {
                throw (InterruptedException) result;
            } else if (result instanceof ExecutionException) {
                throw (ExecutionException) result;
            } else if (result instanceof TimeoutException) {
                throw (TimeoutException) result;
            } else {
                throw new ExecutionException((Throwable) result);
            }
        } else if (result instanceof Packet) {
            setResult(ProxyHelper.getValue((Packet) result));
        } else {
            setResult(result);
        }
    }

    private V getResult() throws ExecutionException {
        if (result == NULL) {
            return null;
        } else if (result instanceof Throwable) {
            throw new ExecutionException((Throwable) result);
        } else {
            return (V) result;
        }
    }
}
