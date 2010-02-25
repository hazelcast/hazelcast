/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import java.util.concurrent.*;

public class FutureProxy<T> implements Future<T> {
    final Callable<T> callable;
    final ProxyHelper proxyHelper;
    private volatile boolean isDone = false;
    private ClientExecutionManagerCallback callback;
    private volatile T result;
    private volatile ExecutionException exception = null;

    public FutureProxy(ProxyHelper proxyHelper, Callable<T> callable) {
        this.proxyHelper = proxyHelper;
        this.callable = callable;
    }

    public boolean cancel(boolean b) {
        return callback.cancel(b);
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return isDone;
    }

    public T get() throws InterruptedException, ExecutionException {
        callback.get();
        return handleResult(result);
    }

    public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        callback.get(l, timeUnit);
        return handleResult(result);
    }

    private T handleResult(Object result) throws ExecutionException {
        if(exception!=null){
            throw this.exception;
        }
        if(isDone){
            return this.result;
        }
        if (result instanceof ExecutionException) {
            this.exception = (ExecutionException) result;
            throw this.exception;
        } else {
            isDone = true;
            this.result = (T)result;
            return this.result;
        }
    }

    public void enqueue(Packet packet) {
        callback.offer(packet);
    }

    public void setCallback(ClientExecutionManagerCallback callback) {
        this.callback = callback;
    }
}
