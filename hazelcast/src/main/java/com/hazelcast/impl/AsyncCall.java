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

package com.hazelcast.impl;

import com.hazelcast.util.SimpleBlockingQueue;

import java.util.concurrent.*;

public abstract class AsyncCall implements Future, Runnable {
    private static final Object NULL = new Object();
    private final BlockingQueue responseQ = new SimpleBlockingQueue();
    private final CallContext callContext = ThreadContext.get().getCallContext();

    protected abstract void call();

    public void run() {
        ThreadContext.get().setCallContext(callContext);
        call();
    }

    public void setResult(Object obj) {
        if (obj == null) {
            obj = NULL;
        }
        responseQ.offer(obj);
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return false;
    }

    public Object get() throws InterruptedException, ExecutionException {
        return processResult(responseQ.take());
    }

    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Object result = responseQ.poll(timeout, unit);
        if (result == null) throw new TimeoutException();
        return processResult(result);
    }

    private Object processResult(Object result) throws ExecutionException {
        if (result == NULL) {
            return null;
        } else if (result instanceof Throwable) {
            throw new ExecutionException((Throwable) result);
        } else {
            return result;
        }
    }
}