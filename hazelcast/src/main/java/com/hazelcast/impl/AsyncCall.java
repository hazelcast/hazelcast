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

package com.hazelcast.impl;

import com.hazelcast.instance.CallContext;
import com.hazelcast.instance.ThreadContext;

import java.util.concurrent.*;

public abstract class AsyncCall implements Future, Runnable {
    private static final Object NULL = new Object();
    private final BlockingQueue responseQ = new LinkedBlockingQueue();
    private final CallContext callContext = ThreadContext.get().getCallContext();
    private volatile Object result = null;

    protected abstract void call();

    public void run() {
        ThreadContext.get().setCallContext(callContext);
        call();
    }

    public void setResult(Object obj) {
        if (obj == null) {
            obj = NULL;
        }
        result = obj;
        responseQ.offer(obj);
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return result != null;
    }

    public Object get() throws InterruptedException, ExecutionException {
        Object r = result;
        if (r == null) {
            r = responseQ.take();
        }
        return processResult(r);
    }

    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Object r = result;
        if (r == null) {
            r = responseQ.poll(timeout, unit);
        }
        if (r == null) throw new TimeoutException();
        return processResult(r);
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