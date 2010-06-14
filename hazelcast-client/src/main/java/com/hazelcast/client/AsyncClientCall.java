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

import com.hazelcast.util.SimpleBlockingQueue;

import java.util.concurrent.*;

public abstract class AsyncClientCall<V> implements Future<V>, Runnable {
    BlockingQueue<Object> responseQ = new SimpleBlockingQueue<Object>();
    private static final Object NULL = new Object();

    protected abstract void call();

    public void run() {
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

    public V get() throws InterruptedException, ExecutionException {
        return (V) getResult(responseQ.take());
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return (V) getResult(responseQ.poll(timeout, unit));
    }

    private Object getResult(Object obj) {
        if (obj == NULL) return null;
        else return obj;
    }
}
