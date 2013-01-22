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

package com.hazelcast.executor;

import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @mdogan 1/18/13
 */
public final class FutureProxy<V> implements Future<V> {

    private final Future future;
    private final NodeEngine nodeEngine;
    private volatile boolean done = false;

    public FutureProxy(Future future, NodeEngine nodeEngine) {
        this.future = future;
        this.nodeEngine = nodeEngine;
    }

    public V get() throws InterruptedException, ExecutionException {
        return nodeEngine.toObject(future.get());
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return nodeEngine.toObject(future.get(timeout, unit));
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        done = true;
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return done;
    }
}
