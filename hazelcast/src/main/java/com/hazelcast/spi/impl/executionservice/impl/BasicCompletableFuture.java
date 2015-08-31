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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * There's a couple of problems and design inconsistencies related to this class that may lead to unexpected behavior
 * including infinite waiting or inconsistent callbacks behavior.
 * Please have a look at the BasicCompletableFutureTest - some of the issues are documented there.
 */
class BasicCompletableFuture<V> extends AbstractCompletableFuture<V> {

    final Future<V> future;

    BasicCompletableFuture(Future<V> future, NodeEngine nodeEngine) {
        super(nodeEngine, nodeEngine.getLogger(BasicCompletableFuture.class));
        this.future = future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        boolean done = future.isDone();
        if (done && !super.isDone()) {
            forceSetResult();
            return true;
        }
        return done || super.isDone();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        V result = future.get(timeout, unit);
        // If not yet set by CompletableFuture task runner, we can go for it!
        if (!super.isDone()) {
            setResult(result);
        }
        return result;
    }

    private void forceSetResult() {
        Object result;
        try {
            result = future.get();
        } catch (Throwable t) {
            result = t;
        }
        setResult(result);
    }
}
