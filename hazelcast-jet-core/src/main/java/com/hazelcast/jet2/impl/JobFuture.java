/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Javadoc pending.
 */
class JobFuture implements Future<Void> {

    private final CountDownLatch completionLatch;
    private final AtomicReference<Throwable> trouble;

    JobFuture(CountDownLatch completionLatch, AtomicReference<Throwable> trouble) {
        this.completionLatch = completionLatch;
        this.trouble = trouble;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return completionLatch.getCount() == 0;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, SECONDS);
        } catch (TimeoutException e) {
            throw new Error("Impossible timeout");
        }
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!completionLatch.await(timeout, unit)) {
            throw new TimeoutException("Jet Execution Service");
        }
        final Throwable t = trouble.get();
        if (t != null) {
            throw new ExecutionException(t);
        }
        return null;
    }
}
