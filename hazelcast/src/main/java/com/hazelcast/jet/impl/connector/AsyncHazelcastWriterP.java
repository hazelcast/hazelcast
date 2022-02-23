/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.impl.connector.HazelcastWriters.handleInstanceNotActive;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.tryIncrement;

public abstract class AsyncHazelcastWriterP implements Processor {

    protected static final int MAX_PARALLEL_ASYNC_OPS_DEFAULT = 1000;

    private final ILogger logger = Logger.getLogger(AsyncHazelcastWriterP.class);
    private final int maxParallelAsyncOps;
    private final AtomicInteger numConcurrentOps = new AtomicInteger();
    private final AtomicReference<Throwable> firstError = new AtomicReference<>();
    private final HazelcastInstance instance;
    private final boolean isLocal;

    private final BiConsumer callback = withTryCatch(logger, (response, t) -> {
        numConcurrentOps.decrementAndGet();
        if (t != null) {
            firstError.compareAndSet(null, t);
        }
    });

    AsyncHazelcastWriterP(@Nonnull HazelcastInstance instance, int maxParallelAsyncOps) {
        this.instance = Objects.requireNonNull(instance, "instance");
        this.maxParallelAsyncOps = maxParallelAsyncOps;
        this.isLocal = ImdgUtil.isMemberInstance(instance);
    }

    @Override
    public final boolean tryProcess() {
        flush();
        return true;
    }

    @Override
    public final void process(int ordinal, @Nonnull Inbox inbox) {
        checkError();
        try {
            processInternal(inbox);
        } catch (HazelcastInstanceNotActiveException e) {
            throw handleInstanceNotActive(e, isLocal());
        }
    }

    @Override
    public final boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        return flush() && asyncCallsDone();
    }

    @Override
    public final boolean complete() {
        return flush() && asyncCallsDone();
    }

    private boolean flush() {
        checkError();
        try {
            return flushInternal();
        } catch (HazelcastInstanceNotActiveException e) {
            throw handleInstanceNotActive(e, isLocal());
        }
    }

    @CheckReturnValue
    protected boolean flushInternal() {
        return true;
    }

    protected abstract void processInternal(Inbox inbox);

    protected final void setCallback(CompletionStage stage) {
        stage.whenCompleteAsync(callback);
    }

    @CheckReturnValue
    protected final boolean tryAcquirePermit() {
        return tryIncrement(numConcurrentOps, 1, maxParallelAsyncOps);
    }

    /**
     * Acquires as many permits as we are able to immediately, up to
     * desiredNumber. Returns the number of actually acquired permits. Can
     * return 0.
     */
    @CheckReturnValue
    protected final int tryAcquirePermits(int desiredNumber) {
        int prev;
        int next;
        do {
            prev = numConcurrentOps.get();
            next = Math.min(prev + desiredNumber, maxParallelAsyncOps);
            if (next == prev) {
                return 0;
            }
        } while (!numConcurrentOps.compareAndSet(prev, next));
        return next - prev;
    }

    protected final HazelcastInstance instance() {
        return instance;
    }

    protected final boolean isLocal() {
        return isLocal;
    }

    private void checkError() {
        Throwable t = firstError.get();
        if (t instanceof HazelcastInstanceNotActiveException) {
            throw handleInstanceNotActive((HazelcastInstanceNotActiveException) t, isLocal());
        } else if (t != null) {
            throw sneakyThrow(t);
        }
    }

    private boolean asyncCallsDone() {
        boolean allWritten = numConcurrentOps.get() == 0;
        checkError();
        return allWritten;
    }
}
