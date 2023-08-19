/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

import java.util.ArrayList;

import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * A {@link Promise} that can be completed with an int value. The advantage is
 * that it doesn't create litter for the int-value.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class IntPromise implements IntBiConsumer<Throwable> {
    private static final TpcLogger LOGGER = TpcLoggerLocator.getLogger(IntPromise.class);

    private static final int STATE_PENDING = 0;
    private static final int STATE_COMPLETED_NORMALLY = 1;
    private static final int STATE_COMPLETED_EXCEPTIONALLY = 2;

    int refCount = 1;
    IntPromiseAllocator allocator;

    private int state = STATE_PENDING;
    private int value;
    private Throwable throwable;
    private final Eventloop eventloop;
    private final ArrayList<IntBiConsumer<Throwable>> consumers = new ArrayList<>();
    private boolean releaseOnComplete;

    /**
     * Creates a new IntPromise.
     * <p/>
     * IntPromise instances should not be created directly but through the {@link Eventloop#intPromiseAllocator()}
     *
     * @param eventloop the {@link Eventloop} to use for handling completion callbacks.
     */
    public IntPromise(Eventloop eventloop) {
        this.eventloop = checkNotNull(eventloop);
    }

    /**
     * Checks if the IntPromise has been completed.
     *
     * @return <code>true</code> if it has been completed, <code>false</code> otherwise.
     */
    public boolean isDone() {
        return state != STATE_PENDING;
    }

    /**
     * Checks if the IntPromise has been completed exceptionally.
     *
     * @return <code>true</code> if completed exceptionally, <code>false</code> otherwise.
     */
    public boolean isCompletedExceptionally() {
        return state == STATE_COMPLETED_EXCEPTIONALLY;
    }

    public void releaseOnComplete() {
        releaseOnComplete = true;

        if (state != STATE_PENDING) {
            release();
        }
    }

    @Override
    public void accept(int v, Throwable throwable) {
        if (throwable == null) {
            complete(v);
        } else {
            completeExceptionally(throwable);
        }
    }

    /**
     * Completes this Promise with the provided exceptional throwable.
     *
     * @param throwable the Throwable to complete with
     * @throws NullPointerException  if throwable is <code>null</code>.
     * @throws IllegalStateException if the IntPromise is already completed.
     */
    public void completeExceptionally(Throwable throwable) {
        checkNotNull(throwable);

        if (this.state != STATE_PENDING) {
            throw new IllegalStateException("Promise is already completed");
        }
        this.state = STATE_COMPLETED_EXCEPTIONALLY;
        this.throwable = throwable;

        int size = consumers.size();
        for (int k = 0; k < size; k++) {
            IntBiConsumer consumer = consumers.get(k);

            // todo: this should be scheduled as a task
            try {
                consumer.accept(0, throwable);
            } catch (Exception e) {
                LOGGER.warning(e);
            }
        }

        if (releaseOnComplete) {
            release();
        }
    }

    public void completeWithIOException(String message, Throwable cause) {
        completeExceptionally(newUncheckedIOException(message, cause));
    }

    /**
     * Completes this Promise with the provided value.
     *
     * @param value the value
     * @throws IllegalStateException if the Promise has already been completed.
     */
    public void complete(int value) {
        if (state != STATE_PENDING) {
            throw new IllegalStateException("Promise is already completed");
        }
        this.state = STATE_COMPLETED_NORMALLY;
        this.value = value;

        int size = consumers.size();
        for (int k = 0; k < size; k++) {
            IntBiConsumer consumer = consumers.get(k);

            // todo: this should be scheduled as a task
            try {
                consumer.accept(value, null);
            } catch (Exception e) {
                LOGGER.warning(e);
            }
        }

        if (releaseOnComplete) {
            release();
        }
    }

    @SuppressWarnings("rawtypes")
    public <T extends Throwable> IntPromise then(IntBiConsumer<T> consumer) {
        checkNotNull(consumer, "consumer");

        switch (state) {
            case STATE_PENDING:
                consumers.add((IntBiConsumer<Throwable>) consumer);
                break;
            case STATE_COMPLETED_NORMALLY:
                consumer.accept(value, null);
                break;
            case STATE_COMPLETED_EXCEPTIONALLY:
                consumer.accept(0, (T) throwable);
                break;
            default:
                throw new IllegalStateException();
        }
        return this;
    }

    public void acquire() {
        if (refCount == 0) {
            throw new IllegalStateException();
        }
        refCount++;
    }

    public void clear() {
        state = STATE_PENDING;
        refCount = 1;
        consumers.clear();
        releaseOnComplete = false;
    }

    public void release() {
        if (refCount == 0) {
            throw new IllegalStateException();
        } else if (refCount == 1) {
            state = STATE_PENDING;
            refCount = 0;
            consumers.clear();
            releaseOnComplete = false;
            if (allocator != null) {
                allocator.free(this);
            }
        } else {
            refCount--;
        }
    }
}
