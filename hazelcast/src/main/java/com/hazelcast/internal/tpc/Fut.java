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

package com.hazelcast.internal.tpc;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * This an object similar in nature to the {@link java.util.concurrent.CompletableFuture} that
 * is designed to work with the {@link Eventloop}.
 * <p>
 * The reason this class is called Fut instead of future, it that it is annoying to have multiple
 * future classes on classpath with respect to code completion.
 * <p>
 * This class is not thread-safe and should only be used inside the {@link Eventloop}.
 * <p>
 * The Fut supports pooling. So when you get a promise, make sure you call {@link #release()}
 * when you are done with it.
 *
 * @param <E>
 */
public class Fut<E> {

    private final static Object EMPTY = new Object();

    private Object value = EMPTY;
    private boolean exceptional;
    private final Eventloop eventloop;
    private List<BiConsumer<E, Throwable>> consumers = new ArrayList<>();
    private boolean releaseOnComplete = false;
    public int refCount = 1;
    public FutAllocator allocator;

    public Fut(Eventloop eventloop) {
        this.eventloop = checkNotNull(eventloop);
    }

    /**
     * Checks if the Fut has been completed.
     *
     * @return <code>true</code> if it has been completed, <code>false</code> otherwise.
     */
    public boolean isDone() {
        return value != EMPTY;
    }

    /**
     * Checks if the Fut has been completed exceptionally.
     *
     * @return <code>true</code> if completed exceptionally, <code>false</code> otherwise.
     */
    public boolean isCompletedExceptionally() {
        return value != EMPTY && exceptional;
    }

    public void releaseOnComplete() {
        releaseOnComplete = true;

        if (this.value != EMPTY) {
            release();
        }
    }

    /**
     * Completes this Fut with the provided exceptional value.
     *
     * @param value the exceptional value.
     * @throws NullPointerException  if value is <code>null</code>.
     * @throws IllegalStateException if the Fut is already completed.
     */
    public void completeExceptionally(Throwable value) {
        checkNotNull(value);

        if (this.value != EMPTY) {
            throw new IllegalStateException("Fut is already completed");
        }
        this.value = value;
        this.exceptional = true;

        int count = consumers.size();
        for (int k = 0; k < count; k++) {
            BiConsumer<E, Throwable> consumer = consumers.get(k);
            try {
                consumer.accept(null, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (releaseOnComplete) {
            release();
        }
    }

    /**
     * Completes this Fut with the provided value.
     *
     * @param value the value
     * @throws IllegalStateException if the Fut has already been completed.
     */
    public void complete(E value) {
        if (this.value != EMPTY) {
            throw new IllegalStateException("Fut is already completed");
        }
        this.value = value;
        this.exceptional = false;

        int count = consumers.size();
        for (int k = 0; k < count; k++) {
            BiConsumer<E, Throwable> consumer = consumers.get(k);
            try {
                consumer.accept(value, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (releaseOnComplete) {
            release();
        }
    }

    public <T extends Throwable> Fut then(BiConsumer<E, T> consumer) {
        checkNotNull(consumer, "consumer can't be null");

        if (value == EMPTY) {
            consumers.add((BiConsumer<E, Throwable>) consumer);
        } else if (exceptional) {
            consumer.accept(null, (T) value);
        } else {
            consumer.accept((E) value, null);
        }
        return this;
    }

    public void acquire() {
        if (refCount == 0) {
            throw new IllegalStateException();
        }
        refCount++;
    }

    public void release() {
        if (refCount == 0) {
            throw new IllegalStateException();
        } else if (refCount == 1) {
            refCount = 0;
            value = EMPTY;
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
