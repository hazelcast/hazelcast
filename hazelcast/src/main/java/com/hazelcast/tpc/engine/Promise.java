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

package com.hazelcast.tpc.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * This an object similar in nature to the {@link java.util.concurrent.CompletableFuture} that
 * is designed to work with the {@link Eventloop}.
 *
 * The reason this class is called promise instead of future, it that it is annoying to have multiple
 * future classes on classpath with respect to code completion. Hence the name promise.
 *
 * This class is not thread-safe and should only be used inside the {@link Eventloop}.
 *
 * The Promise supports pooling. So when you get a promise, make sure you call {@link #release()}
 * when you are done with it.
 *
 * @param <E>
 */
public final class Promise<E> {

    private final static Object EMPTY = new Object();

    private Object value = EMPTY;
    private boolean exceptional;
    private final Eventloop eventloop;
    private List<BiConsumer<E, Throwable>> consumers = new ArrayList<>();
    int refCount = 1;
    PromiseAllocator allocator;

    public Promise(Eventloop eventloop) {
        this.eventloop = checkNotNull(eventloop);
    }

    /**
     * Checks if the Promise has been completed.
     *
     * @return true if it has been completed, false otherwise.
     */
    public boolean isDone() {
        return value != EMPTY;
    }

    /**
     * Checks if the Promise has been completed exceptionally.
     *
     * @return true if completed exceptionally, false otherwise.
     */
    public boolean isCompletedExceptionally() {
        return value != EMPTY && exceptional;
    }

    /**
     * Completes this Promise with the provided exceptional value.
     *
     * @param value the exceptional value.
     * @throws NullPointerException if value is null.
     * @throws IllegalStateException if the Promise is already completed.
     */
    public void completeExceptionally(Throwable value) {
        checkNotNull(value);

        if (this.value != EMPTY) {
            throw new IllegalStateException();
        }
        this.value = value;
        this.exceptional = true;

        if (!consumers.isEmpty()) {
            for (BiConsumer consumer : consumers) {
                try {
                    consumer.accept(null, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            consumers.clear();
        }
    }

    /**
     * Completes this Promise with the provided value.
     *
     * @param value the value
     * @throws IllegalStateException if the Promise has already been completed.
     */
    public void complete(Object value) {
        if (this.value != EMPTY) {
            throw new IllegalStateException();
        }
        this.value = value;
        this.exceptional = false;

        if (!consumers.isEmpty()) {
            for (BiConsumer consumer : consumers) {
                try {
                    consumer.accept(value, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            consumers.clear();
        }
    }

    public void then(BiConsumer<E, Throwable> consumer) {
        checkNotNull(consumer, "consumer can't be null");

        if (value == EMPTY) {
            consumers.add(consumer);
        } else if (exceptional) {
            consumer.accept(null, (Throwable) value);
        } else {
            consumer.accept((E) value, null);
        }
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
            if (allocator != null) {
                allocator.free(this);
            }
        } else {
            refCount--;
        }
    }
}
