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

package com.hazelcast.internal.util.counters;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * A {@link Counter} that is thread-safe; so can be incremented by multiple threads concurrently.
 *
 * The MwCounter is not meant for a huge amount of contention. In that case it would be better to create a counter
 * on the {@link java.util.concurrent.atomic.LongAdder}.
 *
 * This counter does not provide padding to prevent false sharing.
 */
public final class MwCounter implements Counter {

    private static final AtomicLongFieldUpdater<MwCounter> COUNTER = newUpdater(MwCounter.class, "value");

    private volatile long value;

    private MwCounter(long initialValue) {
        this.value = initialValue;
    }

    @Override
    public void set(long newValue) {
        COUNTER.set(this, newValue);
    }

    @Override
    public long getAndSet(long newValue) {
        return COUNTER.getAndSet(this, newValue);
    }

    @Override
    public long get() {
        return value;
    }

    @Override
    public long inc() {
        return COUNTER.incrementAndGet(this);
    }

    @Override
    public long inc(long amount) {
        return COUNTER.addAndGet(this, amount);
    }

    @Override
    public String toString() {
        return "Counter{"
                + "value=" + value
                + '}';
    }

    /**
     * Creates a new MwCounter with 0 as its initial value.
     *
     * @return the new MwCounter, set to 0.
     */
    public static MwCounter newMwCounter() {
        return newMwCounter(0);
    }

    /**
     * Creates a new MwCounter with the given initial value.
     *
     * @param initialValue the initial value of the counter.
     * @return the new MwCounter, set to the given initial value.
     */
    public static MwCounter newMwCounter(long initialValue) {
        return new MwCounter(initialValue);
    }
}
