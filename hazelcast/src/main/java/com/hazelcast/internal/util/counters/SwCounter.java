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

import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * A {@link Counter} that is made to be used by a single writing thread.
 * <p>
 * It makes use of the lazySet to provide a lower overhead than a volatile write on X86 systems.
 * The volatile write requires waiting for the store buffer to be drained which isn't needed for the lazySet.
 * <p>
 * This counter does not provide padding to prevent false sharing.
 * <p>
 * One might wonder why not use the AtomicLong.inc. The problem here is that AtomicLong requires a full fence,
 * so there is waiting for store and load buffers to be drained. This is more expensive.
 * <p>
 * One might also wonder why not use the following:
 * <pre>
 *     atomicLong.lazySet(atomicLong.get()+1)
 * </pre>
 * This causes a lot of syntactic noise due to lack of abstraction.
 * A counter.inc() gives a better clue what the intent is.
 */
public abstract class SwCounter implements Counter {

    private SwCounter() {
    }

    /**
     * Creates a new SwCounter with 0 as initial value.
     *
     * @return the created SwCounter, set to zero.
     */
    public static SwCounter newSwCounter() {
        return newSwCounter(0);
    }

    /**
     * Creates a new SwCounter with the given initial value.
     *
     * @param initialValue the initial value for the SwCounter.
     * @return the created SwCounter.
     */
    public static SwCounter newSwCounter(long initialValue) {
        return GlobalMemoryAccessorRegistry.MEM_AVAILABLE
                ? new UnsafeSwCounter(initialValue) : new SafeSwCounter(initialValue);
    }

    /**
     * The UnsafeSwCounter relies on the same {@link sun.misc.Unsafe#putOrderedLong(Object, long, long)} as the
     * {@link AtomicLongFieldUpdater#lazySet(Object, long)} but it removes all kinds of checks.
     * <p>
     * For the AtomicLongFieldUpdater, these checks are needed since an arbitrary object can be passed to the
     * lazySet method and that needs to be verified. In our case we always pass the UnsafeSwCounter instance so
     * there is no need for these checks.
     */
    static final class UnsafeSwCounter extends SwCounter {
        private static final long OFFSET;

        static {
            Field field = null;
            try {
                field = UnsafeSwCounter.class.getDeclaredField("value");
            } catch (NoSuchFieldException ignore) {
                ignore(ignore);
            }
            OFFSET = MEM.objectFieldOffset(field);
        }

        private volatile long value;

        UnsafeSwCounter(long initialValue) {
            this.value = initialValue;
        }

        @Override
        @SuppressWarnings("checkstyle:innerassignment")
        public long inc() {
            final long newLocalValue = value + 1;
            MEM.putOrderedLong(this, OFFSET, newLocalValue);
            return newLocalValue;
        }

        @Override
        @SuppressWarnings("checkstyle:innerassignment")
        public long inc(long amount) {
            final long newLocalValue = value + amount;
            MEM.putOrderedLong(this, OFFSET, newLocalValue);
            return newLocalValue;
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public void set(long newValue) {
            MEM.putOrderedLong(this, OFFSET, newValue);
        }

        @Override
        public long getAndSet(long newValue) {
            final long oldLocalValue = value;
            MEM.putOrderedLong(this, OFFSET, newValue);
            return oldLocalValue;
        }

        @Override
        public String toString() {
            return "Counter{value=" + value + '}';
        }
    }

    /**
     * Makes use of the AtomicLongFieldUpdater.lazySet.
     */
    static final class SafeSwCounter extends SwCounter {

        private static final AtomicLongFieldUpdater<SafeSwCounter> COUNTER = newUpdater(SafeSwCounter.class, "value");

        private volatile long value;

        SafeSwCounter(long initialValue) {
            this.value = initialValue;
        }

        @Override
        public long inc() {
            final long newValue = value + 1;
            COUNTER.lazySet(this, newValue);
            return newValue;
        }

        @Override
        public long inc(long amount) {
            final long newValue = value + amount;
            COUNTER.lazySet(this, newValue);
            return newValue;
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public void set(long newValue) {
            COUNTER.lazySet(this, newValue);
        }

        @Override
        public long getAndSet(long newValue) {
            final long oldValue = value;
            COUNTER.lazySet(this, newValue);
            return oldValue;
        }

        @Override
        public String toString() {
            return "Counter{value=" + value + '}';
        }
    }
}
