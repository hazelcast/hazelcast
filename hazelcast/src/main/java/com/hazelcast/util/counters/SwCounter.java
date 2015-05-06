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

package com.hazelcast.util.counters;

import com.hazelcast.nio.UnsafeHelper;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.EmptyStatement.ignore;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * A {@link Counter} that is made to be used by a single writing thread.
 *
 * It makes use of the lazy-set to provide a lower overhead than a volatile write on X86 systems. The volatile write requires
 * waiting for the store buffer to be drained which isn't needed for the lazySet.
 *
 * This counter does not provide padding to prevent false sharing.
 *
 * You might wonder why not to use the AtomicLong.inc; the problem here is that it requires a full fence (so waiting for
 * store and load buffers to be drained). This is more expensive.
 *
 * You might wonder why not to use the following:
 * <pre>
 *     atomicLong.lazySet(atomicLong.get()+1)
 * </pre>
 * This causes a lot of syntactic noise due to lack of abstraction. A counter.inc() gives a better clue what the intent is.
 */
public abstract class SwCounter implements Counter {

    private SwCounter() {
    }

    /**
     * Creates a new SwCounter with 0 as initial value.
     *
     * @return the created SwCounter.
     */
    public static SwCounter newSwCounter() {
        return newSwCounter(0);
    }

    /**
     * Creates a new SwCounter with the given initial value.
     *
     * @return the created SwCounter.
     */
    public static SwCounter newSwCounter(int initialValue) {
        if (UnsafeHelper.UNSAFE_AVAILABLE) {
            return new UnsafeSwCounter(initialValue);
        } else {
            return new SafeSwCounter(initialValue);
        }
    }

    /**
     * The UnsafeSwCounter relies on the same {@link Unsafe#putOrderedLong(Object, long, long)} as the
     * {@link AtomicLongFieldUpdater#lazySet(Object, long)} but it removes all kinds of checks.
     *
     * For the AtomicLongFieldUpdater these checks are needed since an arbitrary object can be passed to the
     * lazySet method and that needs to be verified. In our case we always pass UnsafeSwCounter instance so
     * there is no need for all these checks.
     */
    static final class UnsafeSwCounter extends SwCounter {
        private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;
        private static final long OFFSET;

        static {
            Field field = null;
            try {
                field = UnsafeSwCounter.class.getDeclaredField("value");
            } catch (NoSuchFieldException ignore) {
                ignore(ignore);
            }
            OFFSET = UnsafeHelper.UNSAFE.objectFieldOffset(field);
        }

        private long localValue;
        private volatile long value;

        protected UnsafeSwCounter(int initialValue) {
            this.value = initialValue;
        }

        @Override
        public void inc() {
            long newLocalValue = ++localValue;
            localValue = newLocalValue;
            UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        }

        @Override
        public void inc(int amount) {
            long newLocalValue = localValue += amount;
            UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public String toString() {
            return "Counter{"
                    + "value=" + value
                    + '}';
        }
    }

    /**
     * Makes use of the AtomicLongFieldUpdater.lazySet.
     */
    static final class SafeSwCounter extends SwCounter {

        private static final AtomicLongFieldUpdater<SafeSwCounter> COUNTER
                = newUpdater(SafeSwCounter.class, "value");

        private volatile long value;

        protected SafeSwCounter(int initialValue) {
            this.value = initialValue;
        }

        @Override
        public void inc() {
            COUNTER.lazySet(this, value + 1);
        }

        @Override
        public void inc(int amount) {
            COUNTER.lazySet(this, value + amount);
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public String toString() {
            return "Counter{"
                    + "value=" + value
                    + '}';
        }
    }
}
