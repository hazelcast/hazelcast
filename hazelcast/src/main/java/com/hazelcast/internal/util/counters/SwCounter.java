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

package com.hazelcast.internal.util.counters;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

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
public class SwCounter implements Counter {

    private static final VarHandle VALUE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            VALUE = l.findVarHandle(SwCounter.class, "value", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }


    private volatile long value;


    private SwCounter(long value) {
        this.value = value;
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
        return new SwCounter(initialValue);
    }


    @Override
    public long inc() {
        long l = (long) VALUE.getOpaque(this) + 1;
        VALUE.setOpaque(this, l);
        return l;
    }

    @Override
    public long inc(long amount) {
        long l = (long) VALUE.getOpaque(this) + amount;
        VALUE.setOpaque(this, l);
        return l;
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
