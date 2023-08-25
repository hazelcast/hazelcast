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
 * It makes use of the opaque read/writes to provide a lower overhead than a volatile write. Volatile write is
 * pretty expensive. On the X86 is causes subsequent loads to wait till the store in the store buffer has been
 * written to the coherent cache. And this can take some time because it could be that a whole bunch of cache
 * lines need to be invalidated and this can add a lot of latency to those loads. Opaque doesn't provide any
 * ordering guarantees with respect to other variables. It is atomic and coherent and it is super well suited for
 * progress indicators like performance counters. Opaque is primary
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
public final class SwCounter implements Counter {

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
        return (long) VALUE.getOpaque(this);
    }

    @Override
    public void set(long newValue) {
        VALUE.setOpaque(this, newValue);
    }

    @Override
    public long getAndSet(long newValue) {
        long oldValue = (long) VALUE.getOpaque(this);
        VALUE.setOpaque(this, newValue);
        return oldValue;
    }

    @Override
    public String toString() {
        return "Counter{value=" + value + '}';
    }
}
