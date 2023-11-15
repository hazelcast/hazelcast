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
 * It makes use of the opaque read/writes to provide a lower overhead than a
 * volatile read/write. Volatile reads/writes can be pretty expensive. If a
 * volatile write is followed by a volatile read to a different variable, then
 * on the X86 it causes that read and all subsequent reads to stall till the
 * stores in the store buffer has been written to the coherent cache. And this can
 * take some time because it could be that there one or more stores in the store buffer
 * (on Skylake the store buffer can contain 50+ stores) each of these stores
 * needs to wait for the cache line to be successfully invalidated on the other CPUs.
 * And this can add a lot of latency to those loads and any instruction depending
 * on the loaded values; so you can end up with a core idling because few instructions
 * are ready for execution. Since there are roughly 10 LFBs (Line Fill Buffers)
 * on modern Intel processor at most 10 cache lines can be invalidated in parallel.
 * <p/>
 * On platforms with a more relaxed memory model like ARM or RISC-V, volatile
 * imposes additional memory fences that are not needed to update a counter if
 * you don't care for coherence.
 * <p/>
 * Unlike volatile, opaque doesn't provide any ordering guarantees with respect
 * to other variables. The only thing the counter provides is coherence and
 * not consistency.
 * <p/>
 * Opaque provides:
 * <ol>
 *     <li>atomicity: so no torn reads/writes</li>
 *     <li>coherence: (1) you don't go back reading an older version after you read a
 *     newer values and (2) cores will not disagree upon the order of writes to a
 *     single variable)
 *     </li>
 * </ol>
 * And therefore is super well suited for progress indicators like performance
 * counters.
 * <p>
 * This counter does not provide padding to prevent false sharing.
 * <p>
 * The design of the Counter object is stale. The problem is that it creates
 * a wrapper object for every field and if there are many fields that needs to
 * be monitored, it creates a lot of overhead including pressure on the cache,
 * indirection etc. It is better to make some metrics object where in a
 * single object there are multiple primitive fields with some form of progress
 * behavior and use VarHandles for opaque updates. If an object would have 5
 * SwCounter, with the current design it would require 5 SwCounter objects,
 * but with a metrics object, you just need 1 object. The TPC engine already
 * switched to this design.
 * See {@link com.hazelcast.internal.tpcengine.net.AsyncSocketMetrics}
 * for an example.
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
