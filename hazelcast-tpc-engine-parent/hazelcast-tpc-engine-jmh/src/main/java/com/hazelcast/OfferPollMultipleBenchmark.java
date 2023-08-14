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

package com.hazelcast;

import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Since we are doing lots of polling from queues and many of them can be
 * empty, we need a benchmark to check how expensive these empty polls actually
 * are.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
@OperationsPerInvocation(OfferPollMultipleBenchmark.OPERATIONS_PER_INVOCATION)
public class OfferPollMultipleBenchmark {

    public final static int OPERATIONS_PER_INVOCATION = 1024;

    private final MpscArrayQueue mpscArrayQueue = new MpscArrayQueue(OPERATIONS_PER_INVOCATION);
    private final MpmcArrayQueue mpmcArrayQueue = new MpmcArrayQueue(OPERATIONS_PER_INVOCATION);
    private final CircularQueue circularQueue = new CircularQueue(OPERATIONS_PER_INVOCATION);

    @Benchmark
    public Object mpscArrayQueue() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            mpscArrayQueue.offer(this);
        }
        Object result = null;
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            result = mpscArrayQueue.poll();
        }
        return result;
    }

    @Benchmark
    public Object mpmcArrayQueue() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            mpmcArrayQueue.offer(this);
        }
        Object result = null;
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            result = mpmcArrayQueue.poll();
        }
        return result;
    }

    @Benchmark
    public Object circularQueue() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            circularQueue.offer(this);
        }
        Object result = null;
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            result = circularQueue.poll();
        }
        return result;
    }
}
