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

package com.hazelcast;

import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.ReactorBuilder;
import com.hazelcast.internal.tpc.actor.Actor;
import com.hazelcast.internal.tpc.actor.LocalActorRef;
import com.hazelcast.internal.tpc.iouring.IOUringReactorBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
public class ActorBenchmark {
    public static final int OPERATIONS = 100 * 1000 * 1000;

    private Reactor eventloop;

    @Setup
    public void setup() {
        ReactorBuilder builder = new IOUringReactorBuilder();
        //Eventloop.Configuration builder = new NioEventloop.NioConfiguration();
        builder.setBatchSize(16);
        builder.setClockRefreshPeriod(16);
        eventloop = builder.build();
        eventloop.start();
    }

    @TearDown
    public void teardown() throws InterruptedException {
        eventloop.shutdown();
        eventloop.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Benchmark
    @OperationsPerInvocation(value = OPERATIONS)
    public void benchmark() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        CountdownActor actor1 = new CountdownActor();
        CountdownActor actor2 = new CountdownActor();
        actor1.that = actor2.handle();
        actor1.latch = latch;
        actor2.that = actor1.handle();
        actor2.latch = latch;

        actor1.activate(eventloop);
        actor2.activate(eventloop);

        LongValue v = new LongValue();
        v.value = OPERATIONS;
        actor1.handle().send(v);
        latch.await();
    }

    static class CountdownActor extends Actor {
        public LocalActorRef that;
        public CountDownLatch latch;

        @Override
        public void process(Object msg) {
            LongValue l = (LongValue) msg;
            if (l.value == 0) {
                latch.countDown();
            } else {
                l.value--;
                that.send(l);
            }
        }
    }

    static class LongValue {
        public long value;
    }
}
