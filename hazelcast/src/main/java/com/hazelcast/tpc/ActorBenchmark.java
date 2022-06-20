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

package com.hazelcast.tpc;

import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.actor.Actor;
import com.hazelcast.tpc.engine.actor.LocalActorRef;
import com.hazelcast.tpc.engine.nio.NioEventloop;

import java.util.concurrent.CountDownLatch;

public class ActorBenchmark {

    public static void main(String[] args) throws InterruptedException {
        Eventloop eventloop = new NioEventloop();
        eventloop.start();

        CountDownLatch latch = new CountDownLatch(1);

        CountdownActor actor1 = new CountdownActor();
        CountdownActor actor2 = new CountdownActor();
        actor1.that = actor2.handle();
        actor1.latch = latch;
        actor2.that = actor1.handle();
        actor2.latch = latch;

        actor1.activate(eventloop);
        actor2.activate(eventloop);

        long start = System.currentTimeMillis();

        long requestTotal = 1_000_000_000L;
        LongValue v = new LongValue();
        v.value = requestTotal;
        actor1.handle().send(v);
        latch.await();
        long duration = System.currentTimeMillis() - start;
        float throughput = requestTotal * 1000f / duration;
        System.out.println("Throughput:" + throughput);
        System.exit(0);
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
