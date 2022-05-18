package com.hazelcast.tpc;

import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.actor.Actor;
import com.hazelcast.tpc.engine.actor.LocalActorHandle;
import com.hazelcast.tpc.engine.nio.NioEventloop;

import java.util.concurrent.CountDownLatch;

public class ActorBenchmark {

    public static void main(String[] args) throws InterruptedException {
        Eventloop eventloop = new NioEventloop();
        eventloop.start();

        CountDownLatch latch = new CountDownLatch(1);

        CountdownActor actor1 = new CountdownActor();
        CountdownActor actor2 = new CountdownActor();
        actor1.that = actor2.getHandle();
        actor1.latch = latch;
        actor2.that = actor1.getHandle();
        actor2.latch = latch;

        actor1.activate(eventloop);
        actor2.activate(eventloop);

        long start = System.currentTimeMillis();

        long requestTotal = 1_000_000_000L;
        LongValue v = new LongValue();
        v.value = requestTotal;
        actor1.getHandle().send(v);
        latch.await();
        long duration = System.currentTimeMillis() - start;
        float throughput = requestTotal * 1000f / duration;
        System.out.println("Throughput:" + throughput);
        System.exit(0);
    }

    static class CountdownActor extends Actor {
        public LocalActorHandle that;
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
