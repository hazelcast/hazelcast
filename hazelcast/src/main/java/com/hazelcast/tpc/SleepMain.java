package com.hazelcast.tpc;

import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.nio.NioEventloop;

import static java.util.concurrent.TimeUnit.SECONDS;

public class SleepMain {

    public static void main(String[] args) {
        Eventloop eventloop = new NioEventloop();
        eventloop.start();

        eventloop.execute(() -> {
            eventloop.unsafe.sleep(10, SECONDS)
                    .then(o -> System.out.println("Slept 10"));
            eventloop.unsafe.sleep(5, SECONDS)
                    .then(o -> System.out.println("Slept 5"));
        });
    }
}
