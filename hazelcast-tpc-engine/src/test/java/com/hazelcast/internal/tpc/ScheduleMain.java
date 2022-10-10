package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.nio.NioReactorBuilder;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ScheduleMain {

    public static void main(String[] args) {
        ReactorBuilder reactorBuilder = new NioReactorBuilder();
        Reactor reactor = reactorBuilder.build();
        reactor.start();

        reactor.offer(() -> reactor.eventloop().scheduleWithFixedDelay(new Ping(), 1, 1, SECONDS));
    }

    private static class Ping implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println(System.currentTimeMillis());
        }
    }
}
