package com.hazelcast.tpc.engine;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.tpc.engine.EventloopState.NEW;
import static com.hazelcast.tpc.engine.EventloopState.RUNNING;
import static com.hazelcast.tpc.engine.EventloopState.SHUTDOWN;
import static com.hazelcast.tpc.engine.EventloopState.TERMINATED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class EventloopTest {

    public abstract Eventloop createEventloop();

    @Test
    public void testLifecycle() throws InterruptedException {
        Eventloop eventloop = createEventloop();
        assertEquals(NEW, eventloop.state());

        eventloop.start();
        assertEquals(RUNNING, eventloop.state());

        CountDownLatch started = new CountDownLatch(1);
        eventloop.execute(() -> {
            started.countDown();
            Thread.sleep(2000);
        });

        started.countDown();
        eventloop.shutdown();
        assertEquals(SHUTDOWN, eventloop.state());

        assertTrue(eventloop.awaitTermination(5, SECONDS));
        assertEquals(TERMINATED, eventloop.state());
    }
}
