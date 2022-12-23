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

package com.hazelcast.internal.tpc;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.Eventloop.State.NEW;
import static com.hazelcast.internal.tpc.Eventloop.State.RUNNING;
import static com.hazelcast.internal.tpc.Eventloop.State.SHUTDOWN;
import static com.hazelcast.internal.tpc.Eventloop.State.TERMINATED;
import static com.hazelcast.internal.tpc.TpcTestSupport.assertInstanceOf;
import static com.hazelcast.internal.tpc.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpc.TpcTestSupport.sleepMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class EventloopTest {

    public abstract Eventloop createEventloop();

    public abstract Eventloop.Type getType();

    @Test(expected = NullPointerException.class)
    public void test_offer_Runnable_whenNull() {
        Eventloop eventloop = createEventloop();
        eventloop.offer((Runnable) null);
    }

    @Test
    public void test_offer_Runnable() {
        CountDownLatch completed = new CountDownLatch(1);
        Eventloop eventloop = createEventloop();
        eventloop.start();

        boolean result = eventloop.offer(() -> completed.countDown());

        assertTrue(result);
        assertOpenEventually(completed);
    }

    @Test
    public void test_type() {
        Eventloop eventloop = createEventloop();
        assertEquals(getType(), eventloop.type());
    }

    @Test
    public void test_scheduler() {
        Eventloop eventloop = createEventloop();
        assertInstanceOf(NopScheduler.class, eventloop.scheduler());
    }

    @Test(expected = IllegalStateException.class)
    public void test_start_whenAlreadyStarted() {
        Eventloop eventloop = createEventloop();
        eventloop.start();

        eventloop.start();
    }

    @Test(expected = IllegalStateException.class)
    public void test_start_whenAlreadyTerminated() throws InterruptedException {
        Eventloop eventloop = createEventloop();
        eventloop.start();
        eventloop.shutdown();
        eventloop.awaitTermination(5, SECONDS);

        eventloop.start();
    }

    @Test
    public void test_shutdown_whenNotStarted() {
        Eventloop eventloop = createEventloop();
        eventloop.shutdown();
        assertEquals(TERMINATED, eventloop.state());
    }

    @Test
    public void test_shutdown_whenRunning() throws InterruptedException {
        Eventloop eventloop = createEventloop();
        eventloop.start();

        eventloop.shutdown();

        assertTrue(eventloop.awaitTermination(1, SECONDS));
        assertEquals(TERMINATED, eventloop.state());
    }

    @Test
    public void test_shutdown_whenShuttingDown() throws InterruptedException {
        Eventloop eventloop = createEventloop();
        eventloop.start();

        CountDownLatch started = new CountDownLatch(1);
        eventloop.offer(() -> {
            started.countDown();
            sleepMillis(1000);
        });

        started.await();
        eventloop.shutdown();

        eventloop.shutdown();
        assertTrue(eventloop.awaitTermination(2, SECONDS));
        assertEquals(TERMINATED, eventloop.state());
    }

    @Test
    public void test_shutdown_whenTerminated() {
        Eventloop eventloop = createEventloop();
        eventloop.shutdown();

        eventloop.shutdown();

        assertEquals(TERMINATED, eventloop.state());
    }

    @Test
    public void testLifecycle() throws InterruptedException {
        Eventloop eventloop = createEventloop();
        assertEquals(NEW, eventloop.state());

        eventloop.start();
        assertEquals(RUNNING, eventloop.state());

        CountDownLatch started = new CountDownLatch(1);
        eventloop.offer(() -> {
            started.countDown();
            sleepMillis(2000);
        });

        started.countDown();
        eventloop.shutdown();
        assertEquals(SHUTDOWN, eventloop.state());

        assertTrue(eventloop.awaitTermination(5, SECONDS));
        assertEquals(TERMINATED, eventloop.state());
    }
}
