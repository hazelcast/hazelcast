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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpcengine.Reactor.State.NEW;
import static com.hazelcast.internal.tpcengine.Reactor.State.RUNNING;
import static com.hazelcast.internal.tpcengine.Reactor.State.SHUTDOWN;
import static com.hazelcast.internal.tpcengine.Reactor.State.TERMINATED;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.sleepMillis;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class ReactorTest {

    private final List<Reactor> reactors = new ArrayList<>();

    public abstract ReactorBuilder newReactorBuilder();

    public Reactor newReactor() {
        ReactorBuilder builder = newReactorBuilder();
        Reactor reactor = builder.build();
        reactors.add(reactor);
        return reactor;
    }

    public ReactorType getType() {
        return newReactorBuilder().type;
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    @Test
    public void test_context() {
        Reactor reactor = newReactor();
        assertNotNull(reactor.context());
    }

    @Test
    public void test_scheduler() {
        Reactor reactor = newReactor();
        assertNotNull(reactor.scheduler());
    }

    @Test(expected = NullPointerException.class)
    public void test_offer_Runnable_whenNull() {
        Reactor reactor = newReactor();
        reactor.offer((Runnable) null);
    }

    @Test
    public void test_offer_Runnable() {
        CountDownLatch completed = new CountDownLatch(1);
        Reactor reactor = newReactor();
        reactor.start();

        boolean result = reactor.offer(() -> completed.countDown());

        assertTrue(result);
        assertOpenEventually(completed);
    }

    @Test
    public void test_type() {
        Reactor reactor = newReactor();
        assertEquals(getType(), reactor.type());
    }

    @Test(expected = IllegalStateException.class)
    public void test_start_whenAlreadyStarted() {
        Reactor reactor = newReactor();
        reactor.start();

        reactor.start();
    }

    @Test(expected = IllegalStateException.class)
    public void test_start_whenAlreadyTerminated() throws InterruptedException {
        Reactor reactor = newReactor();
        reactor.start();
        reactor.shutdown();
        reactor.awaitTermination(5, SECONDS);

        reactor.start();
    }

    @Test
    public void test_shutdown_whenNotStarted() {
        Reactor reactor = newReactor();
        reactor.shutdown();
        assertEquals(TERMINATED, reactor.state());
    }

    @Test
    public void test_shutdown_whenRunning() throws InterruptedException {
        Reactor reactor = newReactor();
        reactor.start();

        reactor.shutdown();

        assertTrue(reactor.awaitTermination(1, SECONDS));
        assertEquals(TERMINATED, reactor.state());
    }

    @Test
    public void test_shutdown_whenShuttingDown() throws InterruptedException {
        Reactor reactor = newReactor();
        reactor.start();

        CountDownLatch started = new CountDownLatch(1);
        reactor.offer(() -> {
            started.countDown();
            sleepMillis(1000);
        });

        started.await();
        reactor.shutdown();

        reactor.shutdown();
        assertTrue(reactor.awaitTermination(2, SECONDS));
        assertEquals(TERMINATED, reactor.state());
    }

    @Test
    public void test_shutdown_whenTerminated() {
        Reactor reactor = newReactor();
        reactor.shutdown();

        reactor.shutdown();

        assertEquals(TERMINATED, reactor.state());
    }

    @Test
    public void testLifecycle() throws InterruptedException {
        Reactor reactor = newReactor();
        assertEquals(NEW, reactor.state());

        reactor.start();
        assertEquals(RUNNING, reactor.state());

        CountDownLatch started = new CountDownLatch(1);
        reactor.offer(() -> {
            started.countDown();
            sleepMillis(2000);
        });

        started.countDown();
        reactor.shutdown();
        assertEquals(SHUTDOWN, reactor.state());

        assertTrue(reactor.awaitTermination(5, SECONDS));
        assertEquals(TERMINATED, reactor.state());
    }

    @Test
    public void test_shutdown_thenAsyncServerSocketsClosed() {
        Reactor reactor = newReactor();
        reactor.start();
        AsyncServerSocket serverSocket = reactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {
                })
                .build();

        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();

        reactor.shutdown();
        assertTrueEventually(() -> assertTrue(serverSocket.isClosed()));
    }

    @Test
    public void test_shutdown_thenAsyncSocketClosed() {
        Reactor serverReactor = newReactor();
        serverReactor.start();
        AsyncServerSocket serverSocket = serverReactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {
                })
                .build();

        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();

        Reactor clientReactor = newReactor();
        clientReactor.start();
        AsyncSocket clientSocket = clientReactor.newAsyncSocketBuilder()
                .setReader(new AsyncSocketReader() {
                    @Override
                    public void onRead(ByteBuffer src) {
                    }
                })
                .build();
        clientSocket.start();
        clientSocket.connect(serverSocket.getLocalAddress());

        clientReactor.shutdown();
        assertTrueEventually(() -> assertTrue(clientSocket.isClosed()));
    }
}
