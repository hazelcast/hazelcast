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
import com.hazelcast.internal.tpcengine.net.DevNullAsyncSocketReader;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public abstract class ReactorTest {

    private final List<Reactor> reactors = new ArrayList<>();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    public abstract Reactor.Builder newReactorBuilder();

    public Reactor newReactor() {
        return newReactor(null);
    }

    public Reactor newReactor(Consumer<Reactor.Builder> configFn) {
        Reactor.Builder reactorBuilder = newReactorBuilder();
        if (configFn != null) {
            configFn.accept(reactorBuilder);
        }
        Reactor reactor = reactorBuilder.build();
        reactors.add(reactor);
        return reactor;
    }

    public ReactorType getType() {
        return newReactorBuilder().type;
    }

    @Test
    public void testConstruction() {
        Reactor reactor = newReactor();

        assertNotNull(reactor.sockets());
        assertEquals(0, reactor.sockets().size());

        assertNotNull(reactor.serverSockets());
        assertEquals(0, reactor.serverSockets().size());

        assertNotNull(reactor.files());
        assertEquals(0, reactor.files().size());

        assertNotNull(reactor.taskQueues());
        assertEquals(1, reactor.taskQueues().size());

        assertNotNull(reactor.metrics());

        assertNotNull(reactor.context());
        assertEquals(getType(), reactor.type());

        Thread eventloopThread = reactor.eventloopThread();
        assertNotNull(eventloopThread);
        assertTrue(eventloopThread.getName().startsWith("EventloopThread-"));
    }

    @Test
    public void testReactorName() {
        String reactorName = "reactor-banana";
        Reactor reactor = newReactor(builder -> builder.reactorName = reactorName);

        assertEquals(reactorName, reactor.name());
    }

    @Test
    public void test_offer_Runnable_whenNull() {
        Reactor reactor = newReactor();

        assertThrows(NullPointerException.class, () -> reactor.offer((Runnable) null));
    }

    @Test
    public void test_offer_Runnable() {
        CountDownLatch completed = new CountDownLatch(1);
        Reactor reactor = newReactor();
        reactor.start();

        boolean result = reactor.offer(completed::countDown);

        assertTrue(result);
        assertOpenEventually(completed);
    }

    @Test
    public void test_type() {
        Reactor reactor = newReactor();
        assertEquals(getType(), reactor.type());
    }

    @Test
    public void test_start_whenAlreadyStarted() {
        Reactor reactor = newReactor();
        reactor.start();

        assertThrows(IllegalStateException.class, reactor::start);
    }

    @Test
    public void test_start_whenAlreadyTerminated() throws InterruptedException {
        Reactor reactor = newReactor();
        reactor.start();
        reactor.shutdown();
        reactor.awaitTermination(5, SECONDS);

        assertThrows(IllegalStateException.class, reactor::start);
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
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
        };
        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        serverSocket.start();

        reactor.shutdown();
        assertTrueEventually(() -> assertTrue(serverSocket.isClosed()));
    }

    @Test
    public void test_shutdown_thenAsyncSocketClosed() {
        Reactor serverReactor = newReactor();
        serverReactor.start();
        AsyncServerSocket.Builder serverSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
        };
        AsyncServerSocket serverSocket = serverSocketBuilder.build();

        serverSocket.start();

        Reactor clientReactor = newReactor();
        clientReactor.start();
        AsyncSocket.Builder clientSocketBuilder = clientReactor.newAsyncSocketBuilder();
        clientSocketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket clientSocket = clientSocketBuilder.build();
        clientSocket.start();
        clientSocket.connect(serverSocket.getLocalAddress());

        clientReactor.shutdown();
        assertTrueEventually(() -> assertTrue(clientSocket.isClosed()));
    }
}
