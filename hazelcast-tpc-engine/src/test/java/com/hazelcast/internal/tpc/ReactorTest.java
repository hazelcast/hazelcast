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

import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.Reactor.State.NEW;
import static com.hazelcast.internal.tpc.Reactor.State.RUNNING;
import static com.hazelcast.internal.tpc.Reactor.State.SHUTDOWN;
import static com.hazelcast.internal.tpc.Reactor.State.TERMINATED;
import static com.hazelcast.internal.tpc.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpc.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpc.TpcTestSupport.sleepMillis;
import static com.hazelcast.internal.tpc.TpcTestSupport.terminateAll;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class ReactorTest {

    public final List<Reactor> reactors = new ArrayList<>();

    public abstract Reactor newReactor();

    public abstract ReactorType getType();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
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
        AsyncServerSocket serverSocket = reactor.openTcpAsyncServerSocket();
        serverSocket.setReusePort(true);
        SocketAddress local = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(local);
        serverSocket.accept(socket -> {
        });

        reactor.shutdown();
        assertTrueEventually(() -> assertTrue(serverSocket.isClosed()));
    }

    @Test
    public void test_shutdown_thenAsyncSocketClosed() {
        Reactor serverReactor = newReactor();
        serverReactor.start();
        AsyncServerSocket serverSocket = serverReactor.openTcpAsyncServerSocket();
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.setReusePort(true);
        serverSocket.bind(serverAddress);
        serverSocket.accept(socket -> {
        });

        Reactor clientReactor = newReactor();
        AsyncSocket clientSocket = clientReactor.openTcpAsyncSocket();
        clientSocket.setReadHandler(new ReadHandler() {
            @Override
            public void onRead(ByteBuffer receiveBuffer) {
            }
        });
        clientReactor.start();
        clientSocket.start();
        clientSocket.connect(serverAddress);

        clientReactor.shutdown();
        assertTrueEventually(() -> assertTrue(clientSocket.isClosed()));
    }
}
