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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.AssertTask;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.util.CloseUtil;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertCompletesEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public abstract class AsyncServerSocketTest {

    private final List<Reactor> reactors = new ArrayList<>();

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
        return reactor.start();
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    @Test
    public void test_build() {
        Reactor reactor = newReactor();
        InetSocketAddress bindAddress = new InetSocketAddress("127.0.0.1", 0);
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = bindAddress;
        serverSocketBuilder.acceptFn = acceptRequest -> {
        };
        AsyncServerSocket socket = serverSocketBuilder.build();

        assertFalse(socket.isClosed());
        assertSame(reactor, socket.getReactor());
        assertNotNull(socket.metrics());
        InetSocketAddress localAddress = (InetSocketAddress) socket.getLocalAddress();
        assertNotNull(localAddress);
        assertTrue(localAddress.getPort() > 0);
        assertEquals(localAddress.getPort(), socket.getLocalPort());
        assertTrue(socket.getLocalPort() > 0);
    }

    @Test
    public void test_connect() {
        Reactor reactor = newReactor();

        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        serverSocket.start();

        int clientCount = 5;
        for (int k = 0; k < clientCount; k++) {
            AsyncSocket.Builder clientSocketBuilder = reactor.newAsyncSocketBuilder();
            clientSocketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket clientSocket = clientSocketBuilder.build();
            clientSocket.start();

            CompletableFuture<Void> connect = clientSocket.connect(serverSocket.getLocalAddress());
            assertCompletesEventually(connect);
            assertFalse(clientSocket.isClosed());
        }

        assertEquals(clientCount, serverSocket.metrics.accepted());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, reactor.serverSockets().size());
                // times 2 because there is both an active/passive socket.
                assertEquals(clientCount * 2, reactor.sockets().size());
            }
        });
    }

    @Test
    public void test_accept_whenAcceptThrowsException() {
        Reactor reactor = newReactor();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.acceptFn = acceptRequest -> {
            throw new RuntimeException();
        };
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        AsyncServerSocket serverSocket = serverSocketBuilder.build();

        SocketAddress serverAddress = serverSocket.getLocalAddress();
        serverSocket.start();

        AsyncSocket.Builder clientSocketBuilder = reactor.newAsyncSocketBuilder();
        clientSocketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket clientSocket = clientSocketBuilder.build();
        clientSocket.start();

        CompletableFuture<Void> connect = clientSocket.connect(serverAddress);
        assertCompletesEventually(connect);

        assertFalse(serverSocket.isClosed());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                System.out.println("clientSocket.isClosed():" + clientSocket.isClosed());
                assertTrue(clientSocket.isClosed());
                System.out.println("reactor.serverSockets().size():" + reactor.serverSockets().size());
                assertEquals(1, reactor.serverSockets().size());
                System.out.println("reactor.sockets().size():" + reactor.sockets().size());
                assertEquals(0, reactor.sockets().size());
            }
        });
    }

    @Test
    public void test_accept_whenAcceptRequestIsClosed() {
        Reactor reactor = newReactor();

        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        // closes every completed accept.
        serverSocketBuilder.acceptFn = CloseUtil::closeQuietly;
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);

        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        serverSocket.start();

        AsyncSocket.Builder clientSocketBuilder = reactor.newAsyncSocketBuilder();
        clientSocketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket clientSocket = clientSocketBuilder.build();
        clientSocket.start();

        CompletableFuture<Void> connect = clientSocket.connect(serverSocket.getLocalAddress());
        assertCompletesEventually(connect);

        assertFalse(serverSocket.isClosed());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(clientSocket.isClosed());
                assertEquals(1, reactor.serverSockets().size());
                assertEquals(0, reactor.sockets().size());
            }
        });
    }

    @Test
    public void test_tooManySockets() {
        int serverSocketLimit = 10;
        Reactor serverReactor = newReactor(builder -> builder.serverSocketsLimit = serverSocketLimit);

        List<AsyncServerSocket> goodServerSockets = new ArrayList<>();
        for (int k = 0; k < serverSocketLimit; k++) {
            AsyncServerSocket.Builder serverSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
            serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
            serverSocketBuilder.acceptFn = acceptRequest -> {
                AsyncSocket.Builder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest);
                socketBuilder.reader = new DevNullAsyncSocketReader();
                AsyncSocket socket = socketBuilder.build();
                socket.start();
            };

            AsyncServerSocket serverSocket = serverSocketBuilder.build();
            goodServerSockets.add(serverSocket);
            serverSocket.start();
        }

        AsyncServerSocket.Builder badServerSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
        badServerSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };
        badServerSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        AsyncServerSocket badServerSocket = badServerSocketBuilder.build();
        assertThrows(RuntimeException.class, () -> badServerSocket.start());
        assertTrue(badServerSocket.isClosed());

        // we need to make sure that the good sockets are still open
        for (AsyncServerSocket goodSocket : goodServerSockets) {
            assertFalse(goodSocket.isClosed());
        }
    }

    @Test
    public void test_reactor_serverSockets() {
        Reactor reactor = newReactor();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };

        AsyncServerSocket serverSocket = serverSocketBuilder.build();

        assertEquals(0, reactor.sockets().size());

        serverSocket.start();

        List<AsyncServerSocket> list = new ArrayList<>();
        reactor.serverSockets().foreach(list::add);

        assertEquals(Collections.singletonList(serverSocket), list);

        serverSocket.close();

        assertEquals(0, reactor.sockets().size());
    }

    @Test
    public void test_reactor_closeBeforeStart() {
        Reactor reactor = newReactor();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };

        AsyncServerSocket serverSocket = serverSocketBuilder.build();

        serverSocket.close();

        assertEquals(0, reactor.sockets().size());
        assertTrue(serverSocket.isClosed());
    }
}
