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
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertCompletesEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public abstract class AsyncSocketTest {

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
    public void test_construction() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();

        assertNotNull(socket.metrics());
        assertNotNull(socket.context());
    }

    @Test
    public void test_remoteAddress_whenNotConnected() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();

        SocketAddress remoteAddress = socket.getRemoteAddress();
        assertNull(remoteAddress);
    }

    @Test
    public void test_localAddress_whenNotConnected() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();

        SocketAddress localAddress = socket.getLocalAddress();
        assertNull(localAddress);
    }

    @Test
    public void test_write_whenNoWriterSet_thenRejectNonIOBuffer() {
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = startServerSocket(reactor, new InetSocketAddress("127.0.0.1", 0));

        serverSocket.start();

        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();

        socket.connect(serverSocket.getLocalAddress()).join();

        assertThrows(IllegalArgumentException.class, () -> socket.write("foobar"));
    }

    @Test
    public void test_insideWriteAndFlush_whenNoWriterSet_thenRejectNonIOBuffer()
            throws ExecutionException, InterruptedException {
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = startServerSocket(reactor, new InetSocketAddress("127.0.0.1", 0));

        serverSocket.start();

        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();
        socket.connect(serverSocket.getLocalAddress()).join();

        Future<Boolean> f = reactor.submit(() -> {
            try {
                socket.insideWriteAndFlush("foobar");
                throw new RuntimeException();
            } catch (IllegalArgumentException e) {
                return true;
            }
        });

        assertCompletesEventually(f);
        assertEquals(Boolean.TRUE, f.get());
    }

    @Test
    public void test_insideWriteAndFlush_whenNotOnEventloop() {
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = startServerSocket(reactor, new InetSocketAddress("127.0.0.1", 0));

        serverSocket.start();

        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();
        socket.connect(serverSocket.getLocalAddress()).join();

        IOBuffer msg = new IOBuffer(0, true);
        msg.flip();
        assertThrows(IllegalStateException.class, () -> socket.insideWriteAndFlush(msg));
    }

    private static AsyncServerSocket startServerSocket(Reactor reactor, SocketAddress bindAddress) {
        CompletableFuture<AsyncSocket> remoteSocketFuture = new CompletableFuture<>();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = bindAddress;
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            remoteSocketFuture.complete(socket);
            socket.start();
        };
        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        return serverSocket;
    }

    @Test
    public void test_connect() throws ExecutionException, InterruptedException {
        Reactor reactor = newReactor();
        CompletableFuture<AsyncSocket> remoteSocketFuture = new CompletableFuture<>();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            remoteSocketFuture.complete(socket);
            socket.start();
        };
        AsyncServerSocket serverSocket = serverSocketBuilder.build();

        serverSocket.start();

        AsyncSocket.Builder localSocketBuilder = reactor.newAsyncSocketBuilder();
        localSocketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket localSocket = localSocketBuilder.build();
        localSocket.start();

        CompletableFuture<Void> connect = localSocket.connect(serverSocket.getLocalAddress());

        assertCompletesEventually(connect);
        assertCompletesEventually(remoteSocketFuture);

        assertNull(connect.join());

        AsyncSocket remoteSocket = remoteSocketFuture.get();

        assertEquals(serverSocket.getLocalAddress(), localSocket.getRemoteAddress());

        assertNotNull(localSocket.getLocalAddress());
        assertNotNull(localSocket.getRemoteAddress());

        assertNotNull(remoteSocket.getLocalAddress());
        assertNotNull(remoteSocket.getRemoteAddress());

        assertEquals(localSocket.getLocalAddress(), remoteSocket.getRemoteAddress());
        assertEquals(localSocket.getRemoteAddress(), remoteSocket.getLocalAddress());
    }

    @Test
    public void test_connect_whenNoServerRunning() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();

        CompletableFuture<Void> future = socket.connect(new InetSocketAddress("127.0.0.1", 5002));

        assertThrows(CompletionException.class, () -> future.join());
    }

    @Test
    public void test_reactor_sockets() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();

        assertEquals(0, reactor.sockets().size());

        socket.start();

        List<AsyncSocket> list = new ArrayList<>();
        reactor.sockets().foreach(list::add);

        assertEquals(Collections.singletonList(socket), list);

        socket.close();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, reactor.sockets().size());
            }
        });
    }

    @Test
    public void test_close_whenNotStarted() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();


        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close_whenNotActivated_andAlreadyClosed() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();

        socket.close();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_start_whenAlreadyStarted() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();

        socket.start();
        assertThrows(CompletionException.class, socket::start);
    }

    @Test
    public void test_readable() {
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
        serverSocket.start();

        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();

        CompletableFuture<Void> connect = socket.connect(serverSocket.getLocalAddress());

        assertCompletesEventually(connect);
        assertNull(connect.join());
        assertEquals(serverSocket.getLocalAddress(), socket.getRemoteAddress());

        assertTrue(socket.isReadable());
        socket.setReadable(false);
        assertFalse(socket.isReadable());
    }

    @Test
    public void test_tooManySockets() {
        int socketLimit = 10;
        Reactor serverReactor = newReactor(builder -> builder.socketsLimit = socketLimit);

        AsyncServerSocket.Builder serverSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };

        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        serverSocket.start();

        // make sure that the client reactor can handle at least this amount of sockets.
        Reactor clientReactor = newReactor(builder -> builder.socketsLimit = socketLimit + 1);

        // we create as many sockets as is allowed.
        List<AsyncSocket> goodSockets = new ArrayList<>();
        for (int k = 0; k < socketLimit; k++) {
            AsyncSocket.Builder socketBuilder = clientReactor.newAsyncSocketBuilder();
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            goodSockets.add(socket);
            socket.start();

            assertCompletesEventually(socket.connect(serverSocket.getLocalAddress()));
        }

        // And then we create one more socket.
        AsyncSocket.Builder socketBuilder = clientReactor.newAsyncSocketBuilder();
        socketBuilder.reader = new DevNullAsyncSocketReader();
        AsyncSocket socket = socketBuilder.build();
        socket.start();

        assertCompletesEventually(socket.connect(serverSocket.getLocalAddress()));

        // make sure that the extra socket is closed eventually
        assertTrueEventually(() -> assertTrue(socket.isClosed()));

        // we need to make sure that the good sockets are still open
        for (AsyncSocket goodSocket : goodSockets) {
            assertFalse(goodSocket.isClosed());
        }
    }
}
