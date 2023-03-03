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

package com.hazelcast.internal.tpc;

import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.hazelcast.internal.tpc.TpcTestSupport.assertCompletesEventually;
import static com.hazelcast.internal.tpc.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public abstract class AsyncSocketTest {

    private final List<Reactor> reactors = new ArrayList<>();

    public abstract ReactorBuilder newReactorBuilder();

    public Reactor newReactor() {
        ReactorBuilder builder = newReactorBuilder();
        Reactor reactor = builder.build();
        reactors.add(reactor);
        return reactor.start();
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    @Test
    public void test_remoteAddress_whenNotConnected() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor
                .newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();
        socket.start();

        SocketAddress remoteAddress = socket.getRemoteAddress();
        assertNull(remoteAddress);
    }

    @Test
    public void test_localAddress_whenNotConnected() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor
                .newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();
        socket.start();

        SocketAddress localAddress = socket.getLocalAddress();
        assertNull(localAddress);
    }

    @Test
    public void test_connect() {
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = reactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {
                    AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                            .setReadHandler(new DevNullReadHandler())
                            .build();
                    socket.start();
                })
                .build();

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(serverAddress);
        serverSocket.start();

        AsyncSocket clientSocket = reactor.newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();
        clientSocket.start();

        CompletableFuture<Void> connect = clientSocket.connect(serverAddress);

        assertCompletesEventually(connect);
        assertNull(connect.join());
        assertEquals(serverAddress, clientSocket.getRemoteAddress());
    }


    @Test
    public void test_connect_whenNoServerRunning() {
        Reactor reactor = newReactor();
        AsyncSocket clientSocket = reactor.newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();
        clientSocket.start();

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        CompletableFuture<Void> connect = clientSocket.connect(serverAddress);


        CompletableFuture<Void> future = clientSocket.connect(new InetSocketAddress(50000));

        assertThrows(CompletionException.class, () -> future.join());
    }

    @Test
    public void test_close_whenNotStarted() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close_whenNotActivated_andAlreadyClosed() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();

        socket.close();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_start_whenAlreadyStarted() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();

        socket.start();
        assertThrows(CompletionException.class, socket::start);
    }

    @Test
    public void test_readable() {
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = reactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {
                    AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                            .setReadHandler(new DevNullReadHandler())
                            .build();
                    socket.start();
                }).build();

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(serverAddress);
        serverSocket.start();

        AsyncSocket clientSocket = reactor.newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();
        clientSocket.start();

        CompletableFuture<Void> connect = clientSocket.connect(serverAddress);

        assertCompletesEventually(connect);
        assertNull(connect.join());
        assertEquals(serverAddress, clientSocket.getRemoteAddress());

        assertTrue(clientSocket.isReadable());
        clientSocket.setReadable(false);
        assertFalse(clientSocket.isReadable());
    }
}
