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

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertCompletesEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assumeNotIbmJDK8;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static junit.framework.TestCase.assertNotNull;
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

    @BeforeClass
    public static void beforeClass() throws Exception {
        assumeNotIbmJDK8();
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    @Test
    public void test_construction() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor
                .newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();

        assertNotNull(socket.metrics());
        assertNotNull(socket.context());
    }

    @Test
    public void test_remoteAddress_whenNotConnected() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor
                .newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
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
                .setReader(new DevNullAsyncSocketReader())
                .build();
        socket.start();

        SocketAddress localAddress = socket.getLocalAddress();
        assertNull(localAddress);
    }

    @Test
    public void test_connect() throws ExecutionException, InterruptedException {
        Reactor reactor = newReactor();
        CompletableFuture<AsyncSocket> remoteSocketFuture = new CompletableFuture<>();
        AsyncServerSocket serverSocket = reactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {
                    AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                            .setReader(new DevNullAsyncSocketReader())
                            .build();
                    remoteSocketFuture.complete(socket);
                    socket.start();
                })
                .build();

        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();

        AsyncSocket localSocket = reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();
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
        AsyncSocket clientSocket = reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();
        clientSocket.start();

        CompletableFuture<Void> future = clientSocket.connect(new InetSocketAddress("127.0.0.1", 5002));

        assertThrows(CompletionException.class, () -> future.join());
    }

    @Test
    public void test_close_whenNotStarted() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close_whenNotActivated_andAlreadyClosed() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();

        socket.close();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_start_whenAlreadyStarted() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
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
                            .setReader(new DevNullAsyncSocketReader())
                            .build();
                    socket.start();
                }).build();

        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();

        AsyncSocket clientSocket = reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();
        clientSocket.start();

        CompletableFuture<Void> connect = clientSocket.connect(serverSocket.getLocalAddress());

        assertCompletesEventually(connect);
        assertNull(connect.join());
        assertEquals(serverSocket.getLocalAddress(), clientSocket.getRemoteAddress());

        assertTrue(clientSocket.isReadable());
        clientSocket.setReadable(false);
        assertFalse(clientSocket.isReadable());
    }
}
