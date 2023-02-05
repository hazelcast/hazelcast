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

import com.hazelcast.internal.tpc.nio.NioAsyncSocketTest;
import com.hazelcast.internal.tpc.util.JVM;
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
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

public abstract class AsyncSocketTest {

    public final List<Reactor> reactors = new ArrayList<>();

    public abstract Reactor newReactor();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    @Test
    public void test_remoteAddress_whenNotConnected() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.start();

        SocketAddress remoteAddress = socket.getRemoteAddress();
        assertNull(remoteAddress);
    }

    @Test
    public void test_localAddress_whenNotConnected() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.start();

        SocketAddress localAddress = socket.getLocalAddress();
        System.out.println(localAddress);
        assertNull(localAddress);
    }

    @Test
    public void test_receiveBufferSize() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();

        int size = 64 * 1024;
        socket.setReceiveBufferSize(size);
        assertTrue(socket.getReceiveBufferSize() >= size);
    }

    @Test
    public void test_sendBufferSize() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();

        int size = 64 * 1024;
        socket.setSendBufferSize(size);
        assertTrue(socket.getSendBufferSize() >= size);
    }

    @Test
    public void test_tcpNoDelay() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();

        socket.setTcpNoDelay(false);
        assertFalse(socket.isTcpNoDelay());

        socket.setTcpNoDelay(true);
        assertTrue(socket.isTcpNoDelay());
    }

    private void assumeIfNioThenJava11Plus() {
        if (this instanceof NioAsyncSocketTest) {
            assumeTrue(JVM.getMajorVersion() >= 11);
        }
    }

    @Test
    public void test_TcpKeepAliveTime() {
        assumeIfNioThenJava11Plus();

        Reactor reactor = newReactor();

        AsyncSocket socket = reactor.openTcpAsyncSocket();

        socket.setTcpKeepAliveTime(100);
        assertEquals(100, socket.getTcpKeepAliveTime());
    }

    @Test
    public void test_TcpKeepaliveIntvl() {
        assumeIfNioThenJava11Plus();

        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();

        socket.setTcpKeepaliveIntvl(100);
        assertEquals(100, socket.getTcpKeepaliveIntvl());
    }

    @Test
    public void test_TcpKeepAliveProbes() {
        assumeIfNioThenJava11Plus();

        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();

        socket.setTcpKeepAliveProbes(5);
        assertEquals(5, socket.getTcpKeepaliveProbes());
    }

    @Test
    public void test_connect() {
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = reactor.openTcpAsyncServerSocket();

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(serverAddress);
        serverSocket.accept(acceptRequest -> {
        });

        AsyncSocket clientSocket = reactor.openTcpAsyncSocket();
        clientSocket.setReadHandler(mock(ReadHandler.class));
        clientSocket.start();

        CompletableFuture<Void> connect = clientSocket.connect(serverAddress);

        assertCompletesEventually(connect);
        assertNull(connect.join());
        assertEquals(serverAddress, clientSocket.getRemoteAddress());
    }

    @Test
    public void test_connect_whenNotActivated() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        assertThrows(RuntimeException.class, () -> socket.connect(serverAddress).join());
    }

    @Test
    public void test_connect_whenNoServerRunning() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.start();

        CompletableFuture<Void> future = socket.connect(new InetSocketAddress(50000));

        assertThrows(CompletionException.class, () -> future.join());
    }

    @Test
    public void test_close_whenNotActivated() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close_whenNotActivated_andAlreadyClosed() {
        Reactor reactor = newReactor();
        AsyncSocket socket = reactor.openTcpAsyncSocket();
        socket.close();

        socket.close();

        assertTrue(socket.isClosed());
    }


    @Test
    public void test_start_whenAlreadyStarted() {
        Reactor reactor = newReactor();

        AsyncSocket socket = reactor.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));

        socket.start();
        assertThrows(CompletionException.class, () -> socket.start());
    }

    @Test
    public void test_activate_whenReadHandlerNotConfigured() {
        Reactor reactor = newReactor();

        AsyncSocket socket = reactor.openTcpAsyncSocket();
        assertThrows(CompletionException.class, () -> socket.start());
    }

    @Test
    public void test_readable() {
        Reactor reactor = newReactor();
        AsyncServerSocket serverSocket = reactor.openTcpAsyncServerSocket();
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(serverAddress);

        AsyncSocket socket = reactor.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.start();
        socket.connect(serverAddress).join();

        assertTrue(socket.isReadable());
        socket.setReadable(false);
        assertFalse(socket.isReadable());
    }
}
