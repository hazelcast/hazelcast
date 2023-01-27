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

    public List<Eventloop> eventloops = new ArrayList<>();

    public abstract Eventloop createEventloop();

    @After
    public void after() throws InterruptedException {
        terminateAll(eventloops);
    }

    @Test
    public void test_remoteAddress_whenNotConnected() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.activate(eventloop);

        SocketAddress remoteAddress = socket.getRemoteAddress();
        assertNull(remoteAddress);
    }


    @Test
    public void test_localAddress_whenNotConnected() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.activate(eventloop);

        SocketAddress localAddress = socket.getLocalAddress();
        System.out.println(localAddress);
        assertNull(localAddress);
    }

    @Test
    public void test_receiveBufferSize() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();

        int size = 64 * 1024;
        socket.setReceiveBufferSize(size);
        assertTrue(socket.getReceiveBufferSize() >= size);
    }

    @Test
    public void test_sendBufferSize() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();

        int size = 64 * 1024;
        socket.setSendBufferSize(size);
        assertTrue(socket.getSendBufferSize() >= size);
    }

    @Test
    public void test_tcpNoDelay() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();

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

        Eventloop eventloop = createEventloop();

        AsyncSocket socket = eventloop.openTcpAsyncSocket();

        socket.setTcpKeepAliveTime(100);
        assertEquals(100, socket.getTcpKeepAliveTime());
    }

    @Test
    public void test_TcpKeepaliveIntvl() {
        assumeIfNioThenJava11Plus();

        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();

        socket.setTcpKeepaliveIntvl(100);
        assertEquals(100, socket.getTcpKeepaliveIntvl());
    }

    @Test
    public void test_TcpKeepAliveProbes() {
        assumeIfNioThenJava11Plus();

        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();

        socket.setTcpKeepAliveProbes(5);
        assertEquals(5, socket.getTcpKeepaliveProbes());
    }

    @Test
    public void test_connect() {
        Eventloop eventloop = createEventloop();
        AsyncServerSocket serverSocket = eventloop.openTcpAsyncServerSocket();

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(serverAddress);
        serverSocket.accept(asyncSocket -> {
        });

        AsyncSocket clientSocket = eventloop.openTcpAsyncSocket();
        clientSocket.setReadHandler(mock(ReadHandler.class));
        clientSocket.activate(eventloop);

        CompletableFuture<Void> connect = clientSocket.connect(serverAddress);

        assertCompletesEventually(connect);
        assertNull(connect.join());
        assertEquals(serverAddress, clientSocket.getRemoteAddress());
    }

    @Test
    public void test_connect_whenNotActivated() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        assertThrows(IllegalStateException.class, () -> socket.connect(serverAddress));
    }

    @Test
    public void test_connect_whenNoServerRunning() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.activate(eventloop);

        CompletableFuture<Void> future = socket.connect(new InetSocketAddress(50000));

        assertThrows(CompletionException.class, () -> future.join());
    }

    @Test
    public void test_close_whenNotActivated() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close_whenNotActivated_andAlreadyClosed() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        socket.close();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_activate_whenNull() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));

        assertThrows(NullPointerException.class, () -> socket.activate(null));
    }

    @Test
    public void test_activate_whenAlreadyActivated() {
        Eventloop eventloop1 = createEventloop();
        Eventloop eventloop2 = createEventloop();

        AsyncSocket socket = eventloop1.openTcpAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));

        socket.activate(eventloop1);
        assertThrows(IllegalStateException.class, () -> socket.activate(eventloop2));
    }

    @Test
    public void test_activate_whenReadHandlerNotConfigured() {
        Eventloop eventloop = createEventloop();

        AsyncSocket socket = eventloop.openTcpAsyncSocket();
        assertThrows(IllegalStateException.class, () -> socket.activate(eventloop));
    }
}
