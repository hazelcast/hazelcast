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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

public abstract class AsyncSocketTest {

    public List<Eventloop> eventloops = new ArrayList<>();

    public abstract Eventloop createEventloop();

    @After
    public void after() throws InterruptedException {
        for (Eventloop eventloop : eventloops) {
            eventloop.shutdown();
            eventloop.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void test_remoteAddress_whenNotConnected() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        socket.activate(eventloop);

        SocketAddress remoteAddress = socket.getRemoteAddress();
        assertNull(remoteAddress);
    }

    @Test
    public void test_receiveBufferSize() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();

        int size = 64 * 1024;
        socket.setReceiveBufferSize(size);
        assertTrue(socket.getReceiveBufferSize() >= size);
    }

    @Test
    public void test_sendBufferSize() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();

        int size = 64 * 1024;
        socket.setSendBufferSize(size);
        assertTrue(socket.getSendBufferSize() >= size);
    }

    @Test
    public void test_tcpNoDelay() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();

        socket.setTcpNoDelay(false);
        assertFalse(socket.isTcpNoDelay());

        socket.setTcpNoDelay(true);
        assertTrue(socket.isTcpNoDelay());
    }

    @Test
    public void test_soLinger() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();

        socket.setSoLinger(10);
        assertEquals(10, socket.getSoLinger());
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

        AsyncSocket socket = eventloop.openAsyncSocket();

        socket.setTcpKeepAliveTime(100);
        assertEquals(100, socket.getTcpKeepAliveTime());
    }

    @Test
    public void test_sTcpKeepaliveIntvl() {
        assumeIfNioThenJava11Plus();

        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();

        socket.setTcpKeepaliveIntvl(100);
        assertEquals(100, socket.getTcpKeepaliveIntvl());
    }

    @Test
    public void test_TcpKeepAliveProbes() {
        assumeIfNioThenJava11Plus();

        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();

        socket.setTcpKeepAliveProbes(5);
        assertEquals(5, socket.getTcpKeepaliveProbes());
    }

//
//    @Test
//    public void remoteAddress_whenNotConnected() throws IOException {
//        AsyncSocket socket = createAsyncSocket();
//        Eventloop eventloop = createEventloop();
//        socket.activate(eventloop);
//
//         int port = 5000;
//        ServerSocket serverSocket = new ServerSocket(port);
//        closeables.add(serverSocket);
//        CompletableFuture future = new CompletableFuture();
//        Thread t = new Thread(){
//            public void run(){
//                try {
//                    serverSocket.accept();
//                    future.complete(null);
//                } catch (IOException e) {
//                   future.completeExceptionally(e);
//                }
//            }
//        };
//        t.start();
//
//        socket.connect(new InetSocketAddress(port)).join();
//
//        System.out.println("remote address:"+socket.remoteAddress());
//        //Socket accept = serverSocket.accept();
//    }

    @Test
    public void test_connect_whenNotActivated() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));
        try {
            socket.connect(new InetSocketAddress(5000));
            fail();
        } catch (IllegalStateException e) {

        }
    }
//
//    @Test
//    public void remoteAddress_whenConnected() {
//        AsyncSocket socket = createAsyncSocket();
//    }
//
//    public void remoteAddress_AfterConnectedAndClosed() {
//
//    }

    @Test
    public void test_close_whenNotActivated() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();
        socket.close();
        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close_whenNotActivated_andAlreadyClosed() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();
        socket.close();
        socket.close();
        assertTrue(socket.isClosed());
    }

    @Test
    public void test_activate_whenNull() {
        Eventloop eventloop = createEventloop();
        AsyncSocket socket = eventloop.openAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));

        assertThrows(NullPointerException.class, ()->socket.activate(null));
    }

    @Test
    public void test_activate_whenAlreadyActivated() {
        Eventloop eventloop1 = createEventloop();
        Eventloop eventloop2 = createEventloop();

        AsyncSocket socket = eventloop1.openAsyncSocket();
        socket.setReadHandler(mock(ReadHandler.class));

        socket.activate(eventloop1);
        assertThrows(IllegalStateException.class, ()->socket.activate(eventloop2));
    }

    @Test
    public void test_activate_whenReadHandlerNotConfigured() {
        Eventloop eventloop = createEventloop();

        AsyncSocket socket = eventloop.openAsyncSocket();
        assertThrows(IllegalStateException.class, ()->socket.activate(eventloop));
    }
}
