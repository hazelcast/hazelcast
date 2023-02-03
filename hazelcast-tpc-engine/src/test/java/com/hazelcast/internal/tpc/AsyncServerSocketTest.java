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

import com.hazelcast.internal.tpc.nio.NioAsyncServerSocketTest;
import com.hazelcast.internal.tpc.util.JVM;
import org.junit.After;
import org.junit.Test;

import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpc.TpcTestSupport.terminate;
import static com.hazelcast.internal.tpc.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public abstract class AsyncServerSocketTest {

    public final List<Reactor> reactors = new ArrayList<>();

    public abstract Reactor newReactor();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    @Test
    public void test_construction() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        assertSame(reactor, socket.getReactor());
    }

    @Test
    public void test_receiveBufferSize() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        int size = 64 * 1024;
        socket.setReceiveBufferSize(size);
        assertTrue(socket.getReceiveBufferSize() >= size);
    }

    @Test
    public void test_setReceiveBufferSize_whenIOException() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.close();
        assertThrows(UncheckedIOException.class, () -> socket.setReceiveBufferSize(64 * 1024));
    }

    @Test
    public void test_getReceiveBufferSize_whenIOException() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.close();
        assertThrows(UncheckedIOException.class, socket::getReceiveBufferSize);
    }

    @Test
    public void test_reuseAddress() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.setReuseAddress(true);
        assertTrue(socket.isReuseAddress());

        socket.setReuseAddress(false);
        assertFalse(socket.isReuseAddress());
    }

    @Test
    public void test_setReuseAddress_whenIOException() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.close();
        assertThrows(UncheckedIOException.class, () -> socket.setReuseAddress(true));
    }

    @Test
    public void test_isReuseAddress_whenIOException() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.close();
        assertThrows(UncheckedIOException.class, socket::isReuseAddress);
    }

    private void assumeIfNioThenJava11Plus() {
        if (this instanceof NioAsyncServerSocketTest) {
            assumeTrue(JVM.getMajorVersion() >= 11);
        }
    }

    @Test
    public void test_reusePort() {
        assumeIfNioThenJava11Plus();

        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.setReusePort(true);
        assertTrue(socket.isReusePort());

        socket.setReusePort(false);
        assertFalse(socket.isReusePort());
    }

    @Test
    public void test_setReusePort_whenException() {
        assumeIfNioThenJava11Plus();

        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.close();

        assertThrows(UncheckedIOException.class, () -> socket.setReusePort(true));
    }

    @Test
    public void test_isReusePort_whenException() {
        assumeIfNioThenJava11Plus();

        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        socket.close();

        assertThrows(UncheckedIOException.class, socket::isReusePort);
    }

    @Test
    public void test_getLocalPort_whenNotYetBound() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();

        int localPort = socket.getLocalPort();
        assertEquals(-1, localPort);
    }

    @Test
    public void test_bind_whenLocalAddressNull() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();

        System.out.println(socket.getLocalPort());
        assertThrows(NullPointerException.class, () -> socket.bind(null));
    }

    @Test
    public void test_getLocalAddress_whenNotBound() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
        assertNull(socket.getLocalAddress());
    }

    @Test
    public void test_accept_andNoBind() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();

        socket.accept(socket1 -> {

        });
    }

    @Test
    public void test_bind_whenBacklogNegative() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();

        SocketAddress local = new InetSocketAddress("127.0.0.1", 5000);

        assertThrows(IllegalArgumentException.class, () -> socket.bind(local, -1));
    }

    @Test
    public void test_bind() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();

        SocketAddress local = new InetSocketAddress("127.0.0.1", 5000);
        socket.bind(local);

        assertEquals(local, socket.getLocalAddress());
        assertEquals(5000, socket.getLocalPort());

        // we need to close the socket manually only when accept is called, the AsyncSocket is part
        // of the reactor
        socket.close();
    }

    @Test
    public void test_bind_whenAlreadyBound() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();

        SocketAddress local = new InetSocketAddress("127.0.0.1", 5000);
        socket.bind(local);
        assertThrows(UncheckedIOException.class, () -> socket.bind(local));

        socket.close();
    }

    @Test
    public void test_accept_whenConsumerNull() {
        Reactor reactor = newReactor();
        AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();

        SocketAddress local = new InetSocketAddress("127.0.0.1", 5000);
        socket.bind(local);

        assertThrows(NullPointerException.class, () -> socket.accept(null));

        socket.close();
    }

    @Test
    public void test_createCloseLoop_withSamereactor() {
        SocketAddress local = new InetSocketAddress("127.0.0.1", 5000);
        Reactor reactor = newReactor();
        for (int k = 0; k < 1000; k++) {
            System.out.println("at:" + k);
            AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
            socket.setReusePort(true);
            socket.bind(local);
            socket.accept(socket1 -> {
            });
            socket.close();
        }
    }

    @Test
    public void test_createCloseLoop_withNewreactor() {
        SocketAddress local = new InetSocketAddress("127.0.0.1", 5000);
        for (int k = 0; k < 1000; k++) {
            System.out.println("at:" + k);
            Reactor reactor = newReactor();
            reactors.remove(reactor);
            AsyncServerSocket socket = reactor.openTcpAsyncServerSocket();
            socket.setReusePort(true);
            socket.bind(local);
            socket.accept(socket1 -> {
            });
            terminate(reactor);
        }
    }
}
