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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.TpcTestSupport;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.Callable;

import static com.hazelcast.internal.tpc.TpcTestSupport.assertInstanceOf;
import static com.hazelcast.internal.tpc.iouring.NativeSocket.AF_INET;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NativeSocketTest {

    private ServerSocket serverSocket;
    private NativeSocket socket;

    @After
    public void after() {
        closeQuietly(socket);
        closeQuietly(serverSocket);
    }

    @Test
    public void test_openTcpIpv4Socket() {
        socket = NativeSocket.openTcpIpv4Socket();

        assertNotNull(socket);
        assertTrue("socket.fd=" + socket.fd(), socket.fd() >= 0);

        assertEquals(AF_INET, socket.getAddressFamily());
        assertTrue(socket.isOpen());
        assertFalse(socket.isClosed());
    }

    // ============= connect ==============

    @Test(expected = NullPointerException.class)
    public void test_connect_whenNull() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);
        socket.connect(null);
    }
//
//    @Test(expected = NullPointerException.class)
//    public void test_connect_whenNotInetSocketAddress(){
////        socket = Socket.openTcpIpv4Socket();
////        socket.setBlocking();
////        SocketAddress socketAddress =
////        socket.connect(null);
//    }

    @Test(expected = IOException.class)
    public void test_connect_whenIPv6SocketAddress() throws Exception {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);

        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("::1"), 5000);
        socket.connect(address);
    }

    @Test
    public void test_connect_whenAccepted() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));
    }

    @Test(expected = IOException.class)
    public void test_connect_whenNoRemoteSocket() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();

        socket.setBlocking(true);
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));
    }

    @Test(expected = IOException.class)
    public void test_connect_whenAlreadyClosed() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);
        socket.close();
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));
    }

    @Test(expected = IOException.class)
    public void test_connect_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        assertTrue(socket.isClosed());
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));
    }

    // ============= close ==============

    @Test
    public void test_close_whenAlreadyClosed() {
        socket = NativeSocket.openTcpIpv4Socket();

        socket.close();
        socket.close();
        assertTrue(socket.isClosed());
    }

    // ============= bind ==============


    @Test
    public void test_bind() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5000);
        socket.setBlocking(true);
        socket.bind(address);
        socket.listen(10);
    }

    @Test(expected = NullPointerException.class)
    public void test_bind_whenNull() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);
        socket.bind(null);
    }

    @Test(expected = IOException.class)
    public void test_bind_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);
        socket.close();

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5000);
        socket.bind(address);
    }

    @Test(expected = IOException.class)
    public void test_bind_whenIpv4Socket() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);
        socket.close();

        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("::1"), 5000);
        socket.bind(address);
    }

    // ============= sendBufferSize ==============

    @Test
    public void test_sendBufferSize() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        int size = 256 * 1024;
        socket.setSendBufferSize(size);
        assertTrue(size <= socket.getSendBufferSize());
    }

    @Test(expected = IOException.class)
    public void test_setSendBufferSize_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.setSendBufferSize(64 * 1024);
    }

    @Test(expected = IOException.class)
    public void test_getSendBufferSize_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.getSendBufferSize();
    }

    // ============= receiveBufferSize ==============

    @Test
    public void test_receiveBufferSize() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        int size = 256 * 1024;
        socket.setReceiveBufferSize(size);
        assertTrue(size <= socket.getReceiveBufferSize());
    }

    @Test(expected = IOException.class)
    public void test_setReceiveBufferSize_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.setReceiveBufferSize(64 * 1024);
    }

    @Test(expected = IOException.class)
    public void test_getReceiveBufferSize_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.getReceiveBufferSize();
    }


    // ============= tcpNoDelay ==============

    @Test
    public void test_tcpNoDelay() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setTcpNoDelay(true);
        assertTrue(socket.isTcpNoDelay());
        socket.setTcpNoDelay(false);
        assertFalse(socket.isTcpNoDelay());
    }

    @Test(expected = IOException.class)
    public void test_setTcpNoDelay_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.setTcpNoDelay(true);
    }

    @Test(expected = IOException.class)
    public void test_isTcpNoDelay_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.isTcpNoDelay();
    }

    // ============= reusePort ==============

    @Test
    public void test_reusePort() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setReusePort(false);
        assertFalse(socket.isReusePort());
        socket.setReusePort(true);
        assertTrue(socket.isReusePort());
    }

    @Test(expected = IOException.class)
    public void test_setReusePort_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.setReusePort(false);
    }

    @Test(expected = IOException.class)
    public void test_isReusePort_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.isReusePort();
    }

    // ============= reuseAddress ==============

    @Test
    public void test_reuseAddress() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setReuseAddress(false);
        assertFalse(socket.isReuseAddress());
        socket.setReuseAddress(true);
        assertTrue(socket.isReuseAddress());
    }


    @Test(expected = IOException.class)
    public void test_setReuseAddress_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.setReuseAddress(false);
    }

    @Test(expected = IOException.class)
    public void test_getReusePort_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.isReuseAddress();
    }

    // ============= keepAlive ==============

    @Test
    public void test_keepAlive() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setKeepAlive(false);
        assertFalse(socket.isKeepAlive());
        socket.setKeepAlive(true);
        assertTrue(socket.isKeepAlive());
    }

    @Test(expected = IOException.class)
    public void test_setKeepAlive_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.setKeepAlive(false);
    }

    @Test(expected = IOException.class)
    public void test_getKeepAlive_whenClosed() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();
        socket.isKeepAlive();
    }


    // ============= soLinger ==============

    @Test
    public void test_soLinger() throws IOException {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.setSoLinger(10);
        assertEquals(10, socket.getSoLinger());

        socket.setSoLinger(-10);
        assertEquals(-1, socket.getSoLinger());
    }


    // ============= getLocalAddress ==============

    @Test
    public void test_getLocalAddress_whenNotConnected() {
        socket = NativeSocket.openTcpIpv4Socket();

        InetSocketAddress localAddress = socket.getLocalAddress();
        assertNotNull(localAddress);
        assertEquals(new InetSocketAddress("0.0.0.0", 0), localAddress);
    }

    @Test
    public void test_getLocalAddress_whenConnected() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = NativeSocket.openTcpIpv4Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));

        InetSocketAddress localInetSocketAddress = socket.getLocalAddress();
        assertNotNull(localInetSocketAddress);

        InetAddress localInetAddress = localInetSocketAddress.getAddress();
        assertInstanceOf(Inet4Address.class, localInetAddress);
        byte[] ipAddr = new byte[]{127, 0, 0, 1};
        InetAddress addr = InetAddress.getByAddress(ipAddr);
        assertEquals(addr, localInetSocketAddress.getAddress());
    }

    @Test
    public void test_getLocalAddress_whenClosed() {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();

        assertNull(socket.getLocalAddress());
    }

    // ============= getRemoteAddress ==============

    @Test
    public void test_getRemoteAddress_whenNotConnected() {
        socket = NativeSocket.openTcpIpv4Socket();
        assertNull(socket.getRemoteAddress());
    }

    @Test
    public void test_getRemoteAddress_whenConnected() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = NativeSocket.openTcpIpv4Socket();
        socket.setBlocking(true);
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));

        InetSocketAddress remoteAddress = socket.getRemoteAddress();

        assertNotNull(remoteAddress);
        assertEquals(new InetSocketAddress("127.0.0.1", 10000), remoteAddress);
    }

    @Test
    public void test_getRemoteAddress_whenClosed() {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();

        assertNull(socket.getRemoteAddress());
    }

    // ============= listen ==============

    @Test
    public void test_listen() {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.listen(10);
    }

    @Test(expected = IOException.class)
    public void test_listen_whenClosed() {
        socket = NativeSocket.openTcpIpv4Socket();
        socket.close();

        socket.listen(-1);
    }
}
