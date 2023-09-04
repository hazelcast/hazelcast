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

package com.hazelcast.internal.tpcengine.iouring;

import com.hazelcast.internal.tpcengine.TpcTestSupport;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.Callable;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LinuxSocketTest {

    private ServerSocket serverSocket;
    private LinuxSocket socket;

    @After
    public void after() {
        closeQuietly(socket);
        closeQuietly(serverSocket);
    }

    @Test
    public void test_createNonBlockingTcpIpv4Socket() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();

        assertNotNull(socket);
        assertFalse("socket can't be blocking", socket.isBlocking());
        assertTrue("socket.fd=" + socket.fd(), socket.fd() >= 0);
        assertEquals(new InetSocketAddress(0), socket.getLocalAddress());
        assertNull(socket.getRemoteAddress());
        assertEquals(LinuxSocket.AF_INET, socket.getAddressFamily());
    }

    // ============= connect ==============

    @Test
    public void test_connect_whenNull() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setBlocking(true);

        assertThrows(NullPointerException.class, () -> socket.connect(null));
    }
//
//    @Test(expected = NullPointerException.class)
//    public void test_connect_whenNotInetSocketAddress(){
////        socket = Socket.openTcpIpv4Socket();
////        socket.setBlocking();
////        SocketAddress socketAddress =
////        socket.connect(null);
//    }

    @Test
    public void test_connect_whenIPv6SocketAddress() throws Exception {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setBlocking(true);

        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("::1"), 5000);
        assertThrows(IOException.class, () -> socket.connect(address));
    }

    @Test
    public void test_connect_whenAccepted() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setBlocking(true);
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));
    }

    @Test
    public void test_connect_whenNoRemoteSocket() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();

        socket.setBlocking(true);
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 10000);
        assertThrows(IOException.class, () -> socket.connect(address));
    }

    @Test
    public void test_connect_whenAlreadyClosed() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setBlocking(true);
        socket.close();

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 10000);
        assertThrows(IOException.class, () -> socket.connect(address));
    }

    @Test
    public void test_connect_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 10000);
        assertThrows(IOException.class, () -> socket.connect(address));
    }

    // ============= close ==============

    @Test
    public void test_close_whenAlreadyClosed() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();

        socket.close();
        socket.close();
    }

    // ============= bind ==============

    @Test
    public void test_bind() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5000);
        socket.bind(address);
        socket.listen(10);

        assertFalse(socket.isBlocking());
        assertFalse(socket.isClosed());
        assertEquals(address, socket.getLocalAddress());
        assertNull(socket.getRemoteAddress());
    }

    @Test
    public void test_bind_whenNull() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();

        assertThrows(NullPointerException.class, () -> socket.bind(null));
    }

    @Test
    public void test_bind_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setBlocking(true);
        socket.close();

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5001);

        assertThrows(IOException.class, () -> socket.bind(address));
    }

    @Test
    public void test_bind_whenIpv4Socket() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setBlocking(true);
        socket.close();

        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("::1"), 5002);

        assertThrows(IOException.class, () -> socket.bind(address));
    }

    // ============= sendBufferSize ==============

    @Test
    public void test_sendBufferSize() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        int size = 256 * 1024;
        socket.setSendBufferSize(size);
        assertTrue(size <= socket.getSendBufferSize());
    }

    @Test
    public void test_setSendBufferSize_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.setSendBufferSize(64 * 1024));
    }

    @Test
    public void test_getSendBufferSize_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.getSendBufferSize());
    }

    // ============= receiveBufferSize ==============

    @Test
    public void test_receiveBufferSize() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        int size = 256 * 1024;
        socket.setReceiveBufferSize(size);
        assertTrue(size <= socket.getReceiveBufferSize());
    }

    @Test
    public void test_setReceiveBufferSize_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();
        assertThrows(IOException.class, () -> socket.setReceiveBufferSize(64 * 1024));
    }

    @Test
    public void test_getReceiveBufferSize_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.getReceiveBufferSize());
    }


    // ============= tcpNoDelay ==============

    @Test
    public void test_tcpNoDelay() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setTcpNoDelay(true);
        assertTrue(socket.isTcpNoDelay());
        socket.setTcpNoDelay(false);
        assertFalse(socket.isTcpNoDelay());
    }

    @Test
    public void test_setTcpNoDelay_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.setTcpNoDelay(true));
    }

    @Test
    public void test_isTcpNoDelay_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.isTcpNoDelay());
    }

    // ============= reusePort ==============

    @Test
    public void test_reusePort() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setReusePort(false);
        assertFalse(socket.isReusePort());
        socket.setReusePort(true);
        assertTrue(socket.isReusePort());
    }

    @Test
    public void test_setReusePort_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.setReusePort(false));
    }

    @Test
    public void test_isReusePort_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.isReusePort());
    }

    // ============= reuseAddress ==============

    @Test
    public void test_reuseAddress() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setReuseAddress(false);
        assertFalse(socket.isReuseAddress());
        socket.setReuseAddress(true);
        assertTrue(socket.isReuseAddress());
    }

    @Test
    public void test_setReuseAddress_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.setReuseAddress(false));
    }

    @Test
    public void test_getReusePort_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.isReuseAddress());
    }

    // ============= keepAlive ==============

    @Test
    public void test_keepAlive() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setKeepAlive(false);
        assertFalse(socket.isKeepAlive());
        socket.setKeepAlive(true);
        assertTrue(socket.isKeepAlive());
    }

    @Test
    public void test_setKeepAlive_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.setKeepAlive(false));
    }

    @Test
    public void test_getKeepAlive_whenClosed() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.isKeepAlive());
    }

    // ============= soLinger ==============

    @Test
    public void test_soLinger() throws IOException {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setSoLinger(10);
        assertEquals(10, socket.getSoLinger());

        socket.setSoLinger(-10);
        assertEquals(-1, socket.getSoLinger());
    }

    // ============= getLocalAddress ==============

    @Test
    public void test_getLocalAddress_whenNotConnected() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();

        InetSocketAddress localAddress = socket.getLocalAddress();
        assertNotNull(localAddress);
        assertEquals(new InetSocketAddress("0.0.0.0", 0), localAddress);
    }

    @Test
    public void test_getLocalAddress_whenConnected() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));

        InetSocketAddress localInetSocketAddress = socket.getLocalAddress();
        assertNotNull(localInetSocketAddress);

        InetAddress localInetAddress = localInetSocketAddress.getAddress();
        TpcTestSupport.assertInstanceOf(Inet4Address.class, localInetAddress);
        byte[] ipAddr = new byte[]{127, 0, 0, 1};
        InetAddress addr = InetAddress.getByAddress(ipAddr);
        assertEquals(addr, localInetSocketAddress.getAddress());
    }

    @Test
    public void test_getLocalAddress_whenClosed() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertNull(socket.getLocalAddress());
    }

    // ============= getRemoteAddress ==============

    @Test
    public void test_getRemoteAddress_whenNotConnected() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        assertNull(socket.getRemoteAddress());
    }

    @Test
    public void test_getRemoteAddress_whenConnected() throws IOException {
        serverSocket = new ServerSocket(10000);
        TpcTestSupport.spawn((Callable<Object>) () -> serverSocket.accept());

        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.setBlocking(true);
        socket.connect(new InetSocketAddress("127.0.0.1", 10000));

        InetSocketAddress remoteAddress = socket.getRemoteAddress();

        assertNotNull(remoteAddress);
        assertEquals(new InetSocketAddress("127.0.0.1", 10000), remoteAddress);
    }

    @Test
    public void test_getRemoteAddress_whenClosed() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertNull(socket.getRemoteAddress());
    }

    // ============= listen ==============

    @Test
    public void test_listen() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.listen(10);
    }

    @Test
    public void test_listen_whenClosed() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
        socket.close();

        assertThrows(IOException.class, () -> socket.listen(-1));
    }
}
