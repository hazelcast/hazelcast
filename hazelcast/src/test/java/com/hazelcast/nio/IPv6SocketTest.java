/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.nio;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class IPv6SocketTest {


    @Test
    public void test() throws Exception {
        //        System.setProperty("java.net.preferIPv4Stack", "true");
//        System.setProperty("java.net.preferIPv6Addresses", "true");

        final InetAddress ip4 = InetAddress.getByName("127.0.0.1");
        final InetAddress ip42 = InetAddress.getByName("192.168.2.9");
        final InetAddress ip61 = InetAddress.getByName("::1");
        final InetAddress ip62 = InetAddress.getByName("fe80::1%lo0");
        final InetAddress ip63 = InetAddress.getByName("fe80::62c5:47ff:fe05:480a%en0");
        final InetAddress ip6Any = InetAddress.getByName("::");


        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        InetSocketAddress serverInetSocketAddressx = new InetSocketAddress(ip63, 5701);
        serverSocketChannel.socket().bind(serverInetSocketAddressx);
        System.err.println("SERVER: " + serverInetSocketAddressx + " => " + serverSocketChannel);

        System.err.println("");

        final InetSocketAddress serverInetSocketAddress = new InetSocketAddress(ip61, 5701);

        try {
            SocketChannel socketChannel = SocketChannel.open();
            InetSocketAddress inetSocketAddress = new InetSocketAddress(ip61, 0);
            socketChannel.socket().bind(inetSocketAddress);
            System.err.println("CLIENT v6 1- " + inetSocketAddress + " => " + socketChannel);
            socketChannel.connect(serverInetSocketAddress);
            System.err.println("CLIENT v6 2- " + inetSocketAddress + " => " + socketChannel);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.err.println("");
//
        try {
            SocketChannel socketChannel2 = SocketChannel.open();
            InetSocketAddress inetSocketAddress2 = new InetSocketAddress(ip4, 0);
            socketChannel2.socket().bind(inetSocketAddress2);
            System.err.println("CLIENT v4 1- " + inetSocketAddress2 + " => " + socketChannel2);
            socketChannel2.connect(new InetSocketAddress(ip4, 5701));
            System.err.println("CLIENT v4 2- " + inetSocketAddress2 + " => " + socketChannel2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
