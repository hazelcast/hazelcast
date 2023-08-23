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

import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteOrder;

import static com.hazelcast.internal.tpcengine.iouring.Linux.IN_ADDRESS_OFFSETOF_S_ADDR;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_SOCKADDR_IN;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCKADDR_IN_OFFSETOF_SIN_ADDR;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCKADDR_IN_OFFSETOF_SIN_FAMILY;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCKADDR_IN_OFFSETOF_SIN_PORT;
import static com.hazelcast.internal.tpcengine.iouring.LinuxSocket.AF_INET;

/**
 * A Factory for creating {@link java.net.SocketAddress}. This is used in the
 * JNI code to create the Java objects.
 */
public final class SocketAddressFactory {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private SocketAddressFactory() {
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public static InetSocketAddress createIPv4Address(int ip, int port) throws UnknownHostException {
        byte[] addr = {
                (byte) (ip >>> 24),
                (byte) (ip >>> 16),
                (byte) (ip >>> 8),
                (byte) (ip >>> 0)
        };
        InetAddress address = InetAddress.getByAddress(addr);
        return new InetSocketAddress(address, port);
    }

    public static final boolean BIG_ENDIAN_NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    public static void memSet(InetSocketAddress inetSocketAddress, long ptr) {
        UNSAFE.putShort(ptr + SOCKADDR_IN_OFFSETOF_SIN_FAMILY, (short) AF_INET);

        int port = inetSocketAddress.getPort();
       // System.out.println("port:"+port);
        UNSAFE.putShort(ptr + SOCKADDR_IN_OFFSETOF_SIN_PORT, handleNetworkOrder((short) port));

        System.out.println(inetSocketAddress);

        InetAddress inetAddress = inetSocketAddress.getAddress();
        if (!(inetAddress instanceof Inet4Address)) {
            throw new RuntimeException("Only IPv4 address");
        }

        byte[] ipAddress = inetAddress.getAddress();
        if (ipAddress.length != 4) {
            throw new RuntimeException();
        }
        UNSAFE.copyMemory(
                // src + offset
                ipAddress, 0,
                // dst + offset
                null, ptr + SOCKADDR_IN_OFFSETOF_SIN_ADDR + IN_ADDRESS_OFFSETOF_S_ADDR,
                // length
                4);
    }

    private static short handleNetworkOrder(short v) {
        return BIG_ENDIAN_NATIVE_ORDER ? v : Short.reverseBytes(v);
    }
}
