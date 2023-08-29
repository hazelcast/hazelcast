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
import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_SOCKADDR_STORAGE;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCKADDR_IN_OFFSETOF_SIN_ADDR;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCKADDR_IN_OFFSETOF_SIN_FAMILY;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCKADDR_IN_OFFSETOF_SIN_PORT;
import static com.hazelcast.internal.tpcengine.iouring.LinuxSocket.AF_INET;

/**
 * A Factory for creating {@link java.net.SocketAddress}. This is used in the
 * JNI code to create the Java objects.
 */
public final class SocketAddressUtil {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final int IPV4_BYTES_LENGTH = 4;

    private SocketAddressUtil() {
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

    /**
     * Initializes the memory starting from address 'addr' with the content of the
     * inetSocketAddress.
     *
     * @param inetSocketAddress
     * @param addr
     */
    public static void memsetSocketAddrIn(InetSocketAddress inetSocketAddress, long addr) {
        // clear the memory
        UNSAFE.setMemory(addr, SIZEOF_SOCKADDR_STORAGE, (byte) 0);

        UNSAFE.putShort(addr + SOCKADDR_IN_OFFSETOF_SIN_FAMILY, (short) AF_INET);

        int port = inetSocketAddress.getPort();
        UNSAFE.putShort(addr + SOCKADDR_IN_OFFSETOF_SIN_PORT, toNetworkOrder((short) port));

        InetAddress inetAddress = inetSocketAddress.getAddress();
        if (!(inetAddress instanceof Inet4Address)) {
            throw new RuntimeException("Only IPv4 address");
        }

        byte[] ipv4Bytes = inetAddress.getAddress();
        if (ipv4Bytes.length != IPV4_BYTES_LENGTH) {
            throw new RuntimeException();
        }

        for (int k = 0; k < ipv4Bytes.length; k++) {
            UNSAFE.putByte(addr + SOCKADDR_IN_OFFSETOF_SIN_ADDR + IN_ADDRESS_OFFSETOF_S_ADDR + k, ipv4Bytes[k]);
        }
    }

    private static short toNetworkOrder(short v) {
        return ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN ? v : Short.reverseBytes(v);
    }
}
