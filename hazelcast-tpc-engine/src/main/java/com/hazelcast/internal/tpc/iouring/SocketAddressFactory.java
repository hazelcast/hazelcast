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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * A Factory for creating {@link java.net.SocketAddress}. This is used in the JNI
 * code to create the Java objects.
 */
public class SocketAddressFactory {

    private SocketAddressFactory() {
    }

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
}
