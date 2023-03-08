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

package com.hazelcast.internal.tpcengine.util;

import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;

import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkValidPort;

/**
 * Allows for {@link AsyncServerSocket} to be bound to a random port within a given range.
 * <p/>
 * To bind on a random port, the port=0 can be passed when using
 * {@link java.net.Socket#bind(SocketAddress)} when you want to to bind to any free port.
 * But this approach can't be used to if the port needs to be constrained within a range.
 * This is why this class exists.
 */
public class BindRandomPort {

    private final int startPort;
    private final int endPort;
    private final AtomicInteger portGenerator = new AtomicInteger();

    public BindRandomPort(int startPort, int endPort) {
        this.startPort = checkValidPort(startPort, "startPort");
        this.endPort = checkValidPort(endPort, "endPort");
        if (endPort < startPort) {
            throw new IllegalArgumentException("startPort " + startPort + " can't be larger than endPort:" + endPort);
        }
        this.portGenerator.set(startPort);
    }

    /**
     * Finds a random port to bind to.
     *
     * @param serverSocket the serverSocket that requires binding.
     * @param localAddress the local address of the NIC. Can be null; which means that it will
     *                     bind on all local addresses.
     * @return the port
     * @throws NullPointerException if serverSocket is null
     * @throws UncheckedIOException if something failed while binding or no free port can be found.
     */
    public int bind(AsyncServerSocket serverSocket, InetAddress localAddress) {
        checkNotNull(serverSocket, "serverSocket");

        for (; ; ) {
            int port = portGenerator.getAndIncrement();
            if (port > endPort) {
                throw new UncheckedIOException(
                        new BindException("Could not find a free port in range " + startPort + "-" + endPort));
            }

            try {
                serverSocket.bind(new InetSocketAddress(localAddress, port));
                return port;
            } catch (UncheckedIOException e) {
                if (!(e.getCause() instanceof BindException)) {
                    throw e;
                }
            }
        }
    }
}
