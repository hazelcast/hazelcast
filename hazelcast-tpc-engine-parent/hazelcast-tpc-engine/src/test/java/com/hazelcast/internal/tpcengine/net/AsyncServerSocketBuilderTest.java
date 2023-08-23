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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.Reactor;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public abstract class AsyncServerSocketBuilderTest {
    private final List<Reactor> reactors = new ArrayList<>();

    public abstract Reactor.Builder newReactorBuilder();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    public Reactor newReactor() {
        Reactor reactor = newReactorBuilder().build();
        reactors.add(reactor);
        reactor.start();
        return reactor;
    }

    @Test
    public void test_whenBacklogNegative() {
        Reactor reactor = newReactor();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.backlog = -1;
        serverSocketBuilder.acceptFn = acceptRequest -> {
        };

        assertThrows(IllegalArgumentException.class, () -> serverSocketBuilder.build());
    }

    @Test
    public void test_whenBacklogZero() {
        Reactor reactor = newReactor();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.backlog = 0;
        serverSocketBuilder.acceptFn = acceptRequest -> {
        };

        AsyncServerSocket serverSocket = serverSocketBuilder.build();
    }


    @Test
    public void test_whenBindAddressAndBindAddressGeneratorAreNull() {
        Reactor reactor = newReactor();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.backlog = 1;
        serverSocketBuilder.acceptFn = acceptRequest -> {
        };

        assertThrows(IllegalArgumentException.class, () -> serverSocketBuilder.build());
    }

    @Test
    public void test_whenBindAddressAndBindAddressGeneratorAreNotNull() {
        Reactor reactor = newReactor();
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.backlog = 1;
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.bindAddressGenerator = new Supplier<SocketAddress>() {
            @Override
            public SocketAddress get() {
                return new InetSocketAddress("127.0.0.1", 0);
            }
        };
        serverSocketBuilder.acceptFn = acceptRequest -> {
        };

        assertThrows(IllegalArgumentException.class, () -> serverSocketBuilder.build());
    }

    @Test
    public void test_whenAcceptConsumerNotSet() {
        Reactor reactor = newReactor();

        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.backlog = 1;
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);

        assertThrows(NullPointerException.class, serverSocketBuilder::build);
    }

    @Test
    public void test_whenReactorNotStarted() {
        Reactor reactor = newReactorBuilder().build();
        reactors.add(reactor);

        assertThrows(IllegalStateException.class, reactor::newAsyncServerSocketBuilder);
    }

    @Test
    public void test_whenSuccess() {
        Reactor reactor = newReactor();
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 0);

        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.bindAddress = serverAddress;
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.reader = new DevNullAsyncSocketReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };

        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        serverSocket.start();

        assertFalse(serverSocket.isClosed());
    }
}
