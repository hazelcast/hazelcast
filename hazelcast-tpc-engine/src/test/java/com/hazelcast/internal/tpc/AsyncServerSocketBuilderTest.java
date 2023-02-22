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

package com.hazelcast.internal.tpc;

import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpc.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public abstract class AsyncServerSocketBuilderTest {

    private final List<Reactor> reactors = new ArrayList<>();

    public abstract ReactorBuilder newReactorBuilder();

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
    public void setOption_whenNullOption() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertThrows(NullPointerException.class, () -> builder.set(null, 1));
    }

    @Test
    public void setOption_whenNullValue() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertThrows(NullPointerException.class, () -> builder.set(AsyncSocketOptions.SO_RCVBUF, null));
    }

    @Test
    public void whenAcceptConsumerNotSet() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void whenReactorNotStarted() {
        Reactor reactor = newReactorBuilder().build();
        reactors.add(reactor);

        assertThrows(IllegalStateException.class, reactor::newAsyncServerSocketBuilder);
    }

    @Test
    public void whenSuccess() {
        Reactor reactor = newReactor();

        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        builder.setAcceptConsumer(acceptRequest -> {
            AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                    .setReadHandler(new DevNullReadHandler())
                    .build();

            socket.start();
        });

        AsyncServerSocket serverSocket = builder.build();
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);
        serverSocket.bind(serverAddress);
        serverSocket.start();

        assertFalse(serverSocket.isClosed());
    }
}
