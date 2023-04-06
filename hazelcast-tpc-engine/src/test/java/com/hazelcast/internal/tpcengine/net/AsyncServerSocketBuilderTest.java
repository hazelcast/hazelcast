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

import com.hazelcast.internal.tpcengine.Option;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public abstract class AsyncServerSocketBuilderTest {
    private static final Option<String> UNKNOwN_OPTION = new Option<>("banana", String.class);

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
    public void test_setIfSupported_whenNullOption() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertThrows(NullPointerException.class, () -> builder.setIfSupported(null, 1));
    }

    @Test
    public void test_setIfSupported_whenNullValue() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertThrows(NullPointerException.class, () -> builder.setIfSupported(AsyncSocketOptions.SO_RCVBUF, null));
    }

    @Test
    public void test_setIfSupported_whenNotSupported() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertFalse(builder.setIfSupported(UNKNOwN_OPTION, "banana"));
    }

    @Test
    public void test_setIfSupported_whenSuccess() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertTrue(builder.setIfSupported(AsyncSocketOptions.SO_RCVBUF, 10));
    }

    @Test
    public void test_set_whenNotSupported() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertThrows(UnsupportedOperationException.class, () -> builder.set(UNKNOwN_OPTION, "banana"));
    }

    @Test
    public void test_whenAcceptConsumerNotSet() {
        Reactor reactor = newReactor();
        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        assertThrows(IllegalStateException.class, builder::build);
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

        AsyncServerSocketBuilder builder = reactor.newAsyncServerSocketBuilder();
        builder.setAcceptConsumer(acceptRequest -> {
            AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                    .setReader(new DevNullAsyncSocketReader())
                    .build();

            socket.start();
        });

        AsyncServerSocket serverSocket = builder.build();
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocket.bind(serverAddress);
        serverSocket.start();

        assertFalse(serverSocket.isClosed());
    }
}
