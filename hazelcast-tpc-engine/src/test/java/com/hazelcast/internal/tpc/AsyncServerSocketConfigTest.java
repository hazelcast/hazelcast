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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.nio.NioAsyncServerSocketConfigTest;
import com.hazelcast.internal.tpc.util.JVM;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_REUSEADDR;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_REUSEPORT;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpc.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

public abstract class AsyncServerSocketConfigTest {

    private final List<Reactor> reactors = new ArrayList<>();

    public abstract ReactorBuilder newReactorBuilder();

    @After
    public void after() {
        terminateAll(reactors);
    }

    private AsyncServerSocket newServerSocket() {
        ReactorBuilder reactorBuilder = newReactorBuilder();
        Reactor reactor = reactorBuilder.build();
        reactors.add(reactor);
        reactor.start();

        AsyncServerSocket serverSocket = reactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {

                })
                .build();
        return serverSocket;
    }

    @Test
    public void test_set_nullOption() {
        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        assertThrows(NullPointerException.class, () -> options.set(null, 1));
    }

    @Test
    public void test_set_nullValue() {
        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        assertThrows(NullPointerException.class, () -> options.set(SO_RCVBUF, null));
    }

    @Test
    public void test_set_unsupportedOption() {
        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        // SO_SNDBUF is not supported for server sockets.
        assertThrows(UnsupportedOperationException.class, () -> options.set(SO_SNDBUF, 64 * 1024));
    }


    @Test
    public void test_get_unsupportedOption() {
        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        // SO_SNDBUF is not supported for server sockets.
        assertThrows(UnsupportedOperationException.class, () -> options.get(SO_SNDBUF));
    }

    @Test
    public void test_get_nullOption() {
        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        assertThrows(NullPointerException.class, () -> options.get(null));
    }

    @Test
    public void test_SO_RCVBUF() {
        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        options.set(SO_RCVBUF, 64 * 1024);
        assertEquals(Integer.valueOf(64 * 1024), options.get(SO_RCVBUF));
    }

    @Test
    public void test_SO_REUSEADDR() {
        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        options.set(SO_REUSEADDR, true);
        assertEquals(Boolean.TRUE, options.get(SO_REUSEADDR));
        options.set(SO_REUSEADDR, false);
        assertEquals(Boolean.FALSE, options.get(SO_REUSEADDR));
    }


    private void assumeIfNioThenJava11Plus() {
        if (this instanceof NioAsyncServerSocketConfigTest) {
            assumeTrue(JVM.getMajorVersion() >= 11);
        }
    }

    @Test
    public void test_SO_REUSE_PORT() {
        assumeIfNioThenJava11Plus();

        AsyncServerSocket serverSocket = newServerSocket();
        AsyncSocketOptions options = serverSocket.options();
        options.set(SO_REUSEPORT, true);
        assertEquals(Boolean.TRUE, options.get(SO_REUSEPORT));
        options.set(SO_REUSEPORT, false);
        assertEquals(Boolean.FALSE, options.get(SO_REUSEPORT));
    }
}
