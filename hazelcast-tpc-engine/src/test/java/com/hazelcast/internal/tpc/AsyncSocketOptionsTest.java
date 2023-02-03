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

import com.hazelcast.internal.tpc.nio.NioAsyncSocketOptionsTest;
import com.hazelcast.internal.tpc.util.JVM;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_KEEPALIVE;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_REUSEADDR;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_TIMEOUT;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.TCP_KEEPCOUNT;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.TCP_KEEPIDLE;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.TCP_KEEPINTERVAL;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpc.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

public abstract class AsyncSocketOptionsTest {

    public static final Option<String> UNKNOwN_OPTION = new Option<>("banana", String.class);

    private final List<Reactor> reactors = new ArrayList<>();

    public abstract ReactorBuilder newReactorBuilder();

    @After
    public void after() {
        terminateAll(reactors);
    }

    private AsyncSocket newSocket() {
        ReactorBuilder reactorBuilder = newReactorBuilder();
        Reactor reactor = reactorBuilder.build();
        reactors.add(reactor);
        reactor.start();

        AsyncSocket socket = reactor.newAsyncSocketBuilder()
                .setReadHandler(new DevNullReadHandler())
                .build();
        return socket;
    }

    @Test
    public void test_set_nullOption() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertThrows(NullPointerException.class, () -> options.set(null, 1));
    }

    @Test
    public void test_set_nullValue() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertThrows(NullPointerException.class, () -> options.set(SO_RCVBUF, null));
    }

    @Test
    public void test_set_unsupportedOption() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertThrows(UnsupportedOperationException.class, () -> options.set(UNKNOwN_OPTION, ""));
    }

    @Test
    public void test_get_unsupportedOption() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertThrows(UnsupportedOperationException.class, () -> options.get(UNKNOwN_OPTION));
    }

    @Test
    public void test_get_nullOption() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertThrows(NullPointerException.class, () -> options.get(null));
    }

    @Test
    public void test_SO_RCVBUF() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(SO_RCVBUF, 64 * 1024);
        assertEquals(Integer.valueOf(64 * 1024), options.get(SO_RCVBUF));
    }

    @Test
    public void test_SO_SNDBUF() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(SO_SNDBUF, 64 * 1024);
        assertEquals(Integer.valueOf(64 * 1024), options.get(SO_SNDBUF));
    }

    @Test
    public void test_SO_REUSEADDR() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(SO_REUSEADDR, true);
        assertEquals(Boolean.TRUE, options.get(SO_REUSEADDR));
        options.set(SO_REUSEADDR, false);
        assertEquals(Boolean.FALSE, options.get(SO_REUSEADDR));
    }

    @Test
    public void test_TCP_NODELAY() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(TCP_NODELAY, true);
        assertEquals(Boolean.TRUE, options.get(TCP_NODELAY));
        options.set(TCP_NODELAY, false);
        assertEquals(Boolean.FALSE, options.get(TCP_NODELAY));
    }

    @Test
    public void test_SO_KEEPALIVE() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(SO_KEEPALIVE, true);
        assertEquals(Boolean.TRUE, options.get(SO_KEEPALIVE));
        options.set(SO_KEEPALIVE, false);
        assertEquals(Boolean.FALSE, options.get(SO_KEEPALIVE));
    }

    private void assumeIfNioThenJava11Plus() {
        if (this instanceof NioAsyncSocketOptionsTest) {
            assumeTrue(JVM.getMajorVersion() >= 11);
        }
    }

    @Test
    public void test_SO_TIMEOUT() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(SO_TIMEOUT, 3600);
        assertEquals(Integer.valueOf(3600), options.get(SO_TIMEOUT));
    }

    @Test
    public void test_TCP_KEEPCOUNT() {
        assumeIfNioThenJava11Plus();
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(TCP_KEEPCOUNT, 100);
        assertEquals(Integer.valueOf(100), options.get(TCP_KEEPCOUNT));
    }

    @Test
    public void test_TCP_KEEPIDLE() {
        assumeIfNioThenJava11Plus();
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(TCP_KEEPIDLE, 100);
        assertEquals(Integer.valueOf(100), options.get(TCP_KEEPIDLE));
    }

    @Test
    public void test_TCP_KEEPINTERVAL() {
        assumeIfNioThenJava11Plus();
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        options.set(TCP_KEEPINTERVAL, 100);
        assertEquals(Integer.valueOf(100), options.get(TCP_KEEPINTERVAL));
    }


//    @Test
//    public void test_TcpKeepAliveProbes() {
//        assumeIfNioThenJava11Plus();
//
//        Reactor reactor = newReactor();
//        AsyncSocket socket = reactor.openTcpAsyncSocket();
//
//        socket.setTcpKeepAliveProbes(5);
//        assertEquals(5, socket.getTcpKeepaliveProbes());
//    }
}
