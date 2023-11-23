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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_KEEPALIVE;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_REUSEADDR;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SSL_ENGINE_FACTORY;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_KEEPCOUNT;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_KEEPIDLE;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_KEEPINTERVAL;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TLS_EXECUTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public abstract class AsyncSocketOptionsTest {

    private static final Option<String> UNKNOwN_OPTION = new Option<>("banana", String.class);
    private static final Option<Boolean> SUPPORTED_OPTION = SO_KEEPALIVE;

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

        return reactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();
    }

    @Test
    public void test_set() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertTrue(options.set(SUPPORTED_OPTION, true));
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
        assertFalse(options.set(UNKNOwN_OPTION, ""));
    }

    @Test
    public void test_get_unsupportedOption() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertNull(options.get(UNKNOwN_OPTION));
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

    @Test
    public void test_TCP_KEEPCOUNT() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        if (options.isSupported(TCP_KEEPCOUNT)) {
            options.set(TCP_KEEPCOUNT, 100);
            assertEquals(Integer.valueOf(100), options.get(TCP_KEEPCOUNT));
        } else {
            assertFalse(options.set(TCP_KEEPCOUNT, 100));
            assertNull(options.get(TCP_KEEPCOUNT));
        }
    }

    @Test
    public void test_TCP_KEEPIDLE() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        if (options.isSupported(TCP_KEEPIDLE)) {
            options.set(TCP_KEEPIDLE, 100);
            assertEquals(Integer.valueOf(100), options.get(TCP_KEEPIDLE));
        } else {
            assertFalse(options.set(TCP_KEEPIDLE, 100));
            assertNull(options.get(TCP_KEEPIDLE));
        }
    }

    @Test
    public void test_TCP_KEEPINTERVAL() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        if (options.isSupported(TCP_KEEPINTERVAL)) {
            options.set(TCP_KEEPINTERVAL, 100);
            assertEquals(Integer.valueOf(100), options.get(TCP_KEEPINTERVAL));
        } else {
            assertFalse(options.set(TCP_KEEPINTERVAL, 100));
            assertNull(options.get(TCP_KEEPINTERVAL));
        }
    }

    @Test
    public void test_SSL_ENGINE_FACTORY() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        Object sslEngineFactory = new Object();
        assertTrue(options.isSupported(SSL_ENGINE_FACTORY));
        assertTrue(options.set(SSL_ENGINE_FACTORY, sslEngineFactory));
        assertEquals(sslEngineFactory, options.get(SSL_ENGINE_FACTORY));
    }

    @Test
    public void test_default_SSL_ENGINE_FACTORY() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertNull(options.get(SSL_ENGINE_FACTORY));
    }

    @Test
    public void test_TLS_EXECUTOR() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        ExecutorService tlsExecutor = Executors.newSingleThreadExecutor();
        assertTrue(options.isSupported(TLS_EXECUTOR));
        assertTrue(options.set(TLS_EXECUTOR, tlsExecutor));
        assertEquals(tlsExecutor, options.get(TLS_EXECUTOR));
    }

    @Test
    public void test_default_TLS_EXECUTOR() {
        AsyncSocket socket = newSocket();
        AsyncSocketOptions options = socket.options();
        assertNull(options.get(TLS_EXECUTOR));
    }
}
