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
import com.hazelcast.internal.tpcengine.TpcTestSupport;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public abstract class AsyncSocketBuilderTest {
    private static final Option<String> UNKNOwN_OPTION = new Option<>("banana", String.class);

    private final List<Reactor> reactors = new ArrayList<>();

    public abstract ReactorBuilder newReactorBuilder();

    public Reactor newReactor() {
        ReactorBuilder builder = newReactorBuilder();
        Reactor reactor = builder.build();
        reactors.add(reactor);
        return reactor.start();
    }

    @After
    public void after() {
        TpcTestSupport.terminateAll(reactors);
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
    public void test_test_setIfSupported_whenSuccess() {
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
    public void test_build_whenReadHandlerNotSet() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        assertThrows(IllegalStateException.class, () -> builder.build());
    }

    @Test
    public void test_setReadHandler_whenReadHandlerNull() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        assertThrows(NullPointerException.class, () -> builder.setReader(null));
    }

    @Test
    public void test_setReadHandler_whenAlreadyBuild() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        builder.setReader(new DevNullAsyncSocketReader());
        builder.build();

        DevNullAsyncSocketReader readHandler = new DevNullAsyncSocketReader();
        assertThrows(IllegalStateException.class, () -> builder.setReader(readHandler));
    }

    @Test
    public void test_build_whenAlreadyBuild() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        builder.setReader(new DevNullAsyncSocketReader());
        builder.build();

        assertThrows(IllegalStateException.class, () -> builder.build());
    }
}
