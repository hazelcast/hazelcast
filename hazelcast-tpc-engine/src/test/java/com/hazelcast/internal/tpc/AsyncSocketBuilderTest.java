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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThrows;

public abstract class AsyncSocketBuilderTest {

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
    public void test_build_whenReadHandlerNotSet() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        assertThrows(IllegalStateException.class, () -> builder.build());
    }

    @Test
    public void test_setReadHandler_whenReadHandlerNull() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        assertThrows(NullPointerException.class, () -> builder.setReadHandler(null));
    }

    @Test
    public void test_setReadHandler_whenAlreadyBuild() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        builder.setReadHandler(new DevNullReadHandler());
        builder.build();

        DevNullReadHandler readHandler = new DevNullReadHandler();
        assertThrows(IllegalStateException.class, () -> builder.setReadHandler(readHandler));
    }

    @Test
    public void test_build_whenAlreadyBuild() {
        Reactor reactor = newReactor();
        AsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        builder.setReadHandler(new DevNullReadHandler());
        builder.build();

        assertThrows(IllegalStateException.class, () -> builder.build());
    }
}
