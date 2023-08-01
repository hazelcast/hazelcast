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

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static org.junit.Assert.assertThrows;

public abstract class AsyncSocketBuilderTest {

    private final List<Reactor> reactors = new ArrayList<>();

    public abstract Reactor.Builder newReactorBuilder();

    public Reactor newReactor() {
        Reactor reactor = newReactorBuilder().build();
        reactors.add(reactor);
        return reactor.start();
    }

    @After
    public void after() {
        terminateAll(reactors);
    }

    @Test
    public void test_build_whenReadHandlerNotSet() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder serverSocketBuilder = reactor.newAsyncSocketBuilder();
        assertThrows(NullPointerException.class, () -> serverSocketBuilder.build());
    }

    @Test
    public void test_writeQueueCapacity_whenNegative() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.writeQueueCapacity = -1;

        assertThrows(IllegalArgumentException.class, () -> socketBuilder.conclude());
    }

    @Test
    public void test_writeQueueCapacity_whenZero() {
        Reactor reactor = newReactor();
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.writeQueueCapacity = 0;

        assertThrows(IllegalArgumentException.class, () -> socketBuilder.conclude());
    }
}
