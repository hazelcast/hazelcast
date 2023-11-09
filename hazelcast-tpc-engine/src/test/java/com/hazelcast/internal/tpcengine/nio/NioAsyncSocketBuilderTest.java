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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketBuilderTest;
import com.hazelcast.internal.tpcengine.net.DevNullAsyncSocketReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NioAsyncSocketBuilderTest extends AsyncSocketBuilderTest {

    @Override
    public NioReactorBuilder newReactorBuilder() {
        return new NioReactorBuilder();
    }

    @Test
    public void test_setWriteQueueCapacity_whenNegative() {
        NioReactor reactor = (NioReactor) newReactor();
        NioAsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();

        assertThrows(IllegalArgumentException.class, () -> builder.setWriteQueueCapacity(-1));
    }

    @Test
    public void test_setWriteQueueCapacity_whenZero() {
        NioReactor reactor = (NioReactor) newReactor();
        NioAsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();

        assertThrows(IllegalArgumentException.class, () -> builder.setWriteQueueCapacity(0));
    }

    @Test
    public void test_setWriteQueueCapacity_whenAlreadyBuild() {
        NioReactor reactor = (NioReactor) newReactor();
        NioAsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        builder.setReader(new DevNullAsyncSocketReader());
        AsyncSocket socket = builder.build();

        assertThrows(IllegalStateException.class, () -> builder.setWriteQueueCapacity(1024));
    }

    @Test
    public void test_setWriteQueueCapacity() {
        Reactor reactor = newReactor();
        NioAsyncSocketBuilder builder = (NioAsyncSocketBuilder) reactor.newAsyncSocketBuilder();
        builder.setWriteQueueCapacity(16384);

        assertEquals(16384, builder.writeQueueCapacity);
    }

    @Test
    public void test_setReceiveBufferIsDirect_whenAlreadyBuild() {
        NioReactor reactor = (NioReactor) newReactor();
        NioAsyncSocketBuilder builder = reactor.newAsyncSocketBuilder();
        builder.setReader(new DevNullAsyncSocketReader());
        AsyncSocket socket = builder.build();

        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void test_setReceiveBufferIsDirect() {
        Reactor reactor = newReactor();
        NioAsyncSocketBuilder builder = (NioAsyncSocketBuilder) reactor.newAsyncSocketBuilder();

        builder.setDirectBuffers(false);
        assertFalse(builder.directBuffers);

        builder.setDirectBuffers(true);
        assertTrue(builder.directBuffers);
    }
}
