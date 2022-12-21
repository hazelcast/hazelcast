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

import org.junit.After;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.nio.IOUtil.closeResources;
import static org.junit.Assert.assertTrue;

public abstract class AsyncSocketTest {

    public List<Closeable> closeables = new ArrayList<>();

    public abstract Eventloop createEventloop();

    public abstract AsyncSocket createAsyncSocket();

    @After
    public void after() {
        closeResources(closeables);
    }

    @Test
    public void close_whenNotActivated() {
        AsyncSocket socket = createAsyncSocket();
        socket.close();
        assertTrue(socket.isClosed());
    }

    @Test
    public void close_whenNotActivated_andAlreadyClosed() {
        AsyncSocket socket = createAsyncSocket();
        socket.close();
        socket.close();
        assertTrue(socket.isClosed());
    }

    @Test(expected = NullPointerException.class)
    public void activate_whenNull() {
        AsyncSocket socket = createAsyncSocket();
        socket.activate(null);
    }

    @Test(expected = IllegalStateException.class)
    public void activate_whenAlreadyActivated() {
        Eventloop eventloop1 = createEventloop();
        Eventloop eventloop2 = createEventloop();

        AsyncSocket socket = createAsyncSocket();
        eventloop1.start();
        eventloop2.start();

        socket.activate(eventloop1);
        socket.activate(eventloop2);
    }
}
