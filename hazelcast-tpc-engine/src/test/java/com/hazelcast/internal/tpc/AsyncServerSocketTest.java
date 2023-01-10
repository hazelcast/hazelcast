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

import com.hazelcast.internal.tpc.nio.NioAsyncServerSocketTest;
import com.hazelcast.internal.tpc.util.JVM;
import org.junit.After;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.tpc.util.CloseUtil.closeAllQuietly;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public abstract class AsyncServerSocketTest {
    public List<Closeable> closeables = new ArrayList<>();
    public List<Eventloop> loops = new ArrayList<>();

    public abstract Eventloop createEventloop();

    public abstract AsyncServerSocket createAsyncServerSocket(Eventloop eventloop);

    @After
    public void after() throws InterruptedException {
        closeAllQuietly(closeables);

        for (Eventloop eventloop : loops) {
            eventloop.shutdown();
            eventloop.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void receiveBufferSize() {
        Eventloop eventloop = createEventloop();
        AsyncServerSocket socket = createAsyncServerSocket(eventloop);
        int size = 64 * 1024;
        socket.setReceiveBufferSize(size);
        assertTrue(socket.getReceiveBufferSize() >= size);
    }

    @Test
    public void reuseAddress() {
        Eventloop eventloop = createEventloop();
        AsyncServerSocket socket = createAsyncServerSocket(eventloop);
        socket.setReuseAddress(true);
        assertTrue(socket.isReuseAddress());

        socket.setReuseAddress(false);
        assertFalse(socket.isReuseAddress());
    }

    private void assumeIfNioThenJava11Plus() {
        if (this instanceof NioAsyncServerSocketTest) {
            assumeTrue(JVM.getMajorVersion() >= 11);
        }
    }

    @Test
    public void reusePort() {
        assumeIfNioThenJava11Plus();

        Eventloop eventloop = createEventloop();
        AsyncServerSocket socket = createAsyncServerSocket(eventloop);
        socket.setReusePort(true);
        assertTrue(socket.isReusePort());

        socket.setReusePort(false);
        assertFalse(socket.isReusePort());
    }
}
