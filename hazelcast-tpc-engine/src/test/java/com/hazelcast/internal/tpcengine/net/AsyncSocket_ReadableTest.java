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
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueTwoSeconds;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static org.junit.Assert.assertEquals;

/**
 * A test verifying the {@link AsyncSocket#setReadable(boolean)} behavior. It does
 * that by turning it on and off multiple times and see if any data is received.
 */
public abstract class AsyncSocket_ReadableTest {

    private Reactor clientReactor;
    private Reactor serverReactor;

    public abstract ReactorBuilder newReactorBuilder();

    @Before
    public void before() {
        clientReactor = newReactorBuilder().build().start();
        serverReactor = newReactorBuilder().build().start();
    }

    @After
    public void after() {
        terminate(clientReactor);
        terminate(serverReactor);
    }

    @Test
    public void test() {
        CompletableFuture<AsyncSocket> remoteSocketFuture = new CompletableFuture<>();
        AsyncServerSocket serverSocket = serverReactor.newAsyncServerSocketBuilder()
                .setAcceptConsumer(acceptRequest -> {
                    AsyncSocket asyncSocket = serverReactor.newAsyncSocketBuilder(acceptRequest)
                            .setReader(new DevNullAsyncSocketReader())
                            .build();
                    asyncSocket.start();
                    remoteSocketFuture.complete(asyncSocket);
                })
                .build();
        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();

        AsyncSocket localSocket = clientReactor.newAsyncSocketBuilder()
                .setReader(new DevNullAsyncSocketReader())
                .build();
        localSocket.start();
        localSocket.connect(serverSocket.getLocalAddress()).join();

        AsyncSocket remoteSocket = remoteSocketFuture.join();

        // disable remote socket readable, send data, and verify the data is not received.
        remoteSocket.setReadable(false);
        localSocket.writeAndFlush(newSingleLongBuffer());
        assertTrueTwoSeconds(() -> assertEquals(0, remoteSocket.metrics().bytesRead()));

        // enable remote socket readable and verify the data is received eventually.
        remoteSocket.setReadable(true);
        assertTrueEventually(() -> assertEquals(SIZEOF_LONG, remoteSocket.metrics().bytesRead()));

        // disable remote socket readable, send more data, and verify the data is not received.
        remoteSocket.setReadable(false);
        localSocket.writeAndFlush(newSingleLongBuffer());
        assertTrueTwoSeconds(() -> assertEquals(SIZEOF_LONG, remoteSocket.metrics().bytesRead()));

        // enable remote socket readable and verify the data is received eventually.
        remoteSocket.setReadable(true);
        assertTrueEventually(() -> assertEquals(2 * SIZEOF_LONG, remoteSocket.metrics().bytesRead()));
    }

    private static IOBuffer newSingleLongBuffer() {
        IOBuffer buffer = new IOBuffer(SIZEOF_LONG, true);
        buffer.writeLong(1);
        buffer.flip();
        return buffer;
    }

}
