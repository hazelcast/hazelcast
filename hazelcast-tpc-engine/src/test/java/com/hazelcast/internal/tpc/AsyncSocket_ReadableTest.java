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

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpc.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpc.TpcTestSupport.assertTrueTwoSeconds;
import static com.hazelcast.internal.tpc.TpcTestSupport.terminate;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static org.junit.Assert.assertEquals;

/**
 * A test verifying the {@link AsyncSocket#setReadable(boolean)} behavior. It does
 * that by turning it on and off multiple times and see if any data is received.
 */
public abstract class AsyncSocket_ReadableTest {

    private Reactor clientReactor;
    private Reactor serverReactor;

    public abstract Reactor newReactor();

    @Before
    public void before() {
        clientReactor = newReactor();
        serverReactor = newReactor();
    }

    @After
    public void after() {
        terminate(clientReactor);
        terminate(serverReactor);
    }

    @Test
    public void test() {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        AsyncServerSocket serverSocket = serverReactor.openTcpAsyncServerSocket();
        CompletableFuture<AsyncSocket> remoteSocketFuture = new CompletableFuture<>();
        serverSocket.bind(serverAddress);
        serverSocket.accept(asyncSocket -> {
            asyncSocket.setReadHandler(new NullReadHandler());
            asyncSocket.activate(serverReactor);
            remoteSocketFuture.complete(asyncSocket);
        });

        AsyncSocket localSocket = clientReactor.openTcpAsyncSocket();
        localSocket.setReadHandler(new NullReadHandler());
        localSocket.activate(clientReactor);
        localSocket.connect(serverAddress).join();

        AsyncSocket remoteSocket = remoteSocketFuture.join();

        // disable remote socket readable, send data, and verify the data is not received.
        remoteSocket.setReadable(false);
        localSocket.writeAndFlush(newSingleLongBuffer());
        assertTrueTwoSeconds(() -> assertEquals(0, remoteSocket.bytesRead.get()));

        // enable remote socket readable and verify the data is received eventually.
        remoteSocket.setReadable(true);
        assertTrueEventually(() -> assertEquals(SIZEOF_LONG, remoteSocket.bytesRead.get()));

        // disable remote socket readable, send more data, and verify the data is not received.
        remoteSocket.setReadable(false);
        localSocket.writeAndFlush(newSingleLongBuffer());
        assertTrueTwoSeconds(() -> assertEquals(SIZEOF_LONG, remoteSocket.bytesRead.get()));

        // enable remote socket readable and verify the data is received eventually.
        remoteSocket.setReadable(true);
        assertTrueEventually(() -> assertEquals(2 * SIZEOF_LONG, remoteSocket.bytesRead.get()));
    }

    private static IOBuffer newSingleLongBuffer() {
        IOBuffer buffer = new IOBuffer(SIZEOF_LONG, true);
        buffer.writeLong(1);
        buffer.flip();
        return buffer;
    }

    // a read handler that tosses the received data.
    static class NullReadHandler extends ReadHandler {
        @Override
        public void onRead(ByteBuffer receiveBuffer) {
            receiveBuffer.position(receiveBuffer.limit());
        }
    }
}
