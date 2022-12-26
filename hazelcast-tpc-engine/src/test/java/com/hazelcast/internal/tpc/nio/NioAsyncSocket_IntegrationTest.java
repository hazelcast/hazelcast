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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpc.util.IOUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpc.util.IOUtil.SIZEOF_LONG;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NioAsyncSocket_IntegrationTest {
    public static int requestTotal = 1000;
    public static int concurrency = 1;

    private NioEventloop clientEventloop;
    private NioEventloop serverEventloop;
    private NioAsyncSocket clientSocket;
    private NioAsyncServerSocket serverSocket;

    @Before
    public void before() {
        NioEventloop.NioConfiguration clientConfig = new NioEventloop.NioConfiguration();
        clientConfig.setThreadNameSupplier(() -> "client-eventloop");
        clientEventloop = new NioEventloop(clientConfig);
        clientEventloop.start();

        NioEventloop.NioConfiguration serverConfig = new NioEventloop.NioConfiguration();
        serverConfig.setThreadNameSupplier(() -> "server-eventloop");
        serverEventloop = new NioEventloop(serverConfig);
        serverEventloop.start();
    }

    @After
    public void after() throws InterruptedException {
        if (clientSocket != null) {
            clientSocket.close();
        }

        if (serverSocket != null) {
            serverSocket.close();
        }

        if (clientEventloop != null) {
            clientEventloop.shutdown();
            clientEventloop.awaitTermination(10, SECONDS);
        }

        if (serverEventloop != null) {
            serverEventloop.shutdown();
            serverEventloop.awaitTermination(10, SECONDS);
        }
    }

    @Test
    public void test() throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        serverSocket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        NioAsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        for (int k = 0; k < concurrency; k++) {
            IOBuffer buf = new IOBuffer(128);
            buf.writeInt(8);
            buf.writeLong(requestTotal / concurrency);
            buf.flip();
            clientSocket.write(buf);
        }
        clientSocket.flush();

        assertOpenEventually(latch);
    }

    private NioAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        clientSocket = NioAsyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.readHandler(new NioAsyncReadHandler() {
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

            @Override
            public void onRead(ByteBuffer buffer) {
                for (; ; ) {
                    if (buffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        return;
                    }

                    int size = buffer.getInt();
                    long l = buffer.getLong();
                    if (l == 0) {
                        latch.countDown();
                    } else {
                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(8);
                        buf.writeLong(l);
                        buf.flip();
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            }
        });
        clientSocket.activate(clientEventloop);
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private NioAsyncServerSocket newServer(SocketAddress serverAddress) {
        NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(serverEventloop);
        serverSocket.reuseAddress(true);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            socket.tcpNoDelay(true);
            socket.soLinger(-1);
            socket.readHandler(new NioAsyncReadHandler() {
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer buffer) {
                    for (; ; ) {
                        if (buffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                            return;
                        }
                        int size = buffer.getInt();
                        long l = buffer.getLong();

                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.writeLong(l - 1);
                        buf.flip();
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
