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
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.util.IOUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.BufferUtil.put;
import static com.hazelcast.internal.tpc.util.IOUtil.closeResource;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AsyncSocket_LargePayloadTest {
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public static final int SOCKET_BUFFER_SIZE = 16 * 1024;
    public static int requestTotal = 20000;

    private Eventloop clientEventloop;
    private Eventloop serverEventloop;
    private AsyncSocket clientSocket;
    private AsyncServerSocket serverSocket;

    public abstract Eventloop createEventloop();

    @Before
    public void before() {
        clientEventloop = createEventloop();
        serverEventloop = createEventloop();
    }

    @After
    public void after() throws InterruptedException {
        closeResource(clientSocket);
        closeResource(serverSocket);

        if (clientEventloop != null) {
            clientEventloop.shutdown();
            clientEventloop.awaitTermination(10, SECONDS);
        }

        if (serverEventloop != null) {
            serverEventloop.shutdown();
            serverEventloop.awaitTermination(10, SECONDS);
        }
    }

    public class Banana{}

    @Test
    public void test_concurrency_1_payload_0B() throws InterruptedException {
        test(0, 1);
    }

    @Test
    public void test_concurrency_1_payload_1B() throws InterruptedException {
        test(1, 1);
    }

    @Test
    public void test_concurrency_1_payload_1KB() throws InterruptedException {
        test(1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_2KB() throws InterruptedException {
        test(2 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_4KB() throws InterruptedException {
        test(4 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_16KB() throws InterruptedException {
        test(16 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_32KB() throws InterruptedException {
        test(32 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_64KB() throws InterruptedException {
        test(64 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_128KB() throws InterruptedException {
        test(128 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_256KB() throws InterruptedException {
        test(256 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_512KB() throws InterruptedException {
        test(512 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_1MB() throws InterruptedException {
        test(1024 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_2MB() throws InterruptedException {
        test(2048 * 1024, 1);
    }

    @Test
    public void test_concurrency_10_payload_0B() throws InterruptedException {
        test(0, 10);
    }

    @Test
    public void test_concurrency_10_payload_1B() throws InterruptedException {
        test(1, 10);
    }

    @Test
    public void test_concurrency_10_payload_1KB() throws InterruptedException {
        test(1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_2KB() throws InterruptedException {
        test(2 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_4KB() throws InterruptedException {
        test(4 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_16KB() throws InterruptedException {
        test(16 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_32KB() throws InterruptedException {
        test(32 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_64KB() throws InterruptedException {
        test(64 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_128KB() throws InterruptedException {
        test(128 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_256KB() throws InterruptedException {
        test(256 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_512KB() throws InterruptedException {
        test(512 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_1MB() throws InterruptedException {
        test(1024 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_2MB() throws InterruptedException {
        test(2048 * 1024, 10);
    }

    public void test(int payloadSize, int concurrency) throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        AsyncServerSocket serverSocket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        AsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        for (int k = 0; k < concurrency; k++) {
            byte[] payload = new byte[payloadSize];
            IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);
            buf.writeInt(payload.length);
            buf.writeLong(requestTotal / concurrency);
            buf.writeBytes(payload);
            buf.flip();
            if (!clientSocket.write(buf)) {
                throw new RuntimeException();
            }
        }
        clientSocket.flush();

        assertOpenEventually(latch);
    }

    private AsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        clientSocket = clientEventloop.openAsyncSocket();
        clientSocket.setTcpNoDelay(true);
        clientSocket.setSoLinger(0);
        clientSocket.setSendBufferSize(SOCKET_BUFFER_SIZE);
        clientSocket.setReceiveBufferSize(SOCKET_BUFFER_SIZE);
        clientSocket.setReadHandler(new ReadHandler() {
            private boolean firstTime = true;
            private ByteBuffer payloadBuffer;
            private long round;
            private int payloadSize = -1;
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

            @Override
            public void onRead(ByteBuffer receiveBuffer) {
                if (firstTime) {
                    firstTime = false;
                }

                for (; ; ) {
                    if (payloadSize == -1) {
                        if (receiveBuffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                            break;
                        }

                        payloadSize = receiveBuffer.getInt();
                        round = receiveBuffer.getLong();
                        if (round < 0) {
                            throw new RuntimeException("round can't be smaller than 0, found:" + round);
                        }
                        payloadBuffer = ByteBuffer.allocate(payloadSize);
                    }

                    put(payloadBuffer, receiveBuffer);

                    if (payloadBuffer.remaining() > 0) {
                        // not all bytes have been received.
                        break;
                    }
                    payloadBuffer.flip();

                    if (round % 100 == 0) {
                        System.out.println("client round:" + round);
                    }

                    if (round == 0) {
                        latch.countDown();
                    } else {
                        IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                        responseBuf.writeInt(payloadSize);
                        responseBuf.writeLong(round);
                        responseBuf.write(payloadBuffer);
                        responseBuf.flip();
                        if (!socket.unsafeWriteAndFlush(responseBuf)) {
                            throw new RuntimeException();
                        }
                    }
                    payloadSize = -1;
                }
            }
        });
        clientSocket.activate(clientEventloop);
        clientSocket.connect(serverAddress).join();

        return clientSocket;
    }

    private AsyncServerSocket newServer(SocketAddress serverAddress) {
        serverSocket = serverEventloop.openAsyncServerSocket();
        serverSocket.receiveBufferSize(SOCKET_BUFFER_SIZE);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);

        serverSocket.accept(socket -> {
            System.out.println("Server side accepted " + socket);
            socket.setSoLinger(-1);
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(SOCKET_BUFFER_SIZE);
            socket.setReceiveBufferSize(serverSocket.receiveBufferSize());
            socket.setReadHandler(new ReadHandler() {
                private boolean firstTime;
                private ByteBuffer payloadBuffer;
                private long round;
                private int payloadSize = -1;
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer receiveBuffer) {
                    if (firstTime) {
                        firstTime = false;
                    }

                    for (; ; ) {
                        if (payloadSize == -1) {
                            if (receiveBuffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                                break;
                            }
                            payloadSize = receiveBuffer.getInt();
                            round = receiveBuffer.getLong();
                            if (round < 0) {
                                throw new RuntimeException("round can't be smaller than 0, found:" + round);
                            }
                            payloadBuffer = ByteBuffer.allocate(payloadSize);
                        }

                        put(payloadBuffer, receiveBuffer);
                        if (payloadBuffer.remaining() > 0) {
                            // not all bytes have been received.
                            break;
                        }

                        if (round % 100 == 0) {
                            System.out.println("server round:" + round);
                        }

                        payloadBuffer.flip();
                        IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                        responseBuf.writeInt(payloadSize);
                        responseBuf.writeLong(round - 1);
                        responseBuf.write(payloadBuffer);
                        responseBuf.flip();
                        if (!socket.unsafeWriteAndFlush(responseBuf)) {
                            throw new RuntimeException("Socket has no space");
                        }
                        payloadSize = -1;
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
