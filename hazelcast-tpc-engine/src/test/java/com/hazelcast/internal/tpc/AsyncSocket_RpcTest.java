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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.BufferUtil.put;
import static com.hazelcast.internal.tpc.util.IOUtil.closeResource;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Mimics an RPC call. So there are worker threads that send request with a call id and a payload. This request is
 * 'processed' on the remote side and just returned.
 * <p>
 * On the local side a future is registered on that call id and when the response returns the future is looked up
 * and notified. And then the worker thread will send another request.
 */
public abstract class AsyncSocket_RpcTest {
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public static final int SOCKET_BUFFER_SIZE = 16 * 1024;
    public static int requestTotal = 20000;
    public final ConcurrentMap<Long, CompletableFuture> futures = new ConcurrentHashMap<>();
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
    public void test_concurrency_100_payload_1KB() throws InterruptedException {
        test(1024, 100);
    }

    @Test
    public void test_concurrency_100_payload_2KB() throws InterruptedException {
        test(2 * 1024, 100);
    }

    @Test
    public void test_concurrency_100_payload_4KB() throws InterruptedException {
        test(4 * 1024, 100);
    }

    @Test
    public void test_concurrency_100_payload_16KB() throws InterruptedException {
        test(16 * 1024, 100);
    }

    @Test
    public void test_concurrency_100_payload_32KB() throws InterruptedException {
        test(32 * 1024, 100);
    }

    @Test
    public void test_concurrency_100_payload_64KB() throws InterruptedException {
        test(64 * 1024, 100);
    }

    @Test
    public void test_concurrency_100_payload_128KB() throws InterruptedException {
        test(128 * 1024, 100);
    }

    public void test(int payloadSize, int concurrency) throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        AsyncServerSocket serverSocket = newServer(serverAddress);

        AsyncSocket clientSocket = newClient(serverAddress);

        System.out.println("Starting");

        AtomicLong callIdGenerator = new AtomicLong();
        List<WorkerThread> threads = new ArrayList<>();
        int requestPerThread = requestTotal / concurrency;
        for (int k = 0; k < concurrency; k++) {
            WorkerThread thread = new WorkerThread(requestPerThread, payloadSize, callIdGenerator);
            threads.add(thread);
            thread.start();
        }

        for (WorkerThread thread : threads) {
            thread.join();
        }
    }

    public class WorkerThread extends Thread {
        private final int requests;
        private final byte[] payload;
        private final AtomicLong callIdGenerator;

        public WorkerThread(int requests, int payloadSize, AtomicLong callIdGenerator) {
            this.requests = requests;
            this.payload = new byte[payloadSize];
            this.callIdGenerator = callIdGenerator;
        }

        @Override
        public void run() {
            for (int k = 0; k < requests; k++) {
                IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);

                long callId = callIdGenerator.incrementAndGet();
                CompletableFuture future = new CompletableFuture();
                futures.putIfAbsent(callId, future);

                buf.writeInt(payload.length);
                buf.writeLong(callId);
                buf.writeBytes(payload);
                buf.flip();
                if (!clientSocket.writeAndFlush(buf)) {
                    throw new RuntimeException();
                }

                future.join();
            }
        }
    }

    private AsyncSocket newClient(SocketAddress serverAddress) {
        clientSocket = clientEventloop.openAsyncSocket();
        clientSocket.setTcpNoDelay(true);
        clientSocket.setSoLinger(0);
        clientSocket.setSendBufferSize(SOCKET_BUFFER_SIZE);
        clientSocket.setReceiveBufferSize(SOCKET_BUFFER_SIZE);
        clientSocket.setReadHandler(new ReadHandler() {
            private boolean firstTime = true;
            private ByteBuffer payloadBuffer;
            private long callId;
            private int payloadSize = -1;

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
                        callId = receiveBuffer.getLong();
                        payloadBuffer = ByteBuffer.allocate(payloadSize);
                    }

                    put(payloadBuffer, receiveBuffer);

                    if (payloadBuffer.remaining() > 0) {
                        // not all bytes have been received.
                        break;
                    }
                    payloadBuffer.flip();
                    CompletableFuture future = futures.remove(callId);
                    if (future == null) {
                        throw new RuntimeException();
                    }
                    future.complete(null);
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
            socket.setSoLinger(-1);
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(SOCKET_BUFFER_SIZE);
            socket.setReceiveBufferSize(serverSocket.receiveBufferSize());
            socket.setReadHandler(new ReadHandler() {
                private boolean firstTime;
                private ByteBuffer payloadBuffer;
                private long callId;
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
                            callId = receiveBuffer.getLong();
                            payloadBuffer = ByteBuffer.allocate(payloadSize);
                        }

                        put(payloadBuffer, receiveBuffer);
                        if (payloadBuffer.remaining() > 0) {
                            // not all bytes have been received.
                            break;
                        }


                        payloadBuffer.flip();
                        IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                        responseBuf.writeInt(payloadSize);
                        responseBuf.writeLong(callId);
                        responseBuf.write(payloadBuffer);
                        responseBuf.flip();

                        if (!socket.unsafeWriteAndFlush(responseBuf)) {
                            throw new RuntimeException("Socket has no space");
                        }
                        payloadSize = -1;
                    }
                }
            });
            System.out.println("Activating server side socket");
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
