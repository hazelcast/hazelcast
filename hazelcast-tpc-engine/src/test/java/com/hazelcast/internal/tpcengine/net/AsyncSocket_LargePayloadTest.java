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

import com.hazelcast.internal.tpcengine.PrintAtomicLongThread;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.put;

public abstract class AsyncSocket_LargePayloadTest {
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public static final int SOCKET_BUFFER_SIZE = 16 * 1024;
    public int iterations = 20;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    private final AtomicLong iteration = new AtomicLong();
    private final PrintAtomicLongThread printThread = new PrintAtomicLongThread("at:", iteration);
    private Reactor clientReactor;
    private Reactor serverReactor;

    public abstract ReactorBuilder newReactorBuilder();

    protected void customizeClientSocketBuilder(AsyncSocketBuilder socketBuilder) {
    }

    protected void customizeServerSocketBuilder(AsyncSocketBuilder socketBuilder) {
    }

    @Before
    public void before() {
        clientReactor = newReactorBuilder().build().start();
        serverReactor = newReactorBuilder().build().start();
        printThread.start();
    }

    @After
    public void after() throws InterruptedException {
        terminate(clientReactor);
        terminate(serverReactor);
        printThread.shutdown();
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
        AsyncServerSocket serverSocket = newServer();

        CountDownLatch completionLatch = new CountDownLatch(concurrency);

        AsyncSocket clientSocket = newClient(serverSocket.getLocalAddress(), completionLatch);

        System.out.println("Starting");

        for (int k = 0; k < concurrency; k++) {
            byte[] payload = new byte[payloadSize];
            IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);
            buf.writeInt(payload.length);
            buf.writeLong(iterations / concurrency);
            buf.writeBytes(payload);
            buf.flip();
            if (!clientSocket.write(buf)) {
                throw new RuntimeException();
            }
        }
        clientSocket.flush();

        assertOpenEventually(completionLatch, testTimeoutMs);
    }

    private AsyncSocket newClient(SocketAddress serverAddress, CountDownLatch completionLatch) {
        AsyncSocketBuilder asyncSocketBuilder = clientReactor.newAsyncSocketBuilder()
                .set(TCP_NODELAY, true)
                .set(SO_SNDBUF, SOCKET_BUFFER_SIZE)
                .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                .setReader(new ClientAsyncSocketReader(completionLatch));
        customizeClientSocketBuilder(asyncSocketBuilder);
        AsyncSocket clientSocket = asyncSocketBuilder.build();

        clientSocket.start();
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private AsyncServerSocket newServer() {
        AsyncServerSocket serverSocket = serverReactor.newAsyncServerSocketBuilder()
                .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                .setAcceptConsumer(acceptRequest -> {
                    AsyncSocketBuilder asyncSocketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest)
                            .set(TCP_NODELAY, true)
                            .set(SO_SNDBUF, SOCKET_BUFFER_SIZE)
                            .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                            .setReader(new ServerAsyncSocketReader());
                    customizeServerSocketBuilder(asyncSocketBuilder);
                    asyncSocketBuilder
                            .build()
                            .start();
                })
                .build();
        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();
        return serverSocket;
    }

    private static class ServerAsyncSocketReader extends AsyncSocketReader {
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize = -1;
        private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

        @Override
        public void onRead(ByteBuffer src) {
            for (; ; ) {
                if (payloadSize == -1) {
                    if (src.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        break;
                    }
                    payloadSize = src.getInt();
                    round = src.getLong();
                    if (round < 0) {
                        throw new RuntimeException("round can't be smaller than 0, found:" + round);
                    }
                    payloadBuffer = ByteBuffer.allocate(payloadSize);
                }

                put(payloadBuffer, src);
                if (payloadBuffer.remaining() > 0) {
                    // not all bytes have been received.
                    break;
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
    }

    private class ClientAsyncSocketReader extends AsyncSocketReader {
        private final CountDownLatch latch;
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize;
        private final IOBufferAllocator responseAllocator;

        ClientAsyncSocketReader(CountDownLatch latch) {
            this.latch = latch;
            payloadSize = -1;
            responseAllocator = new NonConcurrentIOBufferAllocator(8, true);
        }

        @Override
        public void onRead(ByteBuffer src) {
            for (; ; ) {
                if (payloadSize == -1) {
                    if (src.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        break;
                    }

                    payloadSize = src.getInt();
                    round = src.getLong();
                    if (round < 0) {
                        throw new RuntimeException("round can't be smaller than 0, found:" + round);
                    }
                    payloadBuffer = ByteBuffer.allocate(payloadSize);
                }

                put(payloadBuffer, src);

                if (payloadBuffer.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }
                payloadBuffer.flip();
                iteration.incrementAndGet();

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
    }

}
