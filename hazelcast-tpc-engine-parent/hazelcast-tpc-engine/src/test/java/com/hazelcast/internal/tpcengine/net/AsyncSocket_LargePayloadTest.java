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
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.util.PrintAtomicLongThread;
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
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.put;

/**
 * todo: Should be converted to a time based test instead of ieration based
 */
public abstract class AsyncSocket_LargePayloadTest {
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public static final int SOCKET_BUFFER_SIZE = 16 * 1024;
    public int iterations = 20;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    private final AtomicLong iteration = new AtomicLong();
    private final PrintAtomicLongThread monitorThread = new PrintAtomicLongThread("at:", iteration);
    private Reactor clientReactor;
    private Reactor serverReactor;

    public abstract Reactor.Builder newReactorBuilder();

    @Before
    public void before() {
        clientReactor = newReactorBuilder()
                .build()
                .start();
        serverReactor = newReactorBuilder()
                .build()
                .start();
        monitorThread.start();
    }

    @After
    public void after() throws InterruptedException {
        terminate(clientReactor);
        terminate(serverReactor);
        monitorThread.shutdown();
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

    @Test
    public void test_concurrency_10_payload_4MB() throws InterruptedException {
        test(4096 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_8MB() throws InterruptedException {
        test(8192 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_16MB() throws InterruptedException {
        test(16384 * 1024, 10);
    }

    @Test
    public void test_concurrency_10_payload_32MB() throws InterruptedException {
        test(32768 * 1024, 10);
    }

    public void test(int payloadSize, int concurrency) throws InterruptedException {
        AsyncServerSocket serverSocket = newServer();

        CountDownLatch completionLatch = new CountDownLatch(concurrency);

        AsyncSocket clientSocket = newClient(serverSocket.getLocalAddress(), completionLatch);

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
        AsyncSocket.Builder socketBuilder = clientReactor.newAsyncSocketBuilder();
        socketBuilder.options.set(TCP_NODELAY, true);
        socketBuilder.options.set(SO_SNDBUF, SOCKET_BUFFER_SIZE);
        socketBuilder.options.set(SO_RCVBUF, SOCKET_BUFFER_SIZE);
        socketBuilder.reader = new ClientReader(completionLatch);

        AsyncSocket socket = socketBuilder.build();
        socket.start();
        socket.connect(serverAddress).join();
        return socket;
    }

    private AsyncServerSocket newServer() {
        AsyncServerSocket.Builder serverSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.options.set(SO_RCVBUF, SOCKET_BUFFER_SIZE);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.options.set(TCP_NODELAY, true);
            socketBuilder.options.set(SO_SNDBUF, SOCKET_BUFFER_SIZE);
            socketBuilder.options.set(SO_RCVBUF, SOCKET_BUFFER_SIZE);
            socketBuilder.reader = new ServerReader();
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };
        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        // Bind on any available port.
        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();
        return serverSocket;
    }

    private static class ServerReader extends AsyncSocket.Reader {
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
                if (!socket.insideWriteAndFlush(responseBuf)) {
                    throw new RuntimeException("Socket has no space");
                }
                payloadSize = -1;
            }
        }
    }

    private class ClientReader extends AsyncSocket.Reader {
        private final CountDownLatch latch;
        // TODO: This code can be simplified by creating the response buffer directly
        // instead of dealing with an intermediate payload buffer.
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize;
        private final IOBufferAllocator responseAllocator;

        ClientReader(CountDownLatch latch) {
            this.latch = latch;
            this.payloadSize = -1;
            this.responseAllocator = new NonConcurrentIOBufferAllocator(8, true);
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
                    if (!socket.insideWriteAndFlush(responseBuf)) {
                        throw new RuntimeException();
                    }
                }
                payloadSize = -1;
            }
        }
    }

}
