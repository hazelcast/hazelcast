/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.tpcengine.util.BufferUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertJoinable;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;

/**
 * Mimics an RPC call. So there are worker threads that send request with a call id and a payload. This request is
 * 'processed' on the remote side and just returned.
 * <p>
 * On the local side a future is registered on that call id and when the response returns the future is looked up
 * and notified. And then the worker thread will send another request.
 */
public abstract class AsyncSocket_RpcTest {
    private static final int SIZEOF_HEADER = SIZEOF_INT + SIZEOF_LONG;

    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public static final int SOCKET_BUFFER_SIZE = 16 * 1024;
    public long durationMillis = 500;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;
    public boolean localWrite;
    public boolean tcpNoDelay = true;
    private final AtomicLong counter = new AtomicLong();
    private final PrintAtomicLongThread printThread = new PrintAtomicLongThread("at:", counter);

    private final ConcurrentMap<Long, CompletableFuture<IOBuffer>> futures = new ConcurrentHashMap<>();
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
    public void test_threads_1_payload_0B() throws InterruptedException {
        test(1, 0);
    }

    @Test
    public void test_threads_1_payload_1B() throws InterruptedException {
        test(1, 1);
    }

    @Test
    public void test_threads_1_payload_1KB() throws InterruptedException {
        test(1, 1024);
    }

    @Test
    public void test_threads_1_payload_2KB() throws InterruptedException {
        test(1, 2 * 1024);
    }

    @Test
    public void test_threads_1_payload_4KB() throws InterruptedException {
        test(1, 4 * 1024);
    }

    @Test
    public void test_threads_1_payload_16KB() throws InterruptedException {
        test(1, 16 * 1024);
    }

    @Test
    public void test_threads_1_payload_32KB() throws InterruptedException {
        test(1, 32 * 1024);
    }

    @Test
    public void test_threads_1_payload_64KB() throws InterruptedException {
        test(1, 64 * 1024);
    }

    @Test
    public void test_threads_1_payload_128KB() throws InterruptedException {
        test(1, 128 * 1024);
    }

    @Test
    public void test_threads_1_payload_256KB() throws InterruptedException {
        test(1, 256 * 1024);
    }

    @Test
    public void test_threads_1_payload_512KB() throws InterruptedException {
        test(1, 512 * 1024);
    }

    @Test
    public void test_threads_1_payload_1MB() throws InterruptedException {
        test(1, 1024 * 1024);
    }

    @Test
    public void test_threads_10_payload_0B() throws InterruptedException {
        test(10, 0);
    }

    @Test
    public void test_threads_10_payload_1B() throws InterruptedException {
        test(10, 1);
    }

    @Test
    public void test_threads_10_payload_1KB() throws InterruptedException {
        test(10, 1024);
    }

    @Test
    public void test_threads_10_payload_2KB() throws InterruptedException {
        test(10, 2 * 1024);
    }

    @Test
    public void test_threads_10_payload_4KB() throws InterruptedException {
        test(10, 4 * 1024);
    }

    @Test
    public void test_threads_10_payload_16KB() throws InterruptedException {
        test(10, 16 * 1024);
    }

    @Test
    public void test_threads_10_payload_32KB() throws InterruptedException {
        test(10, 32 * 1024);
    }

    @Test
    public void test_threads_10_payload_64KB() throws InterruptedException {
        test(10, 64 * 1024);
    }

    @Test
    public void test_threads_10_payload_128KB() throws InterruptedException {
        test(10, 128 * 1024);
    }

    @Test
    public void test_threads_10_payload_256KB() throws InterruptedException {
        test(10, 256 * 1024);
    }

    @Test
    public void test_threads_10_payload_512KB() throws InterruptedException {
        test(10, 512 * 1024);
    }

    @Test
    public void test_threads_10_payload_1MB() throws InterruptedException {
        test(10, 1024 * 1024);
    }

    @Test
    public void test_threads_100_payload_0B() throws InterruptedException {
        test(100, 0);
    }

    @Test
    public void test_threads_100_payload_1KB() throws InterruptedException {
        test(100, 1024);
    }

    @Test
    public void test_threads_100_payload_2KB() throws InterruptedException {
        test(100, 2 * 1024);
    }

    @Test
    public void test_threads_100_payload_4KB() throws InterruptedException {
        test(100, 4 * 1024);
    }

    @Test
    public void test_threads_100_payload_16KB() throws InterruptedException {
        test(100, 16 * 1024);
    }

    @Test
    public void test_threads_100_payload_32KB() throws InterruptedException {
        test(100, 32 * 1024);
    }

    @Test
    public void test_threads_100_payload_64KB() throws InterruptedException {
        test(100, 64 * 1024);
    }

    @Test
    public void test_threads_100_payload_128KB() throws InterruptedException {
        test(100, 128 * 1024);
    }

    public void test(int threadCount, int payloadSize) throws InterruptedException {
        try (AsyncServerSocket serverSocket = newServer();
             AsyncSocket clientSocket = newClient(serverSocket.getLocalAddress())) {

            AtomicLong callIdGenerator = new AtomicLong();
            LoadGeneratorThread[] threads = new LoadGeneratorThread[threadCount];
            byte[] payload = new byte[payloadSize];
            ThreadLocalRandom.current().nextBytes(payload);
            for (int k = 0; k < threadCount; k++) {
                LoadGeneratorThread thread = new LoadGeneratorThread(payload, callIdGenerator, clientSocket);
                threads[k] = thread;
                thread.start();
            }

            assertJoinable(testTimeoutMs, threads);
        }
    }

    private AsyncSocket newClient(SocketAddress serverAddress) {
        AsyncSocketBuilder clientSocketBuilder = clientReactor.newAsyncSocketBuilder()
                .set(TCP_NODELAY, tcpNoDelay)
                .set(SO_SNDBUF, SOCKET_BUFFER_SIZE)
                .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                .setReader(new RpcClientReader());
        customizeClientSocketBuilder(clientSocketBuilder);
        AsyncSocket clientSocket = clientSocketBuilder.build();

        clientSocket.start();
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private AsyncServerSocket newServer() {
        AsyncServerSocket serverSocket = serverReactor.newAsyncServerSocketBuilder()
                .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                .setAcceptConsumer(acceptRequest -> {
                    AsyncSocketBuilder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest)
                            .set(TCP_NODELAY, tcpNoDelay)
                            .set(SO_SNDBUF, SOCKET_BUFFER_SIZE)
                            .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                            .setReader(new RpcServerReader());
                    customizeServerSocketBuilder(socketBuilder);
                    AsyncSocket asyncSocket = socketBuilder.build();
                    asyncSocket.start();
                })
                .build();

        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();
        return serverSocket;
    }

    private class LoadGeneratorThread extends Thread {
        private final byte[] payload;
        private final AtomicLong callIdGenerator;
        private final AsyncSocket clientSocket;

        private LoadGeneratorThread(byte[] payload,
                                    AtomicLong callIdGenerator,
                                    AsyncSocket clientSocket) {
            this.payload = payload;
            this.callIdGenerator = callIdGenerator;
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            long endMs = System.currentTimeMillis() + durationMillis;
            while (System.currentTimeMillis() < endMs) {
                IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);

                long callId = callIdGenerator.incrementAndGet();
                CompletableFuture<IOBuffer> future = new CompletableFuture<>();
                futures.putIfAbsent(callId, future);

                buf.writeInt(payload.length);
                buf.writeLong(callId);
                buf.writeBytes(payload);
                buf.flip();

                if (!clientSocket.writeAndFlush(buf)) {
                    throw new RuntimeException("clientSocket.writeAndFlush failed");
                }
                future.join();
            }
        }
    }

    // Base class with common functionality
    private abstract static class BaseRpcReader extends AsyncSocketReader {
        private IOBuffer response;
        protected long callId;

        @Override
        public void onRead(ByteBuffer src) {
            for (; ; ) {
                if (response == null) {
                    if (src.remaining() < SIZEOF_HEADER) {
                        break;
                    }
                    int payloadSize = src.getInt();
                    callId = src.getLong();

                    response = new IOBuffer(SIZEOF_HEADER + payloadSize, false);
                    response.byteBuffer().limit(SIZEOF_HEADER + payloadSize);
                    response.writeInt(payloadSize);
                    response.writeLong(callId);
                }

                BufferUtil.put(response.byteBuffer(), src);

                if (response.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }
                response.flip();

                handleComplete(response);

                response = null;
            }
        }

        // Abstract method to be implemented by subclasses
        protected abstract void handleComplete(IOBuffer response);
    }

    // Server-specific implementation
    private class RpcServerReader extends BaseRpcReader {
        @Override
        protected void handleComplete(IOBuffer response) {
            boolean offered = localWrite
                    ? socket.unsafeWriteAndFlush(response)
                    : socket.writeAndFlush(response);

            if (!offered) {
                throw new RuntimeException("Socket has no space");
            }
        }
    }

    // Client-specific implementation
    private class RpcClientReader extends BaseRpcReader {
        @Override
        protected void handleComplete(IOBuffer response) {
            counter.incrementAndGet();

            CompletableFuture<IOBuffer> future = futures.remove(callId);
            if (future == null) {
                throw new IllegalStateException("Can't find future for callId:" + callId);
            }
            future.complete(response);
        }
    }
}
