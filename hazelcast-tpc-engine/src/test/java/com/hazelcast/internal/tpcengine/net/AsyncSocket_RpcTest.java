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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertJoinable;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.put;
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
    public int iterations = 200;
    public long testTimeoutSeconds = ASSERT_TRUE_EVENTUALLY_TIMEOUT;
    private final AtomicLong iteration = new AtomicLong();
    private final PrintAtomicLongThread printThread = new PrintAtomicLongThread("at:", iteration);

    private final ConcurrentMap<Long, CompletableFuture> futures = new ConcurrentHashMap<>();
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
        test(2 * 1024 * 1024, 1);
    }

    @Test
    public void test_concurrency_1_payload_16MB() throws InterruptedException {
        test(16 * 1024 * 1024, 1);
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
        test(2 * 1024 * 1024, 10);
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

    @Test
    public void test_concurrency_100_payload_1MB() throws InterruptedException {
        test(1024 * 1024, 100);
    }

    public void test(int payloadSize, int concurrency) throws InterruptedException {
        AsyncServerSocket serverSocket = newServer();

        AsyncSocket clientSocket = newClient(serverSocket.getLocalAddress());

        AtomicLong callIdGenerator = new AtomicLong();
        LoadGeneratorThread[] threads = new LoadGeneratorThread[concurrency];
        int requestPerThread = iterations / concurrency;
        for (int k = 0; k < concurrency; k++) {
            LoadGeneratorThread thread = new LoadGeneratorThread(requestPerThread, payloadSize, callIdGenerator, clientSocket);
            threads[k] = thread;
            thread.start();
        }

        assertJoinable(testTimeoutSeconds, threads);
    }

    private AsyncSocket newClient(SocketAddress serverAddress) {
        AsyncSocketBuilder clientSocketBuilder = clientReactor.newAsyncSocketBuilder()
                .set(TCP_NODELAY, true)
                .set(SO_SNDBUF, SOCKET_BUFFER_SIZE)
                .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                .setReader(new ClientAsyncSocketReader());
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
                            .set(TCP_NODELAY, true)
                            .set(SO_SNDBUF, SOCKET_BUFFER_SIZE)
                            .set(SO_RCVBUF, SOCKET_BUFFER_SIZE)
                            .setReader(new ServerAsyncSocketReader());
                    customizeServerSocketBuilder(socketBuilder);
                    socketBuilder.build()
                            .start();
                })
                .build();

        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        serverSocket.start();
        return serverSocket;
    }

    private static class ServerAsyncSocketReader extends AsyncSocketReader {
        private ByteBuffer payloadBuffer;
        private long callId;
        private int payloadSize = -1;
        private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);
        private long nextPrintMs = System.currentTimeMillis() + SECONDS.toMillis(1);
        private long round;

        @Override
        public void onRead(ByteBuffer src) {
            if (nextPrintMs < System.currentTimeMillis()) {
                nextPrintMs += SECONDS.toMillis(1);
                System.out.println(socket + " round " + round);
            }
            for (; ; ) {
                if (payloadSize == -1) {
                    if (src.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        break;
                    }
                    payloadSize = src.getInt();
                    callId = src.getLong();
                    // todo:can be pooled
                    payloadBuffer = ByteBuffer.allocate(payloadSize);
                }

                put(payloadBuffer, src);
                if (payloadBuffer.remaining() > 0) {
                    // System.out.println(socket + " not all bytes received");
                    // not all bytes have been received.
                    break;
                }

                round++;
                // System.out.println(socket + "  all bytes received");

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
    }

    public class LoadGeneratorThread extends Thread {
        private final int requests;
        private final byte[] payload;
        private final AtomicLong callIdGenerator;
        private final AsyncSocket clientSocket;

        public LoadGeneratorThread(int requests, int payloadSize, AtomicLong callIdGenerator, AsyncSocket clientSocket) {
            this.requests = requests;
            this.payload = new byte[payloadSize];
            this.callIdGenerator = callIdGenerator;
            this.clientSocket = clientSocket;
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

    private class ClientAsyncSocketReader extends AsyncSocketReader {
        private ByteBuffer payloadBuffer;
        private long callId;
        private int payloadSize = -1;
        private long nextPrintMs = System.currentTimeMillis() + SECONDS.toMillis(1);
        private long round;

        @Override
        public void onRead(ByteBuffer src) {
            if (nextPrintMs < System.currentTimeMillis()) {
                nextPrintMs += SECONDS.toMillis(1);
                System.out.println(socket + " round " + round);
            }

            for (; ; ) {
                if (payloadSize == -1) {
                    if (src.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        break;
                    }

                    payloadSize = src.getInt();
                    callId = src.getLong();
                    //todo: can be pooled
                    payloadBuffer = ByteBuffer.allocate(payloadSize);
                }

                put(payloadBuffer, src);

                if (payloadBuffer.remaining() > 0) {
                    //System.out.println(socket + " not all bytes received");
                    // not all bytes have been received.
                    break;
                }

                round++;

                //System.out.println(socket + " all bytes received");
                payloadBuffer.flip();

                iteration.incrementAndGet();
                CompletableFuture future = futures.remove(callId);
                if (future == null) {
                    throw new RuntimeException();
                }
                future.complete(null);
                payloadSize = -1;
            }
        }
    }
}
