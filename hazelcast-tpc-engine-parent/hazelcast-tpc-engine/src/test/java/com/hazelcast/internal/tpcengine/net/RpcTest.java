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
import com.hazelcast.internal.tpcengine.util.BufferUtil;
import com.hazelcast.internal.tpcengine.util.PrintAtomicLongThread;
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
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;

/**
 * Mimics an RPC call. So there are worker threads that send request with a
 * call id and a payload. This request is 'processed' on the remote side and
 * just returned.
 * <p>
 * On the local side a future is registered on that call id and when the
 * response returns the future is looked up and notified. And then the
 * worker thread will send another request.
 * <p>
 */
public abstract class RpcTest {
    // use small buffers to cause a lot of network scheduling overhead
    // (and shake down problems)
    private static final int SIZEOF_HEADER = SIZEOF_INT + SIZEOF_LONG;

    public int socketBufferSize = 16 * 1024;
    public long durationMillis = 500;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;
    public boolean localWrite;

    private final AtomicLong counter = new AtomicLong();
    private final PrintAtomicLongThread printThread = new PrintAtomicLongThread("at:", counter);

    private final ConcurrentMap<Long, CompletableFuture<IOBuffer>> futures = new ConcurrentHashMap<>();
    private Reactor clientReactor;
    private Reactor serverReactor;

    public abstract Reactor.Builder newReactorBuilder();

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
        AsyncServerSocket serverSocket = newServer();

        AsyncSocket clientSocket = newClient(serverSocket.getLocalAddress());

        AtomicLong callIdGenerator = new AtomicLong();
        LoadGeneratorThread[] threads = new LoadGeneratorThread[threadCount];
        for (int k = 0; k < threadCount; k++) {
            LoadGeneratorThread thread = new LoadGeneratorThread(payloadSize, callIdGenerator, clientSocket);
            threads[k] = thread;
            thread.start();
        }

        assertJoinable(testTimeoutMs, threads);
    }

    private class LoadGeneratorThread extends Thread {
        private final byte[] payload;
        private final AtomicLong callIdGenerator;
        private final AsyncSocket clientSocket;

        private LoadGeneratorThread(int payloadSize,
                                    AtomicLong callIdGenerator,
                                    AsyncSocket clientSocket) {
            this.payload = new byte[payloadSize];
            this.callIdGenerator = callIdGenerator;
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            long endMs = System.currentTimeMillis() + durationMillis;
            while (System.currentTimeMillis() < endMs) {
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
        AsyncSocket.Builder clientSocketBuilder = clientReactor.newAsyncSocketBuilder();
        clientSocketBuilder.options.set(TCP_NODELAY, true);
        clientSocketBuilder.options.set(SO_SNDBUF, socketBufferSize);
        clientSocketBuilder.options.set(SO_RCVBUF, socketBufferSize);
        clientSocketBuilder.reader = new RpcReader(true);
        AsyncSocket clientSocket = clientSocketBuilder.build();

        clientSocket.start();
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private AsyncServerSocket newServer() {
        AsyncServerSocket.Builder serverSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.options.set(SO_RCVBUF, socketBufferSize);
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.options.set(TCP_NODELAY, true);
            socketBuilder.options.set(SO_SNDBUF, socketBufferSize);
            socketBuilder.options.set(SO_RCVBUF, socketBufferSize);
            socketBuilder.reader = new RpcReader(false);
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };
        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        serverSocket.start();
        return serverSocket;
    }

    private class RpcReader extends AsyncSocket.Reader {
        private IOBuffer response;
        private final boolean clientSide;
        private long callId;

        private RpcReader(boolean clientSide) {
            this.clientSide = clientSide;
        }

        @Override
        public void onRead(ByteBuffer src) {
            for (; ; ) {
                if (response == null) {
                    if (src.remaining() < SIZEOF_HEADER) {
                        break;
                    }
                    int payloadSize = src.getInt();
                    callId = src.getLong();

                    response = new IOBuffer(SIZEOF_HEADER + payloadSize, true);
                    response.byteBuffer().limit(SIZEOF_HEADER + payloadSize);
                    response.writeInt(payloadSize);
                    response.writeLong(callId);
                }

                BufferUtil.put(response.byteBuffer(), src);
                //response.write(src);

                if (response.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }
                response.flip();

                if (clientSide) {
                    counter.incrementAndGet();
                    CompletableFuture<IOBuffer> future = futures.remove(callId);
                    if (future == null) {
                        throw new IllegalStateException("Can't find future for callId:" + callId);
                    }
                    future.complete(response);
                } else {
                    boolean offered = localWrite
                            ? socket.insideWriteAndFlush(response)
                            : socket.writeAndFlush(response);

                    if (!offered) {
                        throw new RuntimeException("Socket has no space");
                    }
                }
                response = null;
            }
        }
    }


//    private class MonitorThread extends Thread {
//        private volatile boolean stop;
//
//        public void run() {
//            long lastCount = counter.get();
//            try {
//                while (!stop) {
//                    Thread.sleep(500);
//
//                    long count = counter.get();
//                    if (lastCount == count) {
//                        Consumer<AsyncSocket> print = s -> {
//                            System.out.println("socket wq.empty=" + s.writeQueue.isEmpty() + " flushed=" + (s.flushThread.get() != null));
//                        };
//                        clientReactor.sockets().foreach(print);
//                        serverReactor.sockets().foreach(print);
//                    }
//                    lastCount = count;
//                }
//            } catch (InterruptedException e) {
//            }
//        }
//
//        public void shutdown() {
//            stop = true;
//            interrupt();
//        }
//    }
}
