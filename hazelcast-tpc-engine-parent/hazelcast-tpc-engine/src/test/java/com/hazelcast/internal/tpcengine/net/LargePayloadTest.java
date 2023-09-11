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
import com.hazelcast.internal.tpcengine.util.BufferUtil;
import com.hazelcast.internal.tpcengine.util.PrintAtomicLongThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertCompletesEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.TCP_QUICKACK;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;

/**
 * todo: Should be converted to a time based test instead of ieration based
 */
public abstract class LargePayloadTest {
    // payloadSize (int) + round (long) + hash (int)
    private static final int SIZEOF_HEADER = SIZEOF_INT + SIZEOF_LONG + SIZEOF_INT;

    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public int socketBufferSize = 16 * 1024;
    public int iterations = 2000;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;
    public boolean tcpNoDelay = true;
    public boolean quickAck = true;

    private final AtomicLong iteration = new AtomicLong();
    private final PrintAtomicLongThread monitorThread = new PrintAtomicLongThread("at:", iteration);
    private final List<Future> futures = new ArrayList<>();
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
    public void test_concurrency_1_payload_0B_withoutWriter() throws Exception {
        test(0, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_1B_withoutWriter() throws Exception {
        test(1, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_1KB_withoutWriter() throws Exception {
        test(1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_2KB_withoutWriter() throws Exception {
        test(2 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_4KB_withoutWriter() throws Exception {
        test(4 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_16KB_withoutWriter() throws Exception {
        test(16 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_32KB_withoutWriter() throws Exception {
        test(32 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_64KB_withoutWriter() throws Exception {
        test(64 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_128KB_withoutWriter() throws Exception {
        test(128 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_256KB_withoutWriter() throws Exception {
        test(256 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_512KB_withoutWriter() throws Exception {
        test(512 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_1MB_withoutWriter() throws Exception {
        test(1024 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_1_payload_2MB_withoutWriter() throws Exception {
        test(2048 * 1024, 1, false);
    }

    @Test
    public void test_concurrency_10_payload_0B_withoutWriter() throws Exception {
        test(0, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_1B_withoutWriter() throws Exception {
        test(1, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_1KB_withoutWriter() throws Exception {
        test(1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_2KB_withoutWriter() throws Exception {
        test(2 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_4KB_withoutWriter() throws Exception {
        test(4 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_16KB_withoutWriter() throws Exception {
        test(16 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_32KB_withoutWriter() throws Exception {
        test(32 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_64KB_withoutWriter() throws Exception {
        test(64 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_128KB_withoutWriter() throws Exception {
        test(128 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_256KB_withoutWriter() throws Exception {
        test(256 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_512KB_withoutWriter() throws Exception {
        test(512 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_1MB_withoutWriter() throws Exception {
        test(1024 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_2MB_withoutWriter() throws Exception {
        test(2048 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_4MB_withoutWriter() throws Exception {
        test(4096 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_8MB_withoutWriter() throws Exception {
        test(8192 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_16MB_withoutWriter() throws Exception {
        test(16384 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_10_payload_32MB_withoutWriter() throws Exception {
        test(32768 * 1024, 10, false);
    }

    @Test
    public void test_concurrency_1_payload_0B_withWriter() throws Exception {
        test(0, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_1B_withWriter() throws Exception {
        test(1, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_1KB_withWriter() throws Exception {
        test(1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_2KB_withWriter() throws Exception {
        test(2 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_4KB_withWriter() throws Exception {
        test(4 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_16KB_withWriter() throws Exception {
        test(16 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_32KB_withWriter() throws Exception {
        test(32 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_64KB_withWriter() throws Exception {
        test(64 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_128KB_withWriter() throws Exception {
        test(128 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_256KB_withWriter() throws Exception {
        test(256 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_512KB_withWriter() throws Exception {
        test(512 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_1MB_withWriter() throws Exception {
        test(1024 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_1_payload_2MB_withWriter() throws Exception {
        test(2048 * 1024, 1, true);
    }

    @Test
    public void test_concurrency_10_payload_0B_withWriter() throws Exception {
        test(0, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_1B_withWriter() throws Exception {
        test(1, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_1KB_withWriter() throws Exception {
        test(1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_2KB_withWriter() throws Exception {
        test(2 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_4KB_withWriter() throws Exception {
        test(4 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_16KB_withWriter() throws Exception {
        test(16 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_32KB_withWriter() throws Exception {
        test(32 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_64KB_withWriter() throws Exception {
        test(64 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_128KB_withWriter() throws Exception {
        test(128 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_256KB_withWriter() throws Exception {
        test(256 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_512KB_withWriter() throws Exception {
        test(512 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_1MB_withWriter() throws Exception {
        test(1024 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_2MB_withWriter() throws Exception {
        test(2048 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_4MB_withWriter() throws Exception {
        test(4096 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_8MB_withWriter() throws Exception {
        test(8192 * 1024, 10, true);
    }

    @Test
    public void test_concurrency_10_payload_16MB_withWriter() throws Exception {
        test(16384 * 1024, 10, true);
    }

    // problematic one
    @Test
    public void test_concurrency_10_payload_32MB_withWriter() throws Exception {
        test(16384 * 1024, 50, true);
    }

    public void test(int payloadSize, int concurrency, boolean useWriter) throws Exception {
        AsyncServerSocket serverSocket = newServer(useWriter);

        AsyncSocket clientSocket = newClient(serverSocket.getLocalAddress(), useWriter);

        // Thread.sleep(10000);


        Random random = new Random();
        for (int k = 0; k < concurrency; k++) {
            byte[] payload = new byte[payloadSize];
            random.nextBytes(payload);
            IOBuffer buf = new IOBuffer(SIZEOF_HEADER + payload.length, true);
            buf.writeInt(payload.length);
            buf.writeLong(iterations / concurrency);
            int pos = buf.position();
            // hash placeholder
            buf.writeInt(0);
            buf.writeBytes(payload);
            // and now we write the hash
            buf.putInt(pos, hash(buf, payloadSize));
            buf.flip();
            if (!clientSocket.write(buf)) {
                throw new RuntimeException();
            }
        }
        clientSocket.flush();

        //Thread.sleep(10000);

        System.out.println("Foo");

        assertCompletesEventually(futures, testTimeoutMs);

        System.out.println("iterations:" + iteration.get());

        for (Future future : futures) {
            future.get();
        }
    }

    private AsyncSocket newClient(SocketAddress serverAddress, boolean useWriter) {
        AsyncSocket.Builder socketBuilder = clientReactor.newAsyncSocketBuilder();
        socketBuilder.options.set(TCP_NODELAY, tcpNoDelay);
        socketBuilder.options.set(TCP_QUICKACK, quickAck);
        socketBuilder.options.set(SO_SNDBUF, socketBufferSize);
        socketBuilder.options.set(SO_RCVBUF, socketBufferSize);
        CompletableFuture future = new CompletableFuture();
        futures.add(future);
        socketBuilder.reader = new ClientReader(future);
        if (useWriter) {
            socketBuilder.writer = new IOBufferWriter();
        }

        AsyncSocket socket = socketBuilder.build();
        socket.start();
        socket.connect(serverAddress).join();
        return socket;
    }

    private AsyncServerSocket newServer(boolean useWriter) {
        AsyncServerSocket.Builder serverSocketBuilder = serverReactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.options.set(SO_RCVBUF, socketBufferSize);
        // todo: revert to port 0
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 9089);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.options.set(TCP_NODELAY, tcpNoDelay);
            socketBuilder.options.set(TCP_QUICKACK, quickAck);
            socketBuilder.options.set(SO_SNDBUF, socketBufferSize);
            socketBuilder.options.set(SO_RCVBUF, socketBufferSize);
            socketBuilder.reader = new ServerReader();
            if (useWriter) {
                socketBuilder.writer = new IOBufferWriter();
            }
            AsyncSocket socket = socketBuilder.build();
            socket.start();
        };
        AsyncServerSocket serverSocket = serverSocketBuilder.build();
        // Bind on any available port.
        serverSocket.start();
        return serverSocket;
    }

    private static class ServerReader extends AsyncSocket.Reader {
        private final IOBufferAllocator bufferAllocator = new NonConcurrentIOBufferAllocator(SIZEOF_HEADER, true);
        private IOBuffer message;

        @Override
        public void onRead(ByteBuffer src) {
            for (; ; ) {
                if (message == null) {
                    if (src.remaining() < SIZEOF_HEADER) {
                        break;
                    }
                    int payloadSize = src.getInt();
                    long round = src.getLong();
                    int hash = src.getInt();
                    message = bufferAllocator.allocate(SIZEOF_HEADER + payloadSize);
                    message.byteBuffer().limit(SIZEOF_HEADER + payloadSize);
                    message.writeInt(payloadSize);
                    message.writeLong(round - 1);
                    message.writeInt(hash);
                }

                BufferUtil.put(message.byteBuffer(), src);
                //response.write(src);

                if (message.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }
                message.flip();

                if (!socket.insideWriteAndFlush(message)) {
                    throw new RuntimeException("Socket has no space");
                }
                message = null;
            }
        }
    }

    private static int hash(IOBuffer buffer, int payloadSize) {
        int hash = 1;
        for (int k = SIZEOF_HEADER; k < SIZEOF_HEADER + payloadSize; k++) {
            byte element = buffer.getByte(k);
            hash = 31 * hash + element;
        }
        return hash;
    }

    /**
     * This writer isn't very interesting; it just writes the IOBuffer
     * on the socket send buffer.
     */
    private class IOBufferWriter extends AsyncSocket.Writer {
        private IOBuffer current;

        @Override
        public boolean onWrite(ByteBuffer dst) {
            if (current == null) {
                current = (IOBuffer) writeQueue.poll();
            }

            while (current != null) {
                BufferUtil.put(dst, current.byteBuffer());
                if (current.byteBuffer().hasRemaining()) {
                    // The current message was not fully written
                    return false;
                }

                current.release();
                current = (IOBuffer) writeQueue.poll();
            }

            return true;
        }
    }

    private class ClientReader extends AsyncSocket.Reader {
        private final CompletableFuture future;
        private long round;
        private int payloadSize;
        private final IOBufferAllocator bufferAllocator = new NonConcurrentIOBufferAllocator(SIZEOF_HEADER, true);
        private IOBuffer message;
        private int hash;

        ClientReader(CompletableFuture future) {
            this.future = future;
        }

        @Override
        public void onRead(ByteBuffer src) {
            for (; ; ) {
                if (message == null) {
                    if (src.remaining() < SIZEOF_HEADER) {
                        break;
                    }

                    payloadSize = src.getInt();
                    round = src.getLong();
                    hash = src.getInt();
                    if (round < 0) {
                        throw new RuntimeException("round can't be smaller than 0, found:" + round);
                    }
                    message = bufferAllocator.allocate(SIZEOF_HEADER + payloadSize);
                    message.byteBuffer().limit(SIZEOF_HEADER + payloadSize);
                    message.writeInt(payloadSize);
                    message.writeLong(round);
                    message.writeInt(hash);
                }

                BufferUtil.put(message.byteBuffer(), src);
                //response.write(src);

                if (message.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }
                message.flip();

                int foundHash = hash(message, payloadSize);
                if (foundHash != hash) {
                    src.clear();
                    future.completeExceptionally(new IllegalStateException("Hash mismatch, datastream is corrupted"));
                    socket.close();
                    return;
                }

                iteration.incrementAndGet();

                if (round == 0) {
                    future.complete(null);
                } else {
                    if (!socket.insideWriteAndFlush(message)) {
                        throw new RuntimeException();
                    }
                }
                message = null;
            }
        }
    }

}
