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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertJoinable;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;

/**
 * The purpose of this test is to ensure that scheduling of sockets
 * on reactors doesn't lead to problems like sockets scheduled multiple
 * times or not at all.
 */
public abstract class SocketSchedulingSoakTest {
    private static final int SIZEOF_HEADER = SIZEOF_INT + SIZEOF_LONG;

    // The public properties are the tunnables for this soak test.
    public long durationMillis = TimeUnit.SECONDS.toMillis(5);
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public int socketBufferSize = 16 * 1024;
    public int concurrency = 10;
    public boolean randomizeConcurrency = true;
    public int payloadSize = 100;
    public boolean randomizePayloadSize = true;
    public int socketCount = 200;
    public int reactorCount = 4;
    public boolean useWriter = false;
    public int loadGeneratorThreadCount = 20;
    public boolean localWrite = false;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    private final AtomicLong callIdGenerator = new AtomicLong();
    private final AtomicLong counter = new AtomicLong();
    private final PrintAtomicLongThread monitorThread = new PrintAtomicLongThread("at:", counter);
    private final List<Reactor> reactorList = new ArrayList<>();
    private final List<AsyncServerSocket> serverSocketList = new ArrayList<>();
    private final List<AsyncSocket> socketList = new ArrayList<>();
    private final ConcurrentMap<Long, CompletableFuture<IOBuffer>> futures = new ConcurrentHashMap<>();

    public abstract Reactor.Builder newReactorBuilder();

    @Before
    public void before() {
        for (int k = 0; k < reactorCount; k++) {
            Reactor reactor = newReactorBuilder().build();
            reactorList.add(reactor);
            reactor.start();
        }

        for (int k = 0; k < reactorCount; k++) {
            Reactor reactor = reactorList.get(k);
            AsyncServerSocket serverSocket = startServerSocket(reactor);
            serverSocketList.add(serverSocket);
        }

        monitorThread.start();

        Random random = new Random();
        for (int k = 0; k < socketCount; k++) {
            AsyncServerSocket serverSocket = serverSocketList.get(k % reactorCount);
            Reactor reactor = reactorList.get(random.nextInt(reactorCount));
            AsyncSocket socket = startAndConnectSocket(reactor, serverSocket.localAddress);
            socketList.add(socket);
        }
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactorList);
        monitorThread.shutdown();
    }

    @Test
    public void test() throws Exception {
        LoadGeneratorThread[] threads = new LoadGeneratorThread[loadGeneratorThreadCount];
        for (int k = 0; k < loadGeneratorThreadCount; k++) {
            LoadGeneratorThread thread = new LoadGeneratorThread(payloadSize);
            threads[k] = thread;
            thread.start();
        }

        assertJoinable(testTimeoutMs, threads);
    }

    private AsyncSocket startAndConnectSocket(Reactor reactor, SocketAddress serverAddress) {
        AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder();
        socketBuilder.options.set(TCP_NODELAY, true);
        socketBuilder.options.set(SO_SNDBUF, socketBufferSize);
        socketBuilder.options.set(SO_RCVBUF, socketBufferSize);
        socketBuilder.reader = new RpcReader(true);
        if (useWriter) {
            socketBuilder.writer = new IOBufferWriter();
        }

        AsyncSocket socket = socketBuilder.build();
        socket.start();
        socket.connect(serverAddress).join();
        return socket;
    }

    private AsyncServerSocket startServerSocket(Reactor reactor) {
        AsyncServerSocket.Builder serverSocketBuilder = reactor.newAsyncServerSocketBuilder();
        serverSocketBuilder.options.set(SO_RCVBUF, socketBufferSize);
        serverSocketBuilder.bindAddress = new InetSocketAddress("127.0.0.1", 0);
        serverSocketBuilder.acceptFn = acceptRequest -> {
            AsyncSocket.Builder socketBuilder = reactor.newAsyncSocketBuilder(acceptRequest);
            socketBuilder.options.set(TCP_NODELAY, true);
            socketBuilder.options.set(SO_SNDBUF, socketBufferSize);
            socketBuilder.options.set(SO_RCVBUF, socketBufferSize);
            socketBuilder.reader = new RpcReader(false);
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

    private class LoadGeneratorThread extends Thread {
        private final byte[] payload;

        private LoadGeneratorThread(int payloadSize) {
            this.payload = new byte[payloadSize];
        }

        @Override
        public void run() {
            long endMs = System.currentTimeMillis() + durationMillis;
            Random random = new Random();

            List<CompletableFuture> futureList = new ArrayList<>();
            while (System.currentTimeMillis() < endMs) {
                int c = randomizeConcurrency ? random.nextInt(concurrency) : concurrency;
                for (int k = 0; k < c; k++) {
                    // todo: pack random level of concurrnecy; issue n concurrent calls and then wait for completion

                    IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);

                    long callId = callIdGenerator.incrementAndGet();
                    CompletableFuture future = new CompletableFuture();
                    futureList.add(future);
                    futures.put(callId, future);

                    buf.writeInt(payload.length);
                    buf.writeLong(callId);
                    buf.writeBytes(payload);
                    buf.flip();

                    AsyncSocket socket = socketList.get(random.nextInt(socketList.size()));
                    if (!socket.writeAndFlush(buf)) {
                        throw new RuntimeException();
                    }
                }

                for (CompletableFuture future : futureList) {
                    future.join();
                }
                futureList.clear();
            }
        }
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
}
