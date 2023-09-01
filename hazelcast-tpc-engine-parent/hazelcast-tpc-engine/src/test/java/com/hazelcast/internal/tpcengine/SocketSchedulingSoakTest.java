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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.util.BufferUtil;
import org.jctools.util.PaddedAtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.FormatUtil.humanReadableByteCountSI;
import static com.hazelcast.internal.tpcengine.FormatUtil.humanReadableCountSI;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertJoinable;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The purpose of this test is to ensure that scheduling farious forms
 * of scheduling like socket scheduling and deadline task scheduling
 * doesn't run into problems when running for an extended period.
 */
public abstract class SocketSchedulingSoakTest {
    private static final int SIZEOF_HEADER = SIZEOF_INT + SIZEOF_LONG;

    // The public properties are the tunnables for this soak test.
    public long runtimeSeconds = 5;
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public int socketBufferSize = 16 * 1024;
    // the number of concurrent RPC per load generator thread
    public int concurrency = 10;
    public boolean randomizeConcurrency = true;
    public int payloadSize = 100;
    public boolean randomizePayloadSize = true;
    // total number of sockets.
    public int socketCount = 200;
    // total number of reactors
    public int reactorCount = 4;
    public boolean useWriter = false;
    public int loadGeneratorThreadCount = 20;
    public long testTimeoutMs = ASSERT_TRUE_EVENTUALLY_TIMEOUT;
    // the number of scheduled tasks per reactor
    public int scheduledTaskCount = 50;
    public long scheduledTaskRateUs = MILLISECONDS.toMillis(5);

    private final AtomicLong callIdGenerator = new AtomicLong();
    private final List<PaddedAtomicLong> scheduledTaskCounters = new ArrayList<>();
    private final List<PaddedAtomicLong> rpcCounters = new ArrayList<>();
    private final MonitorThread monitorThread = new MonitorThread();
    private final List<Reactor> reactorList = new ArrayList<>();
    private final List<AsyncServerSocket> serverSocketList = new ArrayList<>();
    private final List<AsyncSocket> socketList = new ArrayList<>();
    private final ConcurrentMap<Long, CompletableFuture<IOBuffer>> futures = new ConcurrentHashMap<>();
    private boolean stop;

    public abstract Reactor.Builder newReactorBuilder();

    @Before
    public void before() {
        for (int k = 0; k < reactorCount; k++) {
            Reactor reactor = newReactorBuilder().build();
            reactorList.add(reactor);
            reactor.start();
        }

        // open a server socket for every reactor
        for (int k = 0; k < reactorCount; k++) {
            Reactor reactor = reactorList.get(k);
            AsyncServerSocket serverSocket = startServerSocket(reactor);
            serverSocketList.add(serverSocket);
        }

        Random random = new Random();
        // and open sockets to the server sockets.
        for (int k = 0; k < socketCount; k++) {
            AsyncServerSocket serverSocket = serverSocketList.get(k % reactorCount);
            Reactor reactor = reactorList.get(random.nextInt(reactorCount));
            AsyncSocket socket = startAndConnectSocket(reactor, serverSocket.getLocalAddress());
            socketList.add(socket);
        }
    }

    @After
    public void after() throws InterruptedException {
        terminateAll(reactorList);
    }

    @Test
    public void test() throws Exception {
        for (Reactor reactor : reactorList) {
            for (int k = 0; k < scheduledTaskCount; k++) {
                PaddedAtomicLong scheduledTaskCounter = new PaddedAtomicLong();
                scheduledTaskCounters.add(scheduledTaskCounter);

                reactor.offer(new Runnable() {
                    @Override
                    public void run() {
                        DeadlineScheduler deadlineScheduler = reactor.eventloop().deadlineScheduler();
                        deadlineScheduler.scheduleWithFixedDelay(
                                new DeadlineCallable(scheduledTaskCounter),
                                0, scheduledTaskRateUs, MICROSECONDS, reactor.defaultTaskQueue());
                    }
                });
            }
        }

        LoadGeneratorThread[] threads = new LoadGeneratorThread[loadGeneratorThreadCount];
        for (int k = 0; k < loadGeneratorThreadCount; k++) {
            PaddedAtomicLong rpcCounter = new PaddedAtomicLong();
            rpcCounters.add(rpcCounter);
            LoadGeneratorThread thread = new LoadGeneratorThread(payloadSize, rpcCounter);
            threads[k] = thread;
            thread.start();
        }
        monitorThread.start();
        monitorThread.join();
        assertJoinable(testTimeoutMs, threads);
    }

    private class DeadlineCallable implements Callable<Boolean> {
        private final PaddedAtomicLong counter;

        private DeadlineCallable(PaddedAtomicLong counter) {
            this.counter = counter;
        }

        @Override
        public Boolean call() throws Exception {
            counter.incrementAndGet();
            return true;
        }
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
        private final PaddedAtomicLong rpcCounter;

        private LoadGeneratorThread(int payloadSize, PaddedAtomicLong rpcCounter) {
            this.payload = new byte[payloadSize];
            this.rpcCounter = rpcCounter;
        }

        @Override
        public void run() {
            Random random = new Random();

            List<CompletableFuture> futureList = new ArrayList<>();
            while (!stop) {
                int c = randomizeConcurrency ? random.nextInt(concurrency) : concurrency;
                for (int k = 0; k < c; k++) {
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
                    rpcCounter.incrementAndGet();
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
            Random random = new Random();

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
                    CompletableFuture<IOBuffer> future = futures.remove(callId);
                    if (future == null) {
                        throw new IllegalStateException("Can't find future for callId:" + callId);
                    }
                    future.complete(response);
                } else {
                    boolean offered = random.nextBoolean()
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

    private class MonitorThread extends Thread {
        private final StringBuffer sb = new StringBuffer();

        @Override
        public void run() {
            try {
                run0();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stop = true;
        }

        private void run0() throws InterruptedException {
            long runtimeMs = SECONDS.toMillis(runtimeSeconds);
            long startMs = currentTimeMillis();
            long endMs = startMs + runtimeMs;
            long lastMs = startMs;
            Metrics lastMetrics = new Metrics();
            Metrics metrics = new Metrics();

            while (currentTimeMillis() < endMs) {
                Thread.sleep(SECONDS.toMillis(1));
                long nowMs = currentTimeMillis();
                long durationMs = nowMs - lastMs;

                collect(metrics);

                printEtd(nowMs, startMs, runtimeMs);

                printEta(endMs, nowMs);

                printRpcThp(metrics, lastMetrics, durationMs);

                printScheduledTaskThp(metrics, lastMetrics, durationMs);

                printReads(metrics, lastMetrics, durationMs);

                printBytesRead(metrics, lastMetrics, durationMs);

                printWrites(metrics, lastMetrics, durationMs);

                printBytesWritten(metrics, lastMetrics, durationMs);

                System.out.println(sb);
                sb.setLength(0);

                Metrics tmp = lastMetrics;
                lastMetrics = metrics;
                metrics = tmp;
                lastMs = nowMs;
            }
        }

        private void printRpcThp(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long diff = metrics.rpcCount - lastMetrics.rpcCount;
            double thp = ((diff) * 1000d) / durationMs;
            sb.append("[rpc-thp=");
            sb.append(humanReadableCountSI(thp));
            sb.append("/s]");
        }

        private void printScheduledTaskThp(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long diff = metrics.scheduledTaskCount - lastMetrics.scheduledTaskCount;
            double thp = ((diff) * 1000d) / durationMs;
            sb.append("[deadline-thp=");
            sb.append(humanReadableCountSI(thp));
            sb.append("/s]");
        }

        private void printEta(long endMs, long nowMs) {
            long eta = MILLISECONDS.toSeconds(endMs - nowMs);
            sb.append("[eta ");
            sb.append(eta / 60);
            sb.append("m:");
            sb.append(eta % 60);
            sb.append("s]");
        }

        private void printEtd(long nowMs, long startMs, long runtimeMs) {
            long completedSeconds = MILLISECONDS.toSeconds(nowMs - startMs);
            double completed = (100f * completedSeconds) / runtimeSeconds;
            sb.append("[etd ");
            sb.append(completedSeconds / 60);
            sb.append("m:");
            sb.append(completedSeconds % 60);
            sb.append("s ");
            sb.append(String.format("%,.3f", completed));
            sb.append("%]");
        }

        private void printBytesWritten(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long bytesWritten = metrics.bytesWritten;
            double bytesWrittehThp = ((bytesWritten - lastMetrics.bytesWritten) * 1000d) / durationMs;
            sb.append("[write-bytes=");
            sb.append(humanReadableByteCountSI(bytesWrittehThp));
            sb.append("/s]");
        }

        private void printWrites(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long writes = metrics.writes;
            double writesThp = ((writes - lastMetrics.writes) * 1000d) / durationMs;
            sb.append("[writes=");
            sb.append(humanReadableCountSI(writesThp));
            sb.append("/s]");
        }

        private void printBytesRead(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long bytesRead = metrics.bytesRead;
            double bytesReadThp = ((bytesRead - lastMetrics.bytesRead) * 1000d) / durationMs;
            sb.append("[read-bytes=");
            sb.append(humanReadableByteCountSI(bytesReadThp));
            sb.append("/s]");
        }

        private void printReads(Metrics metrics, Metrics lastMetrics, long durationMs) {
            long reads = metrics.reads;
            double readsThp = ((reads - lastMetrics.reads) * 1000d) / durationMs;
            sb.append("[reads=");
            sb.append(humanReadableCountSI(readsThp));
            sb.append("/s]");
        }
    }


    private static long sum(List<PaddedAtomicLong> list) {
        long sum = 0;
        for (PaddedAtomicLong a : list) {
            sum += a.get();
        }
        return sum;
    }

    private void collect(Metrics target) {
        target.clear();

        target.rpcCount = sum(rpcCounters);
        target.scheduledTaskCount = sum(scheduledTaskCounters);

        for (Reactor reactor : reactorList) {
            reactor.sockets().foreach(s -> {
                AsyncSocket.Metrics metrics = s.metrics();
                target.reads += metrics.reads();
                target.writes += metrics.writes();
                target.bytesWritten += metrics.bytesWritten();
                target.bytesRead += metrics.bytesRead();
            });
        }
    }

    private static class Metrics {
        private long scheduledTaskCount;
        private long reads;
        private long writes;
        private long bytesRead;
        private long bytesWritten;
        private long rpcCount;

        private void clear() {
            reads = 0;
            writes = 0;
            bytesRead = 0;
            bytesWritten = 0;
            rpcCount = 0;
            scheduledTaskCount = 0;
        }
    }
}
