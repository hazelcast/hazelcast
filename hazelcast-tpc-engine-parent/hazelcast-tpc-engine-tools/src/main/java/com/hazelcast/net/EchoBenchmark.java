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

package com.hazelcast.net;

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketBuilder;
import com.hazelcast.internal.tpcengine.net.AsyncSocketMetrics;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.internal.tpcengine.nio.NioAsyncSocketBuilder;
import com.hazelcast.internal.util.ThreadAffinity;
import org.jctools.util.PaddedAtomicLong;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.FormatUtil.humanReadableByteCountSI;
import static com.hazelcast.FormatUtil.humanReadableCountSI;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_REUSEPORT;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.put;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A benchmarks that test the throughput of 2 sockets that are bouncing packets
 * with some payload between them.
 * <p>
 * Make sure you add the following the JVM options, otherwise the selector will create
 * garbage:
 * <p>
 * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
 *
 * <p>
 * for IO_Uring read:
 * https://github.com/frevib/io_uring-echo-server/issues/8?spm=a2c65.11461447.0.0.27707555CrwLfj
 * and check out the IORING_FEAT_FAST_POLL comment
 * <p>
 * Good read:
 * https://www.alibabacloud.com/blog/599544
 */
public class EchoBenchmark {
    // Properties of the benchmark
    public int port = 5006;
    public int runtimeSeconds = 2000;
    public int socketBufferSize = 256 * 1024;
    public boolean useDirectByteBuffers = true;
    public int payloadSize = 10;
    // the number of concurrent request send over a single connection.
    public int concurrency = 1;
    // the total number of connections.
    public int connections = 2;
    public boolean tcpNoDelay = true;
    public boolean spin = false;
    public boolean regularSchedule = true;
    public ReactorType reactorType = ReactorType.NIO;
    public String cpuAffinityClient = "1,2";
    public String cpuAffinityServer = "5,6";
    public int clientReactorCount = 2;
    public int serverReactorCount = 2;

    // private to the benchmark
    private volatile boolean stop;
    private PaddedAtomicLong[] echoCounters;
    private List<Reactor> clientReactors;
    private List<Reactor> serverReactors;
    private List<Reactor> reactors = new ArrayList<>();
    private List<AsyncServerSocket> serverSockets;

    public static void main(String[] args) throws InterruptedException {
        EchoBenchmark benchmark = new EchoBenchmark();
        benchmark.run();
    }

    public void run() throws InterruptedException {
        printConfig();

        echoCounters = new PaddedAtomicLong[clientReactorCount];
        for (int k = 0; k < echoCounters.length; k++) {
            echoCounters[k] = new PaddedAtomicLong();
        }

        clientReactors = newClientReactors();
        serverReactors = newServerReactors();
        reactors.addAll(clientReactors);
        reactors.addAll(serverReactors);

        serverSockets = openServerSockets();

        List<AsyncSocket> clientSockets = openClientSockets();

        long start = currentTimeMillis();

        for (int connectionIndex = 0; connectionIndex < connections; connectionIndex++) {
            AsyncSocket clientSocket = clientSockets.get(connectionIndex);
            for (int k = 0; k < concurrency; k++) {
                // write the payload size (int), the number of iterations (long) and the payload (byte[]
                byte[] payload = new byte[payloadSize];
                IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);
                buf.writeInt(payload.length);
                buf.writeLong(Long.MAX_VALUE);
                buf.writeBytes(payload);
                buf.flip();
                if (!clientSocket.write(buf)) {
                    throw new RuntimeException("Failed to write a buffer to the clientSocket");
                }
            }
        }

        for (int i = 0; i < connections; i++) {
            clientSockets.get(i).flush();
        }

        MonitorThread monitor = new MonitorThread();
        monitor.start();
        monitor.join();

        terminateAll(serverReactors);
        terminateAll(clientReactors);

        printResults(start);

        System.exit(0);
    }

    private void printResults(long startMillis) {
        long count = sum(echoCounters);
        long duration = currentTimeMillis() - startMillis;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (count * 1000f / duration) + " echo/second");
    }

    private void printConfig() {
        System.out.println("ReactorType:" + reactorType);
        System.out.println("port:" + port);
        System.out.println("runtimeSeconds:" + runtimeSeconds);
        System.out.println("socketBufferSize:" + socketBufferSize);
        System.out.println("useDirectByteBuffers:" + useDirectByteBuffers);
        System.out.println("payloadSize:" + payloadSize);
        System.out.println("concurrency:" + concurrency);
        System.out.println("tcpNoDelay:" + tcpNoDelay);
        System.out.println("spin:" + spin);
        System.out.println("regularSchedule:" + regularSchedule);
        System.out.println("cpuAffinityClient:" + cpuAffinityClient);
        System.out.println("cpuAffinityServer:" + cpuAffinityServer);
        System.out.println("connections:" + connections);
        System.out.println("clientReactorCount:" + clientReactorCount);
        System.out.println("serverReactorCount:" + serverReactorCount);
    }

    private List<Reactor> newServerReactors() {
        List<Reactor> reactors = new ArrayList<>();
        ThreadAffinity affinity = cpuAffinityServer == null ? null : new ThreadAffinity(cpuAffinityServer);
        for (int k = 0; k < serverReactorCount; k++) {
            ReactorBuilder builder = ReactorBuilder.newReactorBuilder(reactorType);
            builder.setSpin(spin);
            builder.setThreadName("ServerReactor-" + k);
            builder.setThreadAffinity(affinity);
            Reactor reactor = builder.build();
            reactors.add(reactor);
            reactor.start();
        }
        return reactors;
    }

    private List<Reactor> newClientReactors() {
        List<Reactor> reactors = new ArrayList<>();
        ThreadAffinity affinity = cpuAffinityClient == null ? null : new ThreadAffinity(cpuAffinityClient);
        for (int k = 0; k < clientReactorCount; k++) {
            ReactorBuilder builder = ReactorBuilder.newReactorBuilder(reactorType);
            builder.setSpin(spin);
            builder.setThreadName("ClientReactor-" + k);
            builder.setThreadAffinity(affinity);
            Reactor reactor = builder.build();
            reactors.add(reactor);
            reactor.start();
        }
        return reactors;
    }

    private static long sum(PaddedAtomicLong[] array) {
        long sum = 0;
        for (PaddedAtomicLong c : array) {
            sum += c.get();
        }
        return sum;
    }

    private List<AsyncSocket> openClientSockets() {
        List<AsyncSocket> sockets = new ArrayList<>();
        for (int k = 0; k < connections; k++) {
            Reactor clientReactor = clientReactors.get(k % clientReactorCount);
            System.out.println("clientReactor:"+clientReactor);

            PaddedAtomicLong echoCounter = echoCounters[k % clientReactorCount];
            AsyncServerSocket serverSocket = serverSockets.get(k % serverReactorCount);
            System.out.println("serverSocket:"+serverSocket);

            AsyncSocketBuilder socketBuilder = clientReactor.newAsyncSocketBuilder()
                    .set(TCP_NODELAY, tcpNoDelay)
                    .set(SO_SNDBUF, socketBufferSize)
                    .set(SO_RCVBUF, socketBufferSize)
                    .setReader(new ClientAsyncSocketReader(echoCounter));

            if (socketBuilder instanceof NioAsyncSocketBuilder) {
                NioAsyncSocketBuilder nioSocketBuilder = (NioAsyncSocketBuilder) socketBuilder;
                nioSocketBuilder.setReceiveBufferIsDirect(useDirectByteBuffers);
                nioSocketBuilder.setRegularSchedule(regularSchedule);
            }

            AsyncSocket clientSocket = socketBuilder.build();
            clientSocket.start();
            clientSocket.connect(serverSocket.getLocalAddress()).join();
            sockets.add(clientSocket);
        }
        return sockets;
    }

    private List<AsyncServerSocket> openServerSockets() {
        List<AsyncServerSocket> serverSockets = new ArrayList<>();
        for (int k = 0; k < serverReactors.size(); k++) {
            Reactor serverReactor = serverReactors.get(k);
            SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", port + k);

            AsyncServerSocket serverSocket = serverReactor.newAsyncServerSocketBuilder()
                    .set(SO_RCVBUF, socketBufferSize)
                    .set(SO_REUSEPORT, true)
                    .setAcceptFn(acceptRequest -> {
                        AsyncSocketBuilder socketBuilder = serverReactor.newAsyncSocketBuilder(acceptRequest)
                                .set(TCP_NODELAY, tcpNoDelay)
                                .set(SO_RCVBUF, socketBufferSize)
                                .set(SO_SNDBUF, socketBufferSize)
                                .setReader(new ServerAsyncSocketReader());

                        if (socketBuilder instanceof NioAsyncSocketBuilder) {
                            NioAsyncSocketBuilder nioSocketBuilder = (NioAsyncSocketBuilder) socketBuilder;
                            nioSocketBuilder.setReceiveBufferIsDirect(useDirectByteBuffers);
                            nioSocketBuilder.setRegularSchedule(regularSchedule);
                        }
                        AsyncSocket socket = socketBuilder.build();
                        socket.start();
                    }).build();

            serverSocket.bind(serverAddress);
            serverSocket.start();
            serverSockets.add(serverSocket);
        }
        return serverSockets;
    }

    // Future improvement; ideally the server would just take the read buffer and then
    // write it. But currently this isn't possible you have no control on the buffer that
    // is being used for reading.
    private class ServerAsyncSocketReader extends AsyncSocketReader {
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize = -1;
        private final IOBufferAllocator responseAllocator
                = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);

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
                    if (payloadBuffer == null) {
                        payloadBuffer = ByteBuffer.allocateDirect(payloadSize);
                    } else {
                        payloadBuffer.clear();
                    }
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
        private final PaddedAtomicLong echoCounter;
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize;
        private final IOBufferAllocator responseAllocator;

        public ClientAsyncSocketReader(PaddedAtomicLong echoCounter) {
            this.echoCounter = echoCounter;
            this.payloadSize = -1;
            this.responseAllocator = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);
        }

        @Override
        public void onRead(ByteBuffer src) {
            if (stop) {
                return;
            }

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

                    // todo: it could be that the payloadBuffer isn't big enough (check server as well).
                    if (payloadBuffer == null) {
                        payloadBuffer = ByteBuffer.allocateDirect(payloadSize);
                    } else {
                        payloadBuffer.clear();
                    }
                }

                put(payloadBuffer, src);

                if (payloadBuffer.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }
                payloadBuffer.flip();

                echoCounter.lazySet(echoCounter.get() + 1);

                IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                responseBuf.writeInt(payloadSize);
                responseBuf.writeLong(round);
                //todo: another copy is made.
                responseBuf.write(payloadBuffer);
                responseBuf.flip();
                if (!socket.unsafeWriteAndFlush(responseBuf)) {
                    throw new RuntimeException("Socket has no space");
                }

                payloadSize = -1;
            }
        }
    }

    private class MonitorThread extends Thread {
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
            long end = currentTimeMillis() + SECONDS.toMillis(runtimeSeconds);
            Metrics lastMetrics = new Metrics();
            Metrics metrics = new Metrics();
            long lastMs = currentTimeMillis();
            StringBuffer sb = new StringBuffer();
            while (currentTimeMillis() < end) {
                Thread.sleep(SECONDS.toMillis(1));
                long nowMs = currentTimeMillis();

                collect(metrics);

                long diff = metrics.echos - lastMetrics.echos;
                long durationMs = nowMs - lastMs;
                double echoThp = ((diff) * 1000d) / durationMs;
                sb.append("   echo=");
                sb.append(humanReadableCountSI(echoThp));
                sb.append("/s");

                long reads = metrics.reads;
                double readsThp = ((reads - lastMetrics.reads) * 1000d) / durationMs;
                sb.append(" reads=");
                sb.append(humanReadableCountSI(readsThp));
                sb.append("/s");

                long bytesRead = metrics.bytesRead;
                double bytesReadThp = ((bytesRead - lastMetrics.bytesRead) * 1000d) / durationMs;
                sb.append(" read-bytes=");
                sb.append(humanReadableByteCountSI(bytesReadThp));
                sb.append("/s");

                long writes = metrics.writes;
                double writesThp = ((writes - lastMetrics.writes) * 1000d) / durationMs;
                sb.append(" writes=");
                sb.append(humanReadableCountSI(writesThp));
                sb.append("/s");

                long bytesWritten = metrics.bytesWritten;
                double bytesWrittehThp = ((bytesWritten - lastMetrics.bytesWritten) * 1000d) / durationMs;
                sb.append(" write-bytes=");
                sb.append(humanReadableByteCountSI(bytesWrittehThp));
                sb.append("/s");
                System.out.println(sb);
                sb.setLength(0);

                Metrics tmp = lastMetrics;
                lastMetrics = metrics;
                metrics = tmp;
                lastMs = nowMs;
            }
        }
    }

    private void collect(Metrics target) {
        target.clear();

        target.echos = sum(echoCounters);

        for (Reactor reactor : reactors) {
            reactor.sockets().foreach(s -> {
                AsyncSocketMetrics metrics = s.metrics();
                target.reads += metrics.reads();
                target.writes += metrics.writes();
                target.bytesWritten += metrics.bytesWritten();
                target.bytesRead += metrics.bytesRead();
            });
        }
    }

    private static class Metrics {
        private long reads;
        private long writes;
        private long bytesRead;
        private long bytesWritten;
        private long echos;

        private void clear() {
            reads = 0;
            writes = 0;
            bytesRead = 0;
            bytesWritten = 0;
            echos = 0;
        }
    }
}
