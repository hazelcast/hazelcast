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

package com.hazelcast.echo;

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iouring.IOUringReactorBuilder;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketBuilder;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.internal.tpcengine.nio.NioAsyncSocketBuilder;
import com.hazelcast.internal.tpcengine.nio.NioReactorBuilder;
import com.hazelcast.internal.util.ThreadAffinity;
import org.jctools.util.PaddedAtomicLong;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_REUSEPORT;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.put;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A benchmarks that test the throughput of 2 sockets that are bouncing packets
 * with some payload between them.
 *
 * Make sure you add the following the JVM options, otherwise the selector will create
 * garbage:
 *
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
public class EchoBenchmark_Tpc {
    public static final int port = 5006;
    public static final int durationSeconds = 600;
    public static final int socketBufferSize = 128 * 1024;
    public static final boolean useDirectByteBuffers = true;
    public static final int payloadSize = 40;
    public static final int concurrency = 100;
    public static final boolean tcpNoDelay = true;
    public static final boolean spin = false;
    public static final boolean regularSchedule = true;
    public static final ReactorType reactorType = ReactorType.IOURING;
    public static final String cpuAffinityClient = "1";
    public static final String cpuAffinityServer = "4";
    public static final boolean registerRingFd = false;
    public static final int connections = 1;
    public static volatile boolean stop;

    public static void main(String[] args) throws InterruptedException {
        PaddedAtomicLong[] completedArray = new PaddedAtomicLong[connections];
        for (int k = 0; k < completedArray.length; k++) {
            completedArray[k] = new PaddedAtomicLong();
        }

        ReactorBuilder clientReactorBuilder = newReactorBuilder();
        if (clientReactorBuilder instanceof IOUringReactorBuilder) {
            IOUringReactorBuilder b = (IOUringReactorBuilder) clientReactorBuilder;
            b.setRegisterRingFd(registerRingFd);
        }
        clientReactorBuilder.setSpin(spin);
        clientReactorBuilder.setThreadNameSupplier(() -> "Client-Thread");
        clientReactorBuilder.setThreadAffinity(cpuAffinityClient == null ? null : new ThreadAffinity(cpuAffinityClient));
        Reactor clientReactor = clientReactorBuilder.build();
        clientReactor.start();

        ReactorBuilder serverReactorBuilder = newReactorBuilder();
        if (serverReactorBuilder instanceof IOUringReactorBuilder) {
            IOUringReactorBuilder b = (IOUringReactorBuilder) serverReactorBuilder;
            b.setRegisterRingFd(registerRingFd);
        }
        serverReactorBuilder.setSpin(spin);
        serverReactorBuilder.setThreadNameSupplier(() -> "Server-Thread");
        serverReactorBuilder.setThreadAffinity(cpuAffinityServer == null ? null : new ThreadAffinity(cpuAffinityServer));
        Reactor serverReactor = serverReactorBuilder.build();
        serverReactor.start();

        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", port);

        AsyncServerSocket serverSocket = newServer(serverReactor, serverAddress);

        CountDownLatch completionLatch = new CountDownLatch(concurrency * connections);

        AsyncSocket[] clientSockets = new AsyncSocket[connections];
        for (int k = 0; k < clientSockets.length; k++) {
            clientSockets[k] = newClient(clientReactor, serverAddress, completionLatch, completedArray[k]);
        }

        long start = System.currentTimeMillis();

        for (int k = 0; k < concurrency; k++) {
            for (int i = 0; i < connections; i++) {
                AsyncSocket clientSocket = clientSockets[i];
                // write the payload size (int), the number of iterations (long) and the payload (byte[]
                byte[] payload = new byte[payloadSize];
                IOBuffer buf = new IOBuffer(SIZEOF_INT + SIZEOF_LONG + payload.length, true);
                buf.writeInt(payload.length);
                buf.writeLong(Long.MAX_VALUE);
                buf.writeBytes(payload);
                buf.flip();
                if (!clientSocket.write(buf)) {
                    throw new RuntimeException();
                }
            }
        }
        for (int i = 0; i < connections; i++) {
            clientSockets[i].flush();
        }

        Monitor monitor = new Monitor(durationSeconds, completedArray);
        monitor.start();
        completionLatch.await();

        long count = sum(completedArray);

        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (count * 1000f / duration) + " echo/second");

        for (int i = 0; i < connections; i++) {
            clientSockets[i].close();
        }
        serverSocket.close();

        System.exit(0);
    }

    private static long sum(PaddedAtomicLong[] array) {
        long sum = 0;
        for (PaddedAtomicLong c : array) {
            sum += c.get();
        }
        return sum;
    }

    @NotNull
    private static ReactorBuilder newReactorBuilder() {
        if (reactorType == ReactorType.NIO) {
            return new NioReactorBuilder();
        } else {
            return new IOUringReactorBuilder();
        }
    }

    private static AsyncSocket newClient(Reactor clientReactor,
                                         SocketAddress serverAddress,
                                         CountDownLatch latch,
                                         PaddedAtomicLong completed) {
        AsyncSocketBuilder socketBuilder = clientReactor.newAsyncSocketBuilder()
                .set(TCP_NODELAY, tcpNoDelay)
                .set(SO_SNDBUF, socketBufferSize)
                .set(SO_RCVBUF, socketBufferSize)
                .setReader(new ClientAsyncSocketReader(latch, completed));

        if (socketBuilder instanceof NioAsyncSocketBuilder) {
            NioAsyncSocketBuilder nioSocketBuilder = (NioAsyncSocketBuilder) socketBuilder;
            nioSocketBuilder.setReceiveBufferIsDirect(useDirectByteBuffers);
            nioSocketBuilder.setRegularSchedule(regularSchedule);
        }

        AsyncSocket clientSocket = socketBuilder.build();
        clientSocket.start();
        clientSocket.connect(serverAddress).join();

        return clientSocket;
    }

    private static AsyncServerSocket newServer(Reactor serverReactor, SocketAddress serverAddress) {
        AsyncServerSocket serverSocket = serverReactor.newAsyncServerSocketBuilder()
                .set(SO_RCVBUF, socketBufferSize)
                .set(SO_REUSEPORT, true)
                .setAcceptConsumer(acceptRequest -> {
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
        return serverSocket;
    }

    private static class ServerAsyncSocketReader extends AsyncSocketReader {
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize = -1;
        private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);

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
                        payloadBuffer = ByteBuffer.allocate(payloadSize);
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

    private static class ClientAsyncSocketReader extends AsyncSocketReader {
        private final CountDownLatch latch;
        private final PaddedAtomicLong completed;
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize;
        private final IOBufferAllocator responseAllocator;

        public ClientAsyncSocketReader(CountDownLatch latch, PaddedAtomicLong completed) {
            this.latch = latch;
            this.payloadSize = -1;
            this.responseAllocator = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);
            this.completed = completed;
        }

        @Override
        public void onRead(ByteBuffer src) {
            if (stop) {
                latch.countDown();
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
                    if (payloadBuffer == null) {
                        payloadBuffer = ByteBuffer.allocate(payloadSize);
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

                completed.lazySet(completed.get() + 1);

                IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                responseBuf.writeInt(payloadSize);
                responseBuf.writeLong(round);
                responseBuf.write(payloadBuffer);
                responseBuf.flip();
                if (!socket.unsafeWriteAndFlush(responseBuf)) {
                    throw new RuntimeException("Socket has no space");
                }

                payloadSize = -1;
            }
        }
    }

    private static class Monitor extends Thread {
        private final int durationSecond;
        private final PaddedAtomicLong[] completedArray;
        private long last = 0;

        public Monitor(int durationSecond, PaddedAtomicLong[] completedArray) {
            this.durationSecond = durationSecond;
            this.completedArray = completedArray;
        }

        @Override
        public void run() {
            long end = System.currentTimeMillis() + SECONDS.toMillis(durationSecond);
            while (System.currentTimeMillis() < end) {
                try {
                    Thread.sleep(SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                }

                long total = sum(completedArray);
                long diff = total - last;
                last = total;
                System.out.println("  thp " + diff + " echo/sec");
            }

            stop = true;
        }
    }
}
