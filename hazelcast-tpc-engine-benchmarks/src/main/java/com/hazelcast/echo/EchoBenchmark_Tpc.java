/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.AsyncSocketBuilder;
import com.hazelcast.internal.tpc.AsyncSocketOptions;
import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.ReactorBuilder;
import com.hazelcast.internal.tpc.ReactorType;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.iouring.IOUringReactorBuilder;
import com.hazelcast.internal.tpc.nio.NioAsyncSocketBuilder;
import com.hazelcast.internal.tpc.nio.NioReactorBuilder;
import com.hazelcast.internal.util.ThreadAffinity;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_REUSEPORT;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.BufferUtil.put;

/**
 * A benchmarks that test the throughput of 2 sockets that are bouncing packets
 * with some payload between them.
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
    // use small buffers to cause a lot of network scheduling overhead (and shake down problems)
    public static final int socketBufferSize = 128 * 1024;
    public static final boolean useDirectByteBuffers = true;
    public static final long iterations = 4_000_000L;
    public static final int payloadSize = 1000;
    public static final int concurrency = 1;
    public static final boolean tcpNoDelay = true;
    public static final boolean spin = false;
    public static final boolean regularSchedule = true;
    public static final ReactorType reactorType = ReactorType.IOURING;
    public static final String cpuAffinityClient = "2";
    public static final String cpuAffinityServer = "9";
    public static final boolean registerRingFd = false;

    public static void main(String[] args) throws InterruptedException {
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

        CountDownLatch latch = new CountDownLatch(concurrency);

        AsyncSocket clientSocket = newClient(clientReactor, serverAddress, latch);

        long start = System.currentTimeMillis();

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

        latch.await();

        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (iterations * 1000 / duration) + " ops");

        clientSocket.close();
        serverSocket.close();

        System.exit(0);
    }

    @NotNull
    private static ReactorBuilder newReactorBuilder() {
        if (reactorType == ReactorType.NIO) {
            return new NioReactorBuilder();
        } else {
            return new IOUringReactorBuilder();
        }
    }

    private static AsyncSocket newClient(Reactor clientReactor, SocketAddress serverAddress, CountDownLatch latch) {
        AsyncSocketBuilder socketBuilder = clientReactor.newAsyncSocketBuilder()
                .set(TCP_NODELAY, tcpNoDelay)
                .set(SO_SNDBUF, socketBufferSize)
                .set(SO_RCVBUF, socketBufferSize)
                .setReadHandler(new ClientReadHandler(latch));

        if(socketBuilder instanceof NioAsyncSocketBuilder){
            NioAsyncSocketBuilder nioSocketBuilder = (NioAsyncSocketBuilder)socketBuilder;
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
                            .setReadHandler(new ServerReadHandler());

                    if(socketBuilder instanceof NioAsyncSocketBuilder){
                        NioAsyncSocketBuilder nioSocketBuilder = (NioAsyncSocketBuilder)socketBuilder;
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

    private static class ServerReadHandler extends ReadHandler {
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize = -1;
        private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);

        @Override
        public void onRead(ByteBuffer receiveBuffer) {
            for (; ; ) {
                if (payloadSize == -1) {
                    if (receiveBuffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        break;
                    }
                    payloadSize = receiveBuffer.getInt();
                    round = receiveBuffer.getLong();
                    if (round < 0) {
                        throw new RuntimeException("round can't be smaller than 0, found:" + round);
                    }
                    if (payloadBuffer == null) {
                        payloadBuffer = ByteBuffer.allocate(payloadSize);
                    } else {
                        payloadBuffer.clear();
                    }
                }

                put(payloadBuffer, receiveBuffer);
                if (payloadBuffer.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }

//                        if (round % 100 == 0) {
//                            System.out.println("server round:" + round);
//                        }

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

    private static class ClientReadHandler extends ReadHandler {
        private final CountDownLatch latch;
        private ByteBuffer payloadBuffer;
        private long round;
        private int payloadSize;
        private final IOBufferAllocator responseAllocator;

        public ClientReadHandler(CountDownLatch latch) {
            this.latch = latch;
            payloadSize = -1;
            responseAllocator = new NonConcurrentIOBufferAllocator(8, useDirectByteBuffers);
        }

        @Override
        public void onRead(ByteBuffer receiveBuffer) {
            for (; ; ) {
                if (payloadSize == -1) {
                    if (receiveBuffer.remaining() < SIZEOF_INT + SIZEOF_LONG) {
                        break;
                    }

                    payloadSize = receiveBuffer.getInt();
                    round = receiveBuffer.getLong();
                    if (round < 0) {
                        throw new RuntimeException("round can't be smaller than 0, found:" + round);
                    }
                    if (payloadBuffer == null) {
                        payloadBuffer = ByteBuffer.allocate(payloadSize);
                    } else {
                        payloadBuffer.clear();
                    }
                }

                put(payloadBuffer, receiveBuffer);

                if (payloadBuffer.remaining() > 0) {
                    // not all bytes have been received.
                    break;
                }
                payloadBuffer.flip();

                if (round % 1_000_000 == 0) {
                    System.out.println("client round:" + round);
                }

                if (round == 0) {
                    latch.countDown();
                } else {
                    IOBuffer responseBuf = responseAllocator.allocate(SIZEOF_INT + SIZEOF_LONG + payloadSize);
                    responseBuf.writeInt(payloadSize);
                    responseBuf.writeLong(round);
                    responseBuf.write(payloadBuffer);
                    responseBuf.flip();
                    if (!socket.unsafeWriteAndFlush(responseBuf)) {
                        throw new RuntimeException("Socket has no space");
                    }
                }
                payloadSize = -1;
            }
        }
    }
}
