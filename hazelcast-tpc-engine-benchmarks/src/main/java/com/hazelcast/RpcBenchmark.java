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

package com.hazelcast;


import com.hazelcast.internal.tpc.AcceptRequest;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.ReactorBuilder;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.nio.NioReactorBuilder;
import com.hazelcast.internal.tpc.util.BitUtil;
import com.hazelcast.internal.util.ThreadAffinity;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static com.hazelcast.Util.constructComplete;
import static com.hazelcast.internal.tpc.AsyncSocketOptions.TCP_NODELAY;

/**
 * A very trivial benchmark to measure the throughput we can get with a RPC system.
 * <p>
 * JMH would be better; but for now this will give some insights.
 */
public class RpcBenchmark {

    public static int serverCpu = -1;
    public static int clientCpu = -1;
    public static boolean spin = true;
    public static int requestTotal = 100 * 1000 * 1000;
    public static int concurrency = 2000;

    public static void main(String[] args) throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        AsyncServerSocket socket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        AsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        long startMs = System.currentTimeMillis();

        for (int k = 0; k < concurrency; k++) {
            IOBuffer buf = new IOBuffer(128);
            buf.writeInt(-1);
            buf.writeLong(requestTotal / concurrency);
            constructComplete(buf);
            clientSocket.write(buf);
        }
        clientSocket.flush();

        latch.await();

        long duration = System.currentTimeMillis() - startMs;
        float throughput = requestTotal * 1000f / duration;
        System.out.println("Throughput:" + throughput);
        System.exit(0);
    }

    private static AsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        ReactorBuilder reactorBuilder = new NioReactorBuilder();
        if (clientCpu >= 0) {
            reactorBuilder.setThreadAffinity(new ThreadAffinity("" + clientCpu));
        }
        reactorBuilder.setSpin(spin);
        Reactor clientReactor = reactorBuilder.build();
        clientReactor.start();

        AsyncSocket clientSocket = clientReactor.newAsyncSocketBuilder()
                .set(TCP_NODELAY, true)
                .setReadHandler(new ClientReadHandler(latch))
                .build();
        clientSocket.start();
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private static class ClientReadHandler extends ReadHandler {
        private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);
        private final CountDownLatch latch;

        public ClientReadHandler(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onRead(ByteBuffer receiveBuffer) {
            for (; ; ) {
                if (receiveBuffer.remaining() < BitUtil.SIZEOF_INT + BitUtil.SIZEOF_LONG) {
                    return;
                }

                int size = receiveBuffer.getInt();
                long l = receiveBuffer.getLong();
                if (l == 0) {
                    latch.countDown();
                } else {
                    IOBuffer buf = responseAllocator.allocate(8);
                    buf.writeInt(-1);
                    buf.writeLong(l);
                    constructComplete(buf);
                    socket.unsafeWriteAndFlush(buf);
                }
            }
        }
    }

    private static AsyncServerSocket newServer(SocketAddress serverAddress) {
        ReactorBuilder reactorBuilder = new NioReactorBuilder();
        reactorBuilder.setSpin(spin);
        if (serverCpu >= 0) {
            reactorBuilder.setThreadAffinity(new ThreadAffinity("" + serverCpu));
        }
        Reactor serverReactor = reactorBuilder.build();
        serverReactor.start();

        AsyncServerSocket serverSocket = serverReactor
                .newAsyncServerSocketBuilder()
                .setAcceptConsumer(new Consumer<AcceptRequest>() {
                    @Override
                    public void accept(AcceptRequest acceptRequest) {
                        AsyncSocket socket = serverReactor.newAsyncSocketBuilder(acceptRequest)
                                .setReadHandler(new ServerReadHandler())
                                .set(TCP_NODELAY, true)
                                .build();
                        socket.start();
                    }
                })
                .build();
        serverSocket.bind(serverAddress);
        serverReactor.start();

        return serverSocket;
    }

    private static class ServerReadHandler extends ReadHandler {
        private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

        @Override
        public void onRead(ByteBuffer receiveBuffer) {
            for (; ; ) {
                if (receiveBuffer.remaining() < BitUtil.SIZEOF_INT + BitUtil.SIZEOF_LONG) {
                    return;
                }
                int size = receiveBuffer.getInt();
                long l = receiveBuffer.getLong();

                IOBuffer buf = responseAllocator.allocate(8);
                buf.writeInt(-1);
                buf.writeLong(l - 1);
                constructComplete(buf);
                socket.unsafeWriteAndFlush(buf);
            }
        }
    }
}
