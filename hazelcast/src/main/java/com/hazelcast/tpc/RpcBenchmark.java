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

package com.hazelcast.tpc;


import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import com.hazelcast.tpc.engine.frame.SerialFrameAllocator;
import com.hazelcast.tpc.engine.nio.NioAsyncServerSocket;
import com.hazelcast.tpc.engine.nio.NioAsyncSocket;
import com.hazelcast.tpc.engine.nio.NioEventloop;
import com.hazelcast.tpc.engine.nio.NioReadHandler;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;

/**
 * A very trivial benchmark to measure the throughput we can get with a RPC system.
 *
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

        NioAsyncServerSocket socket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        NioAsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        long startMs = System.currentTimeMillis();

        for (int k = 0; k < concurrency; k++) {
            Frame frame = new Frame(128);
            frame.writeInt(-1);
            frame.writeLong(requestTotal / concurrency);
            frame.writeComplete();
            clientSocket.write(frame);
        }
        clientSocket.flush();

        latch.await();

        long duration = System.currentTimeMillis() - startMs;
        float throughput = requestTotal * 1000f / duration;
        System.out.println("Throughput:" + throughput);
        System.exit(0);
    }

    @NotNull
    private static NioAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        NioEventloop.NioConfiguration config = new NioEventloop.NioConfiguration();
        if (clientCpu >= 0) {
            config.setThreadAffinity(new ThreadAffinity("" + clientCpu));
        }
        config.setSpin(spin);
        NioEventloop clientEventLoop = new NioEventloop(config);
        clientEventLoop.start();

        NioAsyncSocket clientSocket = NioAsyncSocket.open();
        clientSocket.setTcpNoDelay(true);
        clientSocket.setReadHandler(new NioReadHandler() {
            private final FrameAllocator responseAllocator = new SerialFrameAllocator(8, true);

            @Override
            public void onRead(ByteBuffer buffer) {
                for (; ; ) {
                    if (buffer.remaining() < BYTES_INT + BYTES_LONG) {
                        return;
                    }

                    int size = buffer.getInt();
                    long l = buffer.getLong();
                    if (l == 0) {
                        latch.countDown();
                    } else {
                        Frame frame = responseAllocator.allocate(8);
                        frame.writeInt(-1);
                        frame.writeLong(l);
                        frame.writeComplete();
                        asyncSocket.unsafeWriteAndFlush(frame);
                    }
                }
            }
        });
        clientSocket.activate(clientEventLoop);
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private static NioAsyncServerSocket newServer(SocketAddress serverAddress) {
        NioEventloop.NioConfiguration config = new NioEventloop.NioConfiguration();
        config.setSpin(spin);
        if (serverCpu >= 0) {
            config.setThreadAffinity(new ThreadAffinity("" + serverCpu));
        }
        NioEventloop serverEventloop = new NioEventloop(config);
        serverEventloop.start();

        NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(serverEventloop);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            socket.setTcpNoDelay(true);
            socket.setReadHandler(new NioReadHandler() {
                private final FrameAllocator responseAllocator = new SerialFrameAllocator(8, true);

                @Override
                public void onRead(ByteBuffer buffer) {
                    for (; ; ) {
                        if (buffer.remaining() < BYTES_INT + BYTES_LONG) {
                            return;
                        }
                        int size = buffer.getInt();
                        long l = buffer.getLong();

                        Frame frame = responseAllocator.allocate(8);
                        frame.writeInt(-1);
                        frame.writeLong(l - 1);
                        frame.writeComplete();
                        asyncSocket.unsafeWriteAndFlush(frame);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
