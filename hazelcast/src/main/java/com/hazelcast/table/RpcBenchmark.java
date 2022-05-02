package com.hazelcast.table;


import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.Scheduler;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import com.hazelcast.tpc.engine.frame.NonConcurrentPooledFrameAllocator;
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
    public static boolean spin = false;
    public static int requestTotal = 1000 * 1000 * 1000;
    public static int concurrency = 10000;

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
            frame.complete();
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
        NioEventloop clientEventLoop = new NioEventloop();
        clientEventLoop.setScheduler(new DummyScheduler());
        clientEventLoop.setSpin(spin);
        if (clientCpu >= 0) {
            clientEventLoop.setThreadAffinity(new ThreadAffinity("" + clientCpu));
        }
        clientEventLoop.start();

        NioAsyncSocket clientSocket = NioAsyncSocket.open();
        clientSocket.setTcpNoDelay(true);
        clientSocket.setReadHandler(new NioReadHandler() {
            private FrameAllocator responseFrameAllocator = new NonConcurrentPooledFrameAllocator(8, true);

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
                        Frame frame = responseFrameAllocator.allocate(8);
                        frame.writeInt(-1);
                        frame.writeLong(l);
                        frame.complete();
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
        NioEventloop serverEventloop = new NioEventloop();
        serverEventloop.setScheduler(new DummyScheduler());
        serverEventloop.setSpin(spin);
        if (serverCpu >= 0) {
            serverEventloop.setThreadAffinity(new ThreadAffinity("" + serverCpu));
        }
        serverEventloop.start();

        NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(serverEventloop);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            socket.setTcpNoDelay(true);
            socket.setReadHandler(new NioReadHandler() {
                private FrameAllocator responseFrameAllocator = new NonConcurrentPooledFrameAllocator(8, true);

                @Override
                public void onRead(ByteBuffer buffer) {
                    for (; ; ) {
                        if (buffer.remaining() < BYTES_INT + BYTES_LONG) {
                            return;
                        }
                        int size = buffer.getInt();
                        long l = buffer.getLong();

                        Frame frame = responseFrameAllocator.allocate(8);
                        frame.writeInt(-1);
                        frame.writeLong(l - 1);
                        frame.complete();
                        asyncSocket.unsafeWriteAndFlush(frame);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }

    static class DummyScheduler implements Scheduler {
        public void setEventloop(Eventloop eventloop) {
        }

        @Override
        public boolean tick() {
            return false;
        }

        @Override
        public void schedule(Frame task) {
        }
    }
}
