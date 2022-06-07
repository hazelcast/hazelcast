package com.hazelcast.tpc.engine.nio;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import com.hazelcast.tpc.engine.frame.SerialFrameAllocator;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class NioAsyncSocket_IntegrationTest {
    public static int requestTotal = 1000;
    public static int concurrency = 1;

    private NioEventloop clientEventloop;
    private NioEventloop serverEventloop;

    @Before
    public void before() {
        clientEventloop = new NioEventloop();
        clientEventloop.start();

        serverEventloop = new NioEventloop();
        serverEventloop.start();
    }

    @After
    public void after() throws InterruptedException {
        if (clientEventloop != null) {
            clientEventloop.shutdown();
            clientEventloop.awaitTermination(10, SECONDS);
        }

        if (serverEventloop != null) {
            serverEventloop.shutdown();
            serverEventloop.awaitTermination(10, SECONDS);
        }
    }

    @Test
    public void test() throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5000);

        NioAsyncServerSocket serverSocket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        NioAsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        for (int k = 0; k < concurrency; k++) {
            Frame frame = new Frame(128);
            frame.writeInt(-1);
            frame.writeLong(requestTotal / concurrency);
            frame.constructComplete();
            clientSocket.write(frame);
        }
        clientSocket.flush();

        assertOpenEventually(latch);
    }


    @NotNull
    private NioAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        NioAsyncSocket clientSocket = NioAsyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.readHandler(new NioAsyncReadHandler() {
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
                        frame.constructComplete();
                        socket.unsafeWriteAndFlush(frame);
                    }
                }
            }
        });
        clientSocket.activate(clientEventloop);
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private NioAsyncServerSocket newServer(SocketAddress serverAddress) {
        NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(serverEventloop);
        serverSocket.reuseAddress(true);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            socket.tcpNoDelay(true);
            socket.readHandler(new NioAsyncReadHandler() {
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
                        frame.reconstructComplete();
                        socket.unsafeWriteAndFlush(frame);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
