package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import com.hazelcast.tpc.engine.frame.SerialFrameAllocator;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class IOUringAsyncSocket_IntegrationTest {
    public static int requestTotal = 1000;
    public static int concurrency = 1;

    private IOUringEventloop clientEventloop;
    private IOUringEventloop serverEventloop;

    @Before
    public void before() {
        clientEventloop = new IOUringEventloop();
        clientEventloop.start();

        serverEventloop = new IOUringEventloop();
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
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1", 5010);

        IOUringAsyncServerSocket serverSocket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        IOUringAsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        for (int k = 0; k < concurrency; k++) {
            Frame frame = new Frame(128, true);
            frame.writeInt(-1);
            frame.writeLong(requestTotal / concurrency);
            frame.constructComplete();
            clientSocket.write(frame);
        }
        clientSocket.flush();

        assertOpenEventually(latch);
    }

    @NotNull
    private IOUringAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        IOUringAsyncSocket clientSocket = IOUringAsyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.readHandler(new IOUringAsyncReadHandler() {
            private final FrameAllocator responseAllocator = new SerialFrameAllocator(8, true);

            @Override
            public void onRead(ByteBuf buffer) {
                for (; ; ) {
                    if (buffer.readableBytes() < BYTES_INT + BYTES_LONG) {
                        return;
                    }

                    int size = buffer.readInt();
                    long l = buffer.readLong();
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

    private IOUringAsyncServerSocket newServer(SocketAddress serverAddress) {
        IOUringAsyncServerSocket serverSocket = IOUringAsyncServerSocket.open(serverEventloop);
        serverSocket.reuseAddress(true);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            socket.tcpNoDelay(true);
            socket.readHandler(new IOUringAsyncReadHandler() {
                private final FrameAllocator responseAllocator = new SerialFrameAllocator(8, true);

                @Override
                public void onRead(ByteBuf buffer) {
                    for (; ; ) {
                        if (buffer.readableBytes() < BYTES_INT + BYTES_LONG) {
                            return;
                        }
                        int size = buffer.readInt();
                        long l = buffer.readLong();

                        Frame frame = responseAllocator.allocate(8);
                        frame.writeInt(-1);
                        frame.writeLong(l - 1);
                        frame.constructComplete();
                        socket.unsafeWriteAndFlush(frame);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
