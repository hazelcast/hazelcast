package com.hazelcast.alto.engine.nio;

import com.hazelcast.internal.tpc.nio.NioAsyncReadHandler;
import com.hazelcast.internal.tpc.nio.NioAsyncServerSocket;
import com.hazelcast.internal.tpc.nio.NioAsyncSocket;
import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
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
            IOBuffer buf = new IOBuffer(128);
            buf.writeInt(8);
            buf.writeLong(requestTotal / concurrency);
            buf.flip();
            clientSocket.write(buf);
        }
        clientSocket.flush();

        assertOpenEventually(latch);
    }


    @NotNull
    private NioAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        NioAsyncSocket clientSocket = NioAsyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.readHandler(new NioAsyncReadHandler() {
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

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
                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(8);
                        buf.writeLong(l);
                        buf.flip();
                        socket.unsafeWriteAndFlush(buf);
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
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer buffer) {
                    for (; ; ) {
                        if (buffer.remaining() < BYTES_INT + BYTES_LONG) {
                            return;
                        }
                        int size = buffer.getInt();
                        long l = buffer.getLong();

                        IOBuffer buf = responseAllocator.allocate(8);
                        buf.writeInt(-1);
                        buf.writeLong(l - 1);
                        buf.flip();
                        socket.unsafeWriteAndFlush(buf);
                    }
                }
            });
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
