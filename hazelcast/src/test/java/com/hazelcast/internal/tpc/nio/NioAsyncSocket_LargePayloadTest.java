package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
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
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.tpc.util.Util.put;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class NioAsyncSocket_LargePayloadTest {
    public static final int SOCKET_BUFFER_SIZE = 64 * 1024;
    public static int requestTotal = 20000;
    public static int concurrency = 10;

    private NioEventloop clientEventloop;
    private NioEventloop serverEventloop;
    private NioAsyncSocket clientSocket;
    private NioAsyncServerSocket serverSocket;

    @Before
    public void before() {
        NioEventloop.NioConfiguration clientConfig = new NioEventloop.NioConfiguration();
        clientConfig.setThreadName("client-eventloop");
        clientEventloop = new NioEventloop(clientConfig);
        clientEventloop.start();

        NioEventloop.NioConfiguration serverConfig = new NioEventloop.NioConfiguration();
        serverConfig.setThreadName("server-eventloop");
        serverEventloop = new NioEventloop(serverConfig);
        serverEventloop.start();
    }

    @After
    public void after() throws InterruptedException {
        if (clientSocket != null) {
            clientSocket.close();
        }

        if (serverSocket != null) {
            serverSocket.close();
        }

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
    public void test_1KB() throws InterruptedException {
        test(1024);
    }

    @Test
    public void test_2KB() throws InterruptedException {
        test(2 * 1024);
    }

    @Test
    public void test_4KB() throws InterruptedException {
        test(4 * 1024);
    }

    @Test
    public void test_16KB() throws InterruptedException {
        test(16 * 1024);
    }

    @Test
    public void test_32KB() throws InterruptedException {
        test(32 * 1024);
    }

    @Test
    public void test_64KB() throws InterruptedException {
        test(64 * 1024);
    }

    @Test
    public void test_128KB() throws InterruptedException {
        test(128 * 1024);
    }

    @Test
    public void test_256KB() throws InterruptedException {
        test(256 * 1024);
    }

    @Test
    public void test_512KB() throws InterruptedException {
        test(512 * 1024);
    }

    @Test
    public void test_1MB() throws InterruptedException {
        test(1024 * 1024);
    }

    @Test
    public void test_2MB() throws InterruptedException {
        test(2048 * 1024);
    }

   public void test(int payloadSize) throws InterruptedException {
        SocketAddress serverAddress = new InetSocketAddress("127.0.0.1",5000);

        NioAsyncServerSocket serverSocket = newServer(serverAddress);

        CountDownLatch latch = new CountDownLatch(concurrency);

        NioAsyncSocket clientSocket = newClient(serverAddress, latch);

        System.out.println("Starting");

        for (int k = 0; k < concurrency; k++) {
            byte[] payload = new byte[payloadSize];
            IOBuffer buf = new IOBuffer(INT_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + payload.length);
            buf.writeInt(payload.length);
            buf.writeLong(requestTotal / concurrency);
            buf.writeBytes(payload);
            buf.flip();
            if (!clientSocket.write(buf)) {
                throw new RuntimeException();
            }
        }
        clientSocket.flush();
        System.out.println("Flushed");

        assertOpenEventually(latch);
    }

    @NotNull
    private NioAsyncSocket newClient(SocketAddress serverAddress, CountDownLatch latch) {
        clientSocket = NioAsyncSocket.open();
        clientSocket.tcpNoDelay(true);
        clientSocket.soLinger(0);
        clientSocket.sendBufferSize(SOCKET_BUFFER_SIZE);
        clientSocket.receiveBufferSize(SOCKET_BUFFER_SIZE);
        clientSocket.readHandler(new NioAsyncReadHandler() {
            private boolean firstTime = true;
            private ByteBuffer payloadBuffer;
            private long round;
            private int payloadSize = -1;
            private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

            @Override
            public void onRead(ByteBuffer receiveBuf) {
                if (firstTime) {
                    firstTime = false;
                    System.out.println("Client: first time");
                }

                for (; ; ) {
                    if (payloadSize == -1) {
                        if (receiveBuf.remaining() < BYTES_INT + BYTES_LONG) {
                            break;
                        }

                        payloadSize = receiveBuf.getInt();
                        round = receiveBuf.getLong();
                        payloadBuffer = ByteBuffer.allocate(payloadSize);
                    }

                    put(payloadBuffer, receiveBuf);

                    if (payloadBuffer.remaining() > 0) {
                        // not all bytes have been received.
                        break;
                    }
                    payloadBuffer.flip();

                    System.out.println("round:" + round);
                    if (round == 0) {
                        latch.countDown();
                    } else {
                        IOBuffer responseBuf = responseAllocator.allocate(BYTES_INT + BYTES_LONG + payloadSize);
                        responseBuf.writeInt(payloadSize);
                        responseBuf.writeLong(round);
                        responseBuf.write(payloadBuffer);
                        responseBuf.flip();
                        if (!socket.unsafeWriteAndFlush(responseBuf)) {
                            throw new RuntimeException();
                        }
                    }
                    payloadSize = -1;
                }
            }
        });
        clientSocket.activate(clientEventloop);
        clientSocket.connect(serverAddress).join();
        return clientSocket;
    }

    private NioAsyncServerSocket newServer(SocketAddress serverAddress) {
        serverSocket = NioAsyncServerSocket.open(serverEventloop);
        serverSocket.receiveBufferSize(SOCKET_BUFFER_SIZE);
        serverSocket.bind(serverAddress);
        serverSocket.listen(10);
        serverSocket.accept(socket -> {
            System.out.println("Server side accepted " + socket);
            socket.soLinger(-1);
            socket.tcpNoDelay(true);
            socket.sendBufferSize(SOCKET_BUFFER_SIZE);
            socket.receiveBufferSize(serverSocket.receiveBufferSize());
            socket.readHandler(new NioAsyncReadHandler() {
                private boolean firstTime;
                private ByteBuffer payloadBuffer;
                private long round;
                private int payloadSize = -1;
                private final IOBufferAllocator responseAllocator = new NonConcurrentIOBufferAllocator(8, true);

                @Override
                public void onRead(ByteBuffer receiveBuf) {
                    if (firstTime) {
                        firstTime = false;
                        System.out.println("Client: first time");
                    }

                    for (; ; ) {
                        if (payloadSize == -1) {
                            if (receiveBuf.remaining() < BYTES_INT + BYTES_LONG) {
                                break;
                            }
                            payloadSize = receiveBuf.getInt();
                            round = receiveBuf.getLong();
                            payloadBuffer = ByteBuffer.allocate(payloadSize);
                        }

                        put(payloadBuffer, receiveBuf);
                        if (payloadBuffer.remaining() > 0) {
                            // not all bytes have been received.
                            break;
                        }

                        payloadBuffer.flip();
                        IOBuffer responseBuf = responseAllocator.allocate(BYTES_INT + BYTES_LONG + payloadSize);
                        responseBuf.writeInt(payloadSize);
                        responseBuf.writeLong(round - 1);
                        responseBuf.write(payloadBuffer);
                        responseBuf.flip();
                        if (!socket.unsafeWriteAndFlush(responseBuf)) {
                            throw new RuntimeException();
                        }
                        payloadSize = -1;
                    }
                }
            });
            System.out.println("Activating server side socket");
            socket.activate(serverEventloop);
        });

        return serverSocket;
    }
}
