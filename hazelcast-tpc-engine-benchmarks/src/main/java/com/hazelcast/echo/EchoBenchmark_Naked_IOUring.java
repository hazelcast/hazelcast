package com.hazelcast.echo;

import com.hazelcast.internal.tpc.iouring.AcceptMemory;
import com.hazelcast.internal.tpc.iouring.CompletionQueue;
import com.hazelcast.internal.tpc.iouring.IOCompletionHandler;
import com.hazelcast.internal.tpc.iouring.IOUring;
import com.hazelcast.internal.tpc.iouring.NativeSocket;
import com.hazelcast.internal.tpc.iouring.SubmissionQueue;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_RECV;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_SEND;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_CLOEXEC;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpc.iouring.Linux.strerror;
import static com.hazelcast.internal.tpc.iouring.NativeSocket.AF_INET;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;

/**
 * Tests the lower level IOUring API. So without all the TPC functionality on top.
 * This helps to give us a base line of performance of the lower level API and how
 * much performance is lost in the TPC layer on top.
 * <p>
 * Good read:
 * https://www.alibabacloud.com/blog/599544
 */
public class EchoBenchmark_Naked_IOUring {

    public static final long iterations = 4_000_000;
    public static final String cpuAffinityClient = "0";
    public static final String cpuAffinityServer = "2";
    public static final int iouringSetupFlags = 0;//IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;

    private final static InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5000);

    public static void main(String[] args) throws IOException, InterruptedException {
        Thread serverThread = new ServerThread();
        serverThread.start();
        Thread.sleep(1000);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread clientThread = new ClientThread(countDownLatch);

        long start = System.currentTimeMillis();
        clientThread.start();

        //  serverThread.join();
        //  clientThread.join();

        countDownLatch.await();
        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (iterations * 1000 / duration) + " ops");

        System.exit(0);
    }

    private static class ClientThread extends Thread {
        private final static int USERDATA_OP_READ = 1;
        private final static int USERDATA_OP_WRITE = 2;

        private final CountDownLatch countdownLatch;
        private ByteBuffer receiveBuffer;
        private long receiveBufferAddress;
        private ByteBuffer sendBuffer;
        private long sendBufferAddress;
        final NativeSocket socket = NativeSocket.openTcpIpv4Socket();
        final CompletionHandler handler = new CompletionHandler();
        private IOUring uring;
        private SubmissionQueue sq;
        private CompletionQueue cq;

        public ClientThread(CountDownLatch countDownLatch) {
            this.countdownLatch = countDownLatch;
        }

        @Override
        public void run() {
            uring = new IOUring(4096, iouringSetupFlags);
            sq = uring.getSubmissionQueue();
            cq = uring.getCompletionQueue();

            try {
                ThreadAffinity threadAffinity = cpuAffinityClient == null ? null : new ThreadAffinity(cpuAffinityClient);
                if (threadAffinity != null) {
                    System.out.println("Setting ClientThread affinity " + cpuAffinityClient);
                    ThreadAffinityHelper.setAffinity(threadAffinity.nextAllowedCpus());
                }

                socket.setTcpNoDelay(true);
                socket.connect(address);
                System.out.println("Connected");

                this.receiveBuffer = ByteBuffer.allocateDirect(64 * 1024);
                this.receiveBufferAddress = addressOf(receiveBuffer);
                this.sendBuffer = ByteBuffer.allocateDirect(64 * 1024);
                this.sendBufferAddress = addressOf(sendBuffer);

                sendBuffer.putLong(iterations);
                sendBuffer.flip();

                //System.out.println("sendBuffer.remaining");

                sq.offer(
                        IORING_OP_SEND,                // op
                        0,                              // flags
                        0,                              // rw-flags
                        socket.fd(),                    // fd
                        sendBufferAddress,              // buffer address
                        sendBuffer.remaining(),         // number of bytes to write.
                        0,                              // offset
                        USERDATA_OP_WRITE               // userdata
                );

                sq.offer(
                        IORING_OP_RECV,                     // op
                        0,                                  // flags
                        0,                                  // rw-flags
                        socket.fd(),                        // fd
                        receiveBufferAddress,               // buffer address
                        receiveBuffer.remaining(),          // length
                        0,                                  // offset
                        USERDATA_OP_READ                    // userdata
                );

                final SubmissionQueue sq = this.sq;
                final CompletionQueue cq = this.cq;
                final CompletionHandler handler = this.handler;
                for (; ; ) {
                    sq.submitAndWait();
                    cq.process(handler);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private class CompletionHandler implements IOCompletionHandler {

            @Override
            public void handle(int res, int flags, long userdata) {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException(strerror(-res)));
                }

                if (userdata == USERDATA_OP_READ) {
                    //System.out.println("Client read " + res + " bytes");

                    receiveBuffer.position(receiveBuffer.position() + res);
                    receiveBuffer.flip();
                    long round = receiveBuffer.getLong();
                    //System.out.println("Client round:" + round);
                    receiveBuffer.clear();

                    if (round == 0) {
                        countdownLatch.countDown();
                        System.out.println("Done");
                        return;
                    }
                    sendBuffer.putLong(round - 1);
                    sendBuffer.flip();

                    sq.offer(
                            IORING_OP_SEND,                // op
                            0,                              // flags
                            0,                              // rw-flags
                            socket.fd(),                    // fd
                            sendBufferAddress,              // buffer address
                            sendBuffer.remaining(),         // number of bytes to write.
                            0,                              // offset
                            USERDATA_OP_WRITE               // userdata
                    );

                    sq.offer(
                            IORING_OP_RECV,                     // op
                            0,                                  // flags
                            0,                                  // rw-flags
                            socket.fd(),                        // fd
                            receiveBufferAddress,               // buffer address
                            receiveBuffer.remaining(),          // length
                            0,                                  // offset
                            USERDATA_OP_READ                    // userdata
                    );
                } else if (userdata == USERDATA_OP_WRITE) {
                    //System.out.println("Client wrote " + res + " bytes");
                    //sendBuffer.position(sendBuffer.position() + res);
                    sendBuffer.clear();
                } else {
                    System.out.println("Client unknown userdata_id");
                }
            }
        }
    }

    private static class ServerThread extends Thread {
        private int handlerIdGenerator = 0;
        private Handler[] handlers = new Handler[1024];
        IOUring uring;
        SubmissionQueue sq;
        CompletionQueue cq;
        final NativeSocket serverSocket = NativeSocket.openTcpIpv4Socket();
        final AcceptMemory acceptMemory = new AcceptMemory();


        @Override
        public void run() {
            uring = new IOUring(4096, iouringSetupFlags);
            sq = uring.getSubmissionQueue();
            cq = uring.getCompletionQueue();
            try {
                ThreadAffinity threadAffinity = cpuAffinityServer == null ? null : new ThreadAffinity(cpuAffinityServer);
                if (threadAffinity != null) {
                    System.out.println("Setting ServerThread affinity " + cpuAffinityServer);
                    ThreadAffinityHelper.setAffinity(threadAffinity.nextAllowedCpus());
                }

                serverSocket.setReusePort(true);
                serverSocket.bind(address);
                serverSocket.listen(10);
                System.out.println("server started on:" + address);

                Handler acceptHandler = new Handler();
                acceptHandler.opcode = IORING_OP_ACCEPT;
                acceptHandler.id = handlerIdGenerator;
                handlers[acceptHandler.id] = acceptHandler;
                handlerIdGenerator++;

                sq.offer(
                        IORING_OP_ACCEPT,
                        0,
                        SOCK_NONBLOCK | SOCK_CLOEXEC,
                        serverSocket.fd(),
                        acceptMemory.memoryAddress,
                        0,
                        acceptMemory.lengthMemoryAddress,
                        acceptHandler.id
                );

                CompletionHandler handler = new CompletionHandler();
                SubmissionQueue sq = this.sq;
                CompletionQueue cq = this.cq;
                for (; ; ) {
                    sq.submitAndWait();
                    cq.process(handler);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private class CompletionHandler implements IOCompletionHandler {

            @Override
            public void handle(int res, int flags, long userdata_id) {
                try {
                    if (res < 0) {
                        throw new UncheckedIOException(new IOException(strerror(-res)));
                    }
                    //System.out.println("server handle " + res + " userdata " + userdata_id);
                    Handler handler = handlers[(int) userdata_id];

                    if (handler.opcode == IORING_OP_ACCEPT) {
                        NativeSocket clientSocket = new NativeSocket(res, AF_INET);
                        clientSocket.setTcpNoDelay(true);

                        sq.offer(
                                IORING_OP_ACCEPT,
                                0,
                                SOCK_NONBLOCK | SOCK_CLOEXEC,
                                serverSocket.fd(),
                                acceptMemory.memoryAddress,
                                0,
                                acceptMemory.lengthMemoryAddress,
                                handler.id
                        );
                        //System.out.println("Connection established " + clientSocket.getLocalAddress() + "->" + clientSocket.getRemoteAddress());

                        Handler readHandler = new Handler();
                        readHandler.opcode = IORING_OP_RECV;
                        readHandler.id = handlerIdGenerator;
                        handlerIdGenerator++;
                        readHandler.buffer = ByteBuffer.allocateDirect(64 * 1024);
                        readHandler.bufferAddress = addressOf(readHandler.buffer);
                        readHandler.socket = clientSocket;
                        handlers[readHandler.id] = readHandler;

                        Handler writeHandler = new Handler();
                        writeHandler.opcode = IORING_OP_SEND;
                        writeHandler.id = handlerIdGenerator;
                        handlerIdGenerator++;
                        writeHandler.buffer = ByteBuffer.allocateDirect(64 * 1024);
                        writeHandler.bufferAddress = addressOf(writeHandler.buffer);
                        writeHandler.socket = clientSocket;
                        handlers[writeHandler.id] = writeHandler;


                        sq.offer(
                                IORING_OP_RECV,                         // op
                                0,                                      // flags
                                0,                                      // rw-flags
                                clientSocket.fd(),                      // fd
                                readHandler.bufferAddress,         // buffer address
                                readHandler.buffer.remaining(),    // length
                                0,                                      // offset
                                readHandler.id                     // userdata
                        );

                    } else if (handler.opcode == IORING_OP_RECV) {
                        //System.out.println("Server Read " + res + " bytes");
                        Handler readHandler = handler;

                        readHandler.buffer.position(handler.buffer.position() + res);
                        readHandler.buffer.flip();
                        long round = readHandler.buffer.getLong();
                        //System.out.println("Server round:" + round);
                        readHandler.buffer.clear();

                        Handler writeHandler = handlers[handler.id + 1];
                        writeHandler.buffer.putLong(round);
                        writeHandler.buffer.flip();

                        sq.offer(
                                IORING_OP_RECV,                         // op
                                0,                                      // flags
                                0,                                      // rw-flags
                                handler.socket.fd(),                      // fd
                                handler.bufferAddress,         // buffer address
                                handler.buffer.remaining(),    // length
                                0,                                      // offset
                                handler.id                     // userdata
                        );

                        sq.offer(
                                IORING_OP_SEND,                            // op
                                0,                                          // flags
                                0,                                          // rw-flags
                                writeHandler.socket.fd(),              // fd
                                writeHandler.bufferAddress,            // buffer address
                                writeHandler.buffer.remaining(),       // number of bytes to write.
                                0,                                          // offset
                                writeHandler.id                        // userdata
                        );
                        //SocketData asyncSocket = userdataArray[(int) userdata];

                    } else if (handler.opcode == IORING_OP_SEND) {
                        //System.out.println("Server wrote " + res + " bytes");
                        handler.buffer.clear();
                    } else {
                        System.out.println("Server unknown userdata_id");

                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class Handler {
        byte opcode;
        int id;
        NativeSocket socket;
        ByteBuffer buffer;
        long bufferAddress;

    }

}
