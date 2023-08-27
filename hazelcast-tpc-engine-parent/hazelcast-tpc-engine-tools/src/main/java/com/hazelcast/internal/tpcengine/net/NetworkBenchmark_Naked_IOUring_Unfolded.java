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

package com.hazelcast.internal.tpcengine.net;


import com.hazelcast.internal.tpcengine.iouring.AcceptMemory;
import com.hazelcast.internal.tpcengine.iouring.CompletionQueue;
import com.hazelcast.internal.tpcengine.iouring.Uring;
import com.hazelcast.internal.tpcengine.iouring.LinuxSocket;
import com.hazelcast.internal.tpcengine.iouring.SubmissionQueue;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.CQE_SIZE;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.OFFSET_CQE_FLAGS;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.OFFSET_CQE_RES;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.OFFSET_CQE_USERDATA;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_RECV;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_SEND;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_SETUP_COOP_TASKRUN;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_SETUP_SINGLE_ISSUER;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCK_CLOEXEC;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpcengine.iouring.Linux.strerror;
import static com.hazelcast.internal.tpcengine.iouring.LinuxSocket.AF_INET;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;

/**
 * Tests the lower level IOUring API. So without all the TPC functionality on top.
 * This helps to give us a base line of performance of the lower level API and how
 * much performance is lost in the TPC layer on top.
 * <p>
 * Good read:
 * https://www.alibabacloud.com/blog/599544
 */
public class NetworkBenchmark_Naked_IOUring_Unfolded {

    public static final long iterations = 400_000_000;
    public static final String cpuAffinityClient = "0";
    public static final String cpuAffinityServer = "2";
    public static final int iouringSetupFlags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN;
    public static final boolean registerRingFd = true;
    private final static InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5000);
    private final static Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    public static void main(String[] args) throws IOException, InterruptedException {
        Thread serverThread = new ServerThread();
        serverThread.start();
        Thread.sleep(1000);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread clientThread = new ClientThread(countDownLatch);

        long start = System.currentTimeMillis();
        clientThread.start();

        countDownLatch.await();
        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (iterations * 1000 / duration) + " ops");

        System.exit(0);
    }

    private static class ClientThread extends Thread {
        private final static int USERDATA_OP_READ = 1;
        private final static int USERDATA_OP_WRITE = 2;
//        final MpmcArrayQueue  concurrentWorkQueue = new MpmcArrayQueue(1024);
//        final AtomicBoolean scheduled = new AtomicBoolean();

        private final CountDownLatch countdownLatch;

        public ClientThread(CountDownLatch countDownLatch) {
            this.countdownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                ThreadAffinity threadAffinity = cpuAffinityClient == null ? null : new ThreadAffinity(cpuAffinityClient);
                if (threadAffinity != null) {
                    System.out.println("Setting ClientThread affinity " + cpuAffinityClient);
                    ThreadAffinityHelper.setAffinity(threadAffinity.nextAllowedCpus());
                }

                doRun();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private void doRun() throws IOException {
            final Uring uring = new Uring(4096, iouringSetupFlags);
            if (registerRingFd) {
                uring.registerRingFd();
            }
            final SubmissionQueue sq = uring.submissionQueue();
            final CompletionQueue cq = uring.completionQueue();
            final LinuxSocket socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
            socket.setTcpNoDelay(true);
            socket.connect(address);
            System.out.println("Connected");

            final ByteBuffer recvBuff = ByteBuffer.allocateDirect(64 * 1024);
            final long recvBuffAddr = addressOf(recvBuff);
            final ByteBuffer sndBuff = ByteBuffer.allocateDirect(64 * 1024);
            final long sendBuffAddr = addressOf(sndBuff);

            sndBuff.putLong(iterations);
            sndBuff.flip();

            //System.out.println("sndBuff.remaining");

            sq.offer(IORING_OP_SEND, 0, 0, socket.fd(), sendBuffAddr, sndBuff.remaining(), 0, USERDATA_OP_WRITE);
            sq.offer(IORING_OP_RECV, 0, 0, socket.fd(), recvBuffAddr, recvBuff.remaining(), 0, USERDATA_OP_READ);

            int localTail = 0; //cq.localTail;
            int localHead = cq.head;
            final long tailAddr = cq.tailAddr;
            final int ringMask = cq.ringMask;
            final long cqesAddr = cq.cqesAddr;
            final long headAddr = cq.headAddr;
            for (; ; ) {
//                concurrentWorkQueue.isEmpty();
//                scheduled.compareAndSet(true,false);
//                scheduled.set(false);
                sq.submitAndWait();
                localTail = UNSAFE.getIntVolatile(null, tailAddr);
                while (localHead < localTail) {
                    int index = localHead & ringMask;
                    long cqeAddress = cqesAddr + index * CQE_SIZE;

                    long userdata = UNSAFE.getLong(null, cqeAddress + OFFSET_CQE_USERDATA);
                    int res = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_RES);
                    int flags = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_FLAGS);

                    if (res < 0) {
                        throw new UncheckedIOException(new IOException(strerror(-res)));
                    }

                    if (userdata == USERDATA_OP_READ) {
                        //System.out.println("Client read " + res + " bytes");

                        recvBuff.position(recvBuff.position() + res);
                        recvBuff.flip();
                        long round = recvBuff.getLong();
                        //System.out.println("Client round:" + round);
                        recvBuff.clear();

                        if (round == 0) {
                            countdownLatch.countDown();
                            System.out.println("Done");
                            return;
                        }
                        sndBuff.putLong(round - 1);
                        sndBuff.flip();

                        sq.offer(IORING_OP_SEND, 0, 0, socket.fd(), sendBuffAddr, sndBuff.remaining(), 0, USERDATA_OP_WRITE);

                        sq.offer(IORING_OP_RECV, 0, 0, socket.fd(), recvBuffAddr, recvBuff.remaining(), 0, USERDATA_OP_READ);
                    } else if (userdata == USERDATA_OP_WRITE) {
                        //System.out.println("Client wrote " + res + " bytes");
                        //sndBuff.position(sndBuff.position() + res);
                        sndBuff.clear();
                    } else {
                        System.out.println("Client unknown userdata_id");
                    }

                    localHead++;
                }

                // release-store.
                UNSAFE.putOrderedInt(null, headAddr, localHead);
            }
        }
    }

    private static class ServerThread extends Thread {
//        final MpmcArrayQueue  concurrentWorkQueue = new MpmcArrayQueue(1024);
//        final AtomicBoolean scheduled = new AtomicBoolean();

        @Override
        public void run() {
            try {
                ThreadAffinity threadAffinity = cpuAffinityServer == null ? null : new ThreadAffinity(cpuAffinityServer);
                if (threadAffinity != null) {
                    System.out.println("Setting ServerThread affinity " + cpuAffinityServer);
                    ThreadAffinityHelper.setAffinity(threadAffinity.nextAllowedCpus());
                }
                doRun();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private void doRun() throws IOException {
            final Handler[] handlers = new Handler[1024];
            int handlerIdGenerator = 0;
            final Uring uring = new Uring(4096, iouringSetupFlags);
            if (registerRingFd) {
                uring.registerRingFd();
            }
            final SubmissionQueue sq = uring.submissionQueue();
            final CompletionQueue cq = uring.completionQueue();
            final LinuxSocket serverSocket = LinuxSocket.createNonBlockingTcpIpv4Socket();
            final AcceptMemory acceptMemory = new AcceptMemory();

            serverSocket.setReusePort(true);
            serverSocket.bind(address);
            serverSocket.listen(10);
            System.out.println("server started on:" + address);

            Handler acceptHandler = new Handler();
            acceptHandler.op = IORING_OP_ACCEPT;
            acceptHandler.id = handlerIdGenerator;
            handlers[acceptHandler.id] = acceptHandler;
            handlerIdGenerator++;

            sq.offer(IORING_OP_ACCEPT, 0, SOCK_NONBLOCK | SOCK_CLOEXEC, serverSocket.fd(), acceptMemory.addr, 0, acceptMemory.lenAddr, acceptHandler.id);

            int localTail = 0;;//cq.localTail;
            int localHead = cq.head;
            final long tailAddr = cq.tailAddr;
            final int ringMask = cq.ringMask;
            final long cqesAddr = cq.cqesAddr;
            final long headAddr = cq.headAddr;
            for (; ; ) {
//                concurrentWorkQueue.isEmpty();
//                scheduled.compareAndSet(true,false);
//                scheduled.set(false);
                sq.submitAndWait();
                // acquire load.
                localTail = UNSAFE.getIntVolatile(null, tailAddr);

                while (localHead < localTail) {
                    int index = localHead & ringMask;
                    long cqeAddress = cqesAddr + index * CQE_SIZE;

                    long userdata = UNSAFE.getLong(null, cqeAddress + OFFSET_CQE_USERDATA);
                    int res = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_RES);
                    int flags = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_FLAGS);
                    if (res < 0) {
                        throw new UncheckedIOException(new IOException(strerror(-res)));
                    }
                    final Handler handler = handlers[(int) userdata];

                    if (handler.op == IORING_OP_ACCEPT) {
                        LinuxSocket clientSocket = new LinuxSocket(res, AF_INET);
                        clientSocket.setTcpNoDelay(true);

                        sq.offer(IORING_OP_ACCEPT, 0, SOCK_NONBLOCK | SOCK_CLOEXEC, serverSocket.fd(), acceptMemory.addr, 0, acceptMemory.lenAddr, handler.id);
                        //System.out.println("Connection established " + clientSocket.getLocalAddress() + "->" + clientSocket.getRemoteAddress());

                        Handler readHandler = new Handler();
                        readHandler.op = IORING_OP_RECV;
                        readHandler.id = handlerIdGenerator;
                        handlerIdGenerator++;
                        readHandler.buff = ByteBuffer.allocateDirect(64 * 1024);
                        readHandler.buffAddr = addressOf(readHandler.buff);
                        readHandler.socket = clientSocket;
                        readHandler.socketFd = clientSocket.fd();
                        handlers[readHandler.id] = readHandler;

                        Handler writeHandler = new Handler();
                        writeHandler.op = IORING_OP_SEND;
                        writeHandler.id = handlerIdGenerator;
                        handlerIdGenerator++;
                        writeHandler.buff = ByteBuffer.allocateDirect(64 * 1024);
                        writeHandler.buffAddr = addressOf(writeHandler.buff);
                        writeHandler.socket = clientSocket;
                        writeHandler.socketFd = clientSocket.fd();
                        handlers[writeHandler.id] = writeHandler;

                        sq.offer(IORING_OP_RECV, 0, 0, readHandler.socketFd, readHandler.buffAddr, readHandler.buff.remaining(), 0, readHandler.id);
                    } else if (handler.op == IORING_OP_RECV) {
                        //System.out.println("Server Read " + res + " bytes");
                        Handler readHandler = handler;
                        readHandler.buff.position(handler.buff.position() + res);
                        readHandler.buff.flip();
                        long round = readHandler.buff.getLong();
                        //System.out.println("Server round:" + round);
                        readHandler.buff.clear();

                        Handler writeHandler = handlers[readHandler.id + 1];
                        writeHandler.buff.putLong(round);
                        writeHandler.buff.flip();

                        sq.offer(IORING_OP_RECV, 0, 0, readHandler.socketFd, readHandler.buffAddr, readHandler.buff.remaining(), 0, readHandler.id);
                        sq.offer(IORING_OP_SEND, 0, 0, writeHandler.socketFd, writeHandler.buffAddr, writeHandler.buff.remaining(), 0, writeHandler.id);
                        //SocketData asyncSocket = userdataArray[(int) userdata];

                    } else if (handler.op == IORING_OP_SEND) {
                        //System.out.println("Server wrote " + res + " bytes");
                        handler.buff.clear();
                    } else {
                        System.out.println("Server unknown userdata_id");

                    }
                    localHead++;
                }
                // release-store.
                UNSAFE.putOrderedInt(null, headAddr, localHead);
            }
        }
    }

    private static class Handler {
        byte op;
        int id;
        LinuxSocket socket;
        int socketFd;
        ByteBuffer buff;
        long buffAddr;

    }

}
