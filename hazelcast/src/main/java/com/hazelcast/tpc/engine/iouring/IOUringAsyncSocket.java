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

package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.engine.AsyncSocket;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.ReadHandler;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.Buffer;
import io.netty.channel.unix.IovArray;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringSubmissionQueue;
import io.netty.incubator.channel.uring.LinuxSocket;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static io.netty.incubator.channel.uring.Native.IORING_OP_READ;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITEV;

public final class IOUringAsyncSocket extends AsyncSocket {

    static {
        // Ensure JNI is initialized as soon as this class is loaded
        IOUring.ensureAvailability();
    }

    private final boolean clientSide;
    private Thread eventloopThread;

    public static IOUringAsyncSocket open() {
        return new IOUringAsyncSocket();
    }

    private final LinuxSocket socket;
    private IOUringEventloop eventloop;

    // ======================================================
    // For the reading side of the socket
    // ======================================================
    private ByteBuf receiveBuff;
    private IOUringSubmissionQueue sq;

    // ======================================================
    // for the writing side of the socket.
    // ======================================================
    // concurrent state
    public AtomicReference<Thread> flushThread = new AtomicReference<>();
    public final MpmcArrayQueue<IOBuffer> unflushedBufs = new MpmcArrayQueue<>(4096);

    // isolated state.
    public IovArray iovArray;
    public final IOVector ioVector = new IOVector();
    private IOUringAsyncReadHandler readHandler;
    private final EventloopHandler eventloopHandler = new EventloopHandler();

    private IOUringAsyncSocket() {
        try {
            this.socket = LinuxSocket.newSocketStream(false);
            socket.setBlocking();
            this.clientSide = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    IOUringAsyncSocket(LinuxSocket socket) {
        try {
            this.socket = socket;
            socket.setBlocking();
            this.remoteAddress = socket.remoteAddress();
            this.localAddress = socket.localAddress();
            this.clientSide = false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IOUringEventloop eventloop() {
        return eventloop;
    }

    public LinuxSocket socket() {
        return socket;
    }

    @Override
    public void readHandler(ReadHandler readHandler) {
        this.readHandler = (IOUringAsyncReadHandler) checkNotNull(readHandler);
        this.readHandler.init(this);
    }

    @Override
    public void soLinger(int soLinger) {
        try {
            socket.setSoLinger(soLinger);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int soLinger() {
        try {
            return socket.getSoLinger();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void keepAlive(boolean keepAlive) {
        try {
            socket.setKeepAlive(keepAlive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isKeepAlive() {
        try {
            return socket.isKeepAlive();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isTcpNoDelay() {
        try {
            return socket.isTcpNoDelay();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void tcpNoDelay(boolean tcpNoDelay) {
        try {
            socket.setTcpNoDelay(tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int receiveBufferSize() {
        try {
            return socket.getReceiveBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void receiveBufferSize(int size) {
        try {
            socket.setReceiveBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int sendBufferSize() {
        try {
            return socket.getSendBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void sendBufferSize(int size) {
        try {
            socket.setSendBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void activate(Eventloop l) {
        if (this.eventloop != null) {
            throw new IllegalStateException("Can't activate an already activated AsyncSocket");
        }

        IOUringEventloop eventloop = (IOUringEventloop) l;
        this.eventloop = eventloop;
        this.eventloopThread = eventloop.eventloopThread();
        this.eventloop.execute(() -> {
            ByteBuf iovArrayBuffer = eventloop.iovArrayBufferAllocator.directBuffer(1024 * IovArray.IOV_SIZE);
            iovArray = new IovArray(iovArrayBuffer);
            sq = eventloop.sq;
            eventloop.completionListeners.put(socket.intValue(), new EventloopHandler());
            receiveBuff = eventloop.allocator.directBuffer(receiveBufferSize());

            if (!clientSide) {
                sq_addRead();
            }
        });
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                eventloop.localRunQueue.add(eventloopHandler);
            } else {
                eventloop.execute(eventloopHandler);
            }
        }
    }

    // called by the Reactor.
    private void resetFlushed() {
        if (!ioVector.isEmpty()) {
            eventloop.localRunQueue.add(eventloopHandler);
            return;
        }

        flushThread.set(null);

        if (!unflushedBufs.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                eventloop.execute(eventloopHandler);
            }
        }
    }

    @Override
    public boolean write(IOBuffer buf) {
        return unflushedBufs.add(buf);
    }

    @Override
    public boolean writeAll(Collection<IOBuffer> bufs) {
        return unflushedBufs.addAll(bufs);
    }

    @Override
    public boolean writeAndFlush(IOBuffer buf) {
        boolean result = unflushedBufs.add(buf);
        flush();
        return result;
    }

    @Override
    public boolean unsafeWriteAndFlush(IOBuffer buf) {
        Thread currentFlushThread = flushThread.get();
        Thread currentThread = Thread.currentThread();

        assert currentThread == eventloopThread;

        boolean result;
        if (currentFlushThread == null) {
            if (flushThread.compareAndSet(null, currentThread)) {
                eventloop.localRunQueue.add(eventloopHandler);
                if (ioVector.add(buf)) {
                    result = true;
                } else {
                    result = unflushedBufs.add(buf);
                }
            } else {
                result = unflushedBufs.add(buf);
            }
        } else if (currentFlushThread == eventloopThread) {
            if (ioVector.add(buf)) {
                result = true;
            } else {
                result = unflushedBufs.add(buf);
            }
        } else {
            result = unflushedBufs.add(buf);
            flush();
        }
        return result;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (logger.isInfoEnabled()) {
                logger.info("Closing  " + this);
            }
            //todo: also think about releasing the resources like IOBuffers
            // perhaps add a one time close check

            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (eventloop != null) {
                eventloop.deregisterResource(this);
            }
        }
    }

    private void sq_addRead() {
        //System.out.println("sq_addRead writerIndex:" + b.writerIndex() + " capacity:" + b.capacity());
        sq.addRead(socket.intValue(),
                receiveBuff.memoryAddress(),
                receiveBuff.writerIndex(),
                receiveBuff.capacity(),
                (short) 0);
    }

    @Override
    public CompletableFuture<AsyncSocket> connect(SocketAddress address) {
        if (logger.isFineEnabled()) {
            logger.fine("Connect to address:" + address);
        }

        CompletableFuture<AsyncSocket> future = new CompletableFuture<>();

        try {
            //System.out.println(getName() + " connectRequest to address:" + address);

            if (socket.connect(address)) {
                this.remoteAddress = socket.remoteAddress();
                this.localAddress = socket.localAddress();

                if (logger.isInfoEnabled()) {
                    logger.info("Connected from " + localAddress + "->" + remoteAddress);
                }

                eventloop.execute(() -> sq_addRead());

                future.complete(this);
            } else {
                future.completeExceptionally(new IOException("Could not connect to " + address));
            }
        } catch (Exception e) {
            e.printStackTrace();
            future.completeExceptionally(e);
        }

        return future;
    }

    private class EventloopHandler implements CompletionListener, Runnable {

        @Override
        public void run() {
            if (flushThread.get() == null) {
                throw new RuntimeException("Channel should be in flushed state");
            }

            ioVector.fill(unflushedBufs);

            int ioVectorSize = ioVector.size();
            if (ioVectorSize == 1) {
                ByteBuffer buffer = ioVector.get(0).byteBuffer();
                sq.addWrite(socket.intValue(),
                        Buffer.memoryAddress(buffer),
                        buffer.position(),
                        buffer.limit(),
                        (short) 0);
            } else {
                int offset = iovArray.count();
                ioVector.fillIoArray(iovArray);

                sq.addWritev(socket.intValue(),
                        iovArray.memoryAddress(offset),
                        iovArray.count() - offset,
                        (short) 0);
            }
        }

        @Override
        public void handle(int fd, int res, int flags, byte op, short data) {
            if (res >= 0) {
                if (op == IORING_OP_READ) {
                    readEvents.inc();
                    bytesRead.inc(res);
                    //System.out.println("handle_IORING_OP_READ fd:" + fd + " bytes read: " + res);
                    receiveBuff.writerIndex(receiveBuff.writerIndex() + res);
                    readHandler.onRead(receiveBuff);
                    receiveBuff.discardReadBytes();
                    // we want to read more data.
                    sq_addRead();
                } else if (op == IORING_OP_WRITE) {
                    //System.out.println("handle_IORING_OP_WRITE fd:" + fd + " bytes written: " + res);
                    ioVector.compact(res);
                    resetFlushed();
                } else if (op == IORING_OP_WRITEV) {
                    //System.out.println("handle_IORING_OP_WRITEV fd:" + fd + " bytes written: " + res);
                    ioVector.compact(res);
                    iovArray.clear();
                    resetFlushed();
                }
            } else {
                logger.warning("Problem: handle_IORING_OP_READ res:" + res);
            }
        }
    }
}
