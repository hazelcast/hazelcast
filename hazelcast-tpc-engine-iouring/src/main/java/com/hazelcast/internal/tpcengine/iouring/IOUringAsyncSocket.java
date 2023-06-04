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

package com.hazelcast.internal.tpcengine.iouring;


import com.hazelcast.internal.tpcengine.TaskQueue;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import org.jctools.queues.MpmcArrayQueue;
import sun.misc.Unsafe;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCQEFailedException;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_RECV;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_SEND;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_WRITE;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_WRITEV;
import static com.hazelcast.internal.tpcengine.iouring.Linux.EAGAIN;
import static com.hazelcast.internal.tpcengine.iouring.Linux.ECONNRESET;
import static com.hazelcast.internal.tpcengine.iouring.Linux.IOV_MAX;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_addr;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_fd;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_flags;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_ioprio;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_len;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_off;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_opcode;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_rw_flags;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_user_data;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.SIZEOF_SQE;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;

// TODO: In the future add padding to get isolated state separated from state accessed by other threads.
@SuppressWarnings({"checkstyle:TrailingComment",
        "checkstyle:MemberName",
        "checkstyle:TypeName",
        "checkstyle:MethodName",
        "checkstyle:VisibilityModifier",
        "checkstyle:ExecutableStatementCount",
        "checkstyle:DeclarationOrder"})
public final class IOUringAsyncSocket extends AsyncSocket {

    static {
        // Ensure JNI is initialized as soon as this class is loaded
        IOUringLibrary.ensureAvailable();
    }

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final IOUringEventloop eventloop;
    private final IOUringReactor reactor;
    private final Thread eventloopThread;
    private final SubmissionQueue sq;

    private final Handler_OP_READ handler_OP_READ;
    private final long userdata_OP_READ;

    private final Handler_OP_WRITE handler_OP_WRITE;
    private final long userdata_OP_WRITE;

    private final Handler_OP_WRITEV handler_op_WRITEV;
    private final long userdata_OP_WRITEV;

    private final LinuxSocket linuxSocket;
    private final TaskQueue localTaskQueue;

    // ======================================================
    // For the reading side of the socket
    // ======================================================
    private final ByteBuffer rcvBuff;

    // ======================================================
    // for the writing side of the socket.
    // ======================================================
    // concurrent state
    public final AtomicReference<Thread> flushThread = new AtomicReference<>();
    public final MpmcArrayQueue<IOBuffer> unflushedBufs = new MpmcArrayQueue<>(4096);

    // isolated state.
    private final IOVector ioVector = new IOVector(IOV_MAX);
    private final EventloopTask eventloopTask = new EventloopTask();
    private final IOUringAsyncSocketOptions options;
    private final AsyncSocketReader reader;

    // only accessed from eventloop thread.
    private boolean started;

    IOUringAsyncSocket(IOUringAsyncSocketBuilder builder) {
        super(builder.clientSide);

        assert Thread.currentThread() == builder.reactor.eventloopThread();

        this.linuxSocket = builder.nativeSocket;
        this.options = builder.options;
        if (!clientSide) {
            this.localAddress = linuxSocket.getLocalAddress();
            this.remoteAddress = linuxSocket.getRemoteAddress();
        }

        this.reactor = builder.reactor;
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.sq = eventloop.sq;
        this.localTaskQueue = eventloop.getTaskQueue(builder.taskGroupHandle);
        this.eventloopThread = reactor.eventloopThread();
        this.rcvBuff = ByteBuffer.allocateDirect(options.get(AsyncSocketOptions.SO_RCVBUF));

        this.handler_OP_READ = new Handler_OP_READ();
        this.userdata_OP_READ = eventloop.nextPermanentHandlerId();
        eventloop.handlers.put(userdata_OP_READ, handler_OP_READ);

        this.handler_OP_WRITE = new Handler_OP_WRITE();
        this.userdata_OP_WRITE = eventloop.nextPermanentHandlerId();
        eventloop.handlers.put(userdata_OP_WRITE, handler_OP_WRITE);

        this.handler_op_WRITEV = new Handler_OP_WRITEV();
        this.userdata_OP_WRITEV = eventloop.nextPermanentHandlerId();
        eventloop.handlers.put(userdata_OP_WRITEV, handler_op_WRITEV);

        // todo: deal with return value
        reactor.registerCloseable(IOUringAsyncSocket.this);

        // todo: on closing of the socket we need to deregister the event handlers.

        this.reader = builder.reader;
        reader.init(this);
    }

    @Override
    public IOUringReactor reactor() {
        return reactor;
    }

    public LinuxSocket nativeSocket() {
        return linuxSocket;
    }

    @Override
    public AsyncSocketOptions options() {
        return options;
    }

    @Override
    public void setReadable(boolean readable) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isReadable() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void start() {
        if (Thread.currentThread() == reactor.eventloopThread()) {
            start0();
        } else {
            CompletableFuture future = new CompletableFuture();
            reactor.execute(() -> {
                try {
                    start0();
                    future.complete(null);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                    throw sneakyThrow(e);
                }
            });
            future.join();
        }
    }

    private void start0() {
        if (started) {
            throw new IllegalStateException(this + " is already started");
        }
        started = true;

        if (!clientSide) {
            sq_addRead();
        }
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                localTaskQueue.local.add(eventloopTask);
            } else {
                reactor.offer(eventloopTask);
            }
        }
    }

    private void resetFlushed() {
        if (!ioVector.isEmpty()) {
            localTaskQueue.local.add(eventloopTask);
            return;
        }

        flushThread.set(null);

        if (!unflushedBufs.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                reactor.offer(eventloopTask);
            }
        }
    }

    @Override
    public boolean write(IOBuffer buf) {
        if (!buf.byteBuffer().isDirect()) {
            throw new IllegalArgumentException();
        }
        return unflushedBufs.add(buf);
    }

    @Override
    public boolean writeAll(Collection<IOBuffer> bufs) {
        return unflushedBufs.addAll(bufs);
    }

    @Override
    public boolean writeAndFlush(IOBuffer buf) {
        if (!buf.byteBuffer().isDirect()) {
            throw new IllegalArgumentException();
        }
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
                localTaskQueue.local.add(eventloopTask);
                if (ioVector.offer(buf)) {
                    result = true;
                } else {
                    result = unflushedBufs.offer(buf);
                }
            } else {
                result = unflushedBufs.offer(buf);
            }
        } else if (currentFlushThread == eventloopThread) {
            if (ioVector.offer(buf)) {
                result = true;
            } else {
                result = unflushedBufs.offer(buf);
            }
        } else {
            result = unflushedBufs.offer(buf);
            flush();
        }
        return result;
    }

    @Override
    protected void close0() {
        //todo: also think about releasing the resources like IOBuffers

        if (reactor != null) {
            reactor.deregisterCloseable(this);
        }

        if (linuxSocket != null) {
            linuxSocket.close();
        }
    }

    // todo: boolean return
    private void sq_addRead() {
        int pos = rcvBuff.position();

        // todo: why is this done every time
        long address = addressOf(rcvBuff) + pos;
        int length = rcvBuff.remaining();
        if (length == 0) {
            throw new RuntimeException("Calling sq_addRead with 0 length for the read buffer");
        }

        int index = sq.nextIndex();
        if (index >= 0) {
            long sqeAddr = sq.sqesAddr + index * SIZEOF_SQE;
            // IORING_OP_RECV provides better performance than IORING_OP_READ
            // https://github.com/axboe/liburing/issues/536
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_RECV);
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
            UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, linuxSocket.fd());
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, address);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, length);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata_OP_READ);
        } else {
            throw new RuntimeException("No space in submission queue");
        }
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress address) {
        if (logger.isFineEnabled()) {
            logger.fine("Connect to address:" + address);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            boolean oldBlocking = linuxSocket.isBlocking();
            linuxSocket.setBlocking(true);
            boolean connect = linuxSocket.connect(address);
            linuxSocket.setBlocking(oldBlocking);
            if (connect) {
                this.remoteAddress = linuxSocket.getRemoteAddress();
                this.localAddress = linuxSocket.getLocalAddress();

                if (logger.isInfoEnabled()) {
                    logger.info("Connected from " + localAddress + "->" + remoteAddress);
                }

                reactor.offer(this::sq_addRead);

                future.complete(null);
            } else {
                future.completeExceptionally(new IOException("Could not connect to " + address));
            }
        } catch (Exception e) {
            logger.warning(e);
            future.completeExceptionally(e);
        }

        return future;
    }

    private class EventloopTask implements Runnable {

        @Override
        public void run() {
            try {
                if (flushThread.get() == null) {
                    throw new RuntimeException("Channel should be in flushed state");
                }

                int index = sq.nextIndex();
                if (index >= 0) {
                    long sqeAddr = sq.sqesAddr + index * SIZEOF_SQE;

                    ioVector.populate(unflushedBufs);

                    if (ioVector.cnt() == 1) {
                        // There is just one item in the ioVecArray, so instead of doing a vectorized write,
                        // we do a regular write.
                        ByteBuffer buffer = ioVector.get(0).byteBuffer();

                        UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_SEND);
                        UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                        UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                        UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, linuxSocket.fd());
                        UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
                        UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, addressOf(buffer) + buffer.position());
                        UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, buffer.remaining());
                        UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                        UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata_OP_WRITE);
                    } else {
                        UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_WRITEV);
                        UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                        UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                        UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, linuxSocket.fd());
                        UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
                        UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, ioVector.addr());
                        UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, ioVector.cnt());
                        UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                        UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata_OP_WRITEV);
                    }
                } else {
                    throw new RuntimeException("No space in submission queue");
                }
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_WRITEV implements CompletionHandler {
//        private long handle;
//        private long egain;

        @Override
        public void handle(int res, int flags, long userdata) {
            try {
//                handle++;
//
//                if (handle % 10000 == 0) {
//                    System.out.println(100f * egain / handle + " %");
//                }

                if (res >= 0) {
                    //System.out.println(IOUringAsyncSocket.this + " written " + res);
                    ioVector.compact(res);
                    resetFlushed();
                } else if (res == -EAGAIN) {
                    //System.out.println(ioVector.count());

                    //egain++;
                    // TODO: Can this lead to spinning?
                    //System.out.println("-----");
                    // Deal with spurious EAGAIN; so we just reschedule the socket to be written.
                    localTaskQueue.local.add(eventloopTask);
                } else {
                    throw newCQEFailedException("Failed to write data to the socket.", "writev(3p)", IORING_OP_WRITEV, -res);
                }
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_WRITE implements CompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res >= 0) {
                    //System.out.println(IOUringAsyncSocket.this + " written " + res);
                    ioVector.compact(res);
                    resetFlushed();
                } else if (res == -EAGAIN) {
                    // Deal with spurious EAGAIN; so we just reschedule the socket to be written.
                    localTaskQueue.local.add(eventloopTask);
                } else {
                    throw newCQEFailedException("Failed to write data to the socket.", "write(2)", IORING_OP_WRITE, -res);
                }
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_READ implements CompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res > 0) {
                    int bytesRead = res;
                    LAST_READ_TIME_NANOS.setOpaque(IOUringAsyncSocket.this, eventloop.taskQueueStartNanos());
                    metrics.incReadEvents();
                    metrics.incBytesRead(bytesRead);

                    // io_uring has written the new data into the byteBuffer, but the position we
                    // need to manually update.
                    rcvBuff.position(rcvBuff.position() + bytesRead);

                    // prepare buffer for reading
                    rcvBuff.flip();

                    // offer the read data for processing
                    reader.onRead(rcvBuff);

                    // prepare buffer for writing.
                    compactOrClear(rcvBuff);

                    // we want to read more data
                    sq_addRead();
                } else if (res == 0) {
                    // 0 indicates end of stream.
                    // https://man7.org/linux/man-pages/man2/recv.2.html
                    close("Socket closed by peer.", null);
                } else if (res == -ECONNRESET) {
                    // https://man7.org/linux/man-pages/man2/recv.2.html
                    close("Socket reset by peer.", null);
                } else {
                    throw newCQEFailedException("Failed to read data from the socket.", "recv(2)", IORING_OP_RECV, -res);
                }

                // TODO: It could be that we run into an EAGAIN or EWOULDBLOCK.

                //System.out.println("Bytes read:" + res);
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }
}
