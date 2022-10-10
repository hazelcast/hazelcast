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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.AsyncSocketOptions;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpc.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_RECV;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_SEND;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_WRITEV;
import static com.hazelcast.internal.tpc.iouring.Linux.IOV_MAX;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpc.util.ExceptionUtil.sneakyThrow;


public final class IOUringAsyncSocket extends AsyncSocket {

    static {
        // Ensure JNI is initialized as soon as this class is loaded
        IOUringLibrary.ensureAvailable();
    }

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

    private final NativeSocket nativeSocket;
    private final CircularQueue localTaskQueue;

    // ======================================================
    // For the reading side of the socket
    // ======================================================
    private final ByteBuffer receiveBuff;

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
    private final ReadHandler readHandler;

    // only accessed from eventloop thread.
    private boolean started;

    IOUringAsyncSocket(IOUringAsyncSocketBuilder builder) {
        super(builder.clientSide);

        assert Thread.currentThread() == builder.reactor.eventloopThread();

        this.nativeSocket = builder.nativeSocket;
        this.options = builder.options;
        if (!clientSide) {
            this.localAddress = nativeSocket.getLocalAddress();
            this.remoteAddress = nativeSocket.getRemoteAddress();
        }

        this.reactor = builder.reactor;
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.sq = eventloop.sq;
        this.localTaskQueue = eventloop.localTaskQueue;
        this.eventloopThread = reactor.eventloopThread();
        this.receiveBuff = ByteBuffer.allocateDirect(options.get(SO_RCVBUF));

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

        this.readHandler = builder.readHandler;
        readHandler.init(this);
    }

    @Override
    public IOUringReactor reactor() {
        return reactor;
    }

    public NativeSocket nativeSocket() {
        return nativeSocket;
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
                localTaskQueue.add(eventloopTask);
            } else {
                reactor.offer(eventloopTask);
            }
        }
    }

    private void resetFlushed() {
        if (!ioVector.isEmpty()) {
            localTaskQueue.add(eventloopTask);
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
                localTaskQueue.add(eventloopTask);
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
    protected void close0() throws IOException {
        //todo: also think about releasing the resources like IOBuffers

        if (reactor != null) {
            reactor.deregisterCloseable(this);
        }

        if (nativeSocket != null) {
            nativeSocket.close();
        }
    }

    // todo: boolean return
    private void sq_addRead() {
        int pos = receiveBuff.position();
        long address = addressOf(receiveBuff) + pos;
        int length = receiveBuff.remaining();
        if (length == 0) {
            throw new RuntimeException("Calling sq_addRead with 0 length for the read buffer");
        }

        // IORING_OP_RECV provides better performance than IORING_OP_READ
        // https://github.com/axboe/liburing/issues/536
        sq.offer(
                IORING_OP_RECV,     // op
                0,                  // flags
                0,                  // rw-flags
                nativeSocket.fd(),      // fd
                address,            // buffer address
                length,             // length
                0,                  // offset
                userdata_OP_READ    // userdata
        );
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress address) {
        if (logger.isFineEnabled()) {
            logger.fine("Connect to address:" + address);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            boolean oldBlocking = nativeSocket.isBlocking();
            nativeSocket.setBlocking(true);
            boolean connect = nativeSocket.connect(address);
            nativeSocket.setBlocking(oldBlocking);
            if (connect) {
                this.remoteAddress = nativeSocket.getRemoteAddress();
                this.localAddress = nativeSocket.getLocalAddress();

                if (logger.isInfoEnabled()) {
                    logger.info("Connected from " + localAddress + "->" + remoteAddress);
                }

                reactor.offer(() -> sq_addRead());

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

                ioVector.populate(unflushedBufs);

                if (ioVector.count() == 1) {
                    // There is just one item in the ioVecArray, so instead of doing a vectorized write, we do a regular write.
                    ByteBuffer buffer = ioVector.get(0).byteBuffer();
                    long bufferAddress = addressOf(buffer);
                    long address = bufferAddress + buffer.position();
                    int length = buffer.remaining();

                    // IORING_OP_SEND is more efficient than IORING_OP_WRITE
                    // todo: return value
                    sq.offer(
                            IORING_OP_SEND,         // op
                            0,                       // flags
                            0,                       // rw-flags
                            nativeSocket.fd(),           // fd
                            address,                 // buffer address
                            length,                  // number of bytes to write.
                            0,                       // offset
                            userdata_OP_WRITE        // userdata
                    );
                } else {
                    long address = ioVector.addr();
                    int count = ioVector.count();

                    // todo: return value
                    sq.offer(
                            IORING_OP_WRITEV,       // op
                            0,                      // flags
                            0,                      // rw-flags
                            nativeSocket.fd(),          // fd
                            address,                // iov start address
                            count,                  // number of iov entries
                            0,                      // offset
                            userdata_OP_WRITEV      // userdata
                    );
                }
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_WRITEV implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException("Socket writev failed. " + Linux.strerror(-res)));
                }

                //System.out.println(IOUringAsyncSocket.this + " written " + res);

                ioVector.compact(res);
                resetFlushed();
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_WRITE implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException("Socket write failed" + Linux.strerror(-res)));
                }
                //System.out.println(IOUringAsyncSocket.this + " written " + res);

                ioVector.compact(res);
                resetFlushed();
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class Handler_OP_READ implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw new UncheckedIOException(new IOException("Socket read failed. " + Linux.strerror(-res)));
                }

                //System.out.println("Bytes read:" + res);

                int read = res;
                readEvents.inc();
                bytesRead.inc(read);

                // io_uring has written the new data into the byteBuffer, but the position we
                // need to manually update.
                receiveBuff.position(receiveBuff.position() + read);

                // prepare buffer for reading
                receiveBuff.flip();

                // offer the read data for processing
                readHandler.onRead(receiveBuff);

                // prepare buffer for writing.
                compactOrClear(receiveBuff);

                // signal that we want to read more data.
                sq_addRead();
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }
}
