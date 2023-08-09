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


import com.hazelcast.internal.tpcengine.Option;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.NetworkScheduler;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCQEFailedException;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_RECV;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_SEND;
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
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNull;

@SuppressWarnings({"checkstyle:DeclarationOrder"})
public final class IOUringAsyncSocket extends AsyncSocket {

    // Ensure JNI is initialized as soon as this class is loaded
    static {
        IOUringLibrary.ensureAvailable();
    }

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final LinuxSocket linuxSocket;
    private final ReadHandler readHandler;
    final WriteHandler writeHandler;
    private final IOUring uring;

    private IOUringAsyncSocket(Builder builder) {
        super(builder);

        this.uring = builder.uring;
        this.linuxSocket = builder.linuxSocket;
        if (!clientSide) {
            this.localAddress = linuxSocket.getLocalAddress();
            this.remoteAddress = linuxSocket.getRemoteAddress();
        }
        this.readHandler = new ReadHandler(builder, this);
        this.writeHandler = new WriteHandler(builder, this);

        reader.init(this);
        if (writer != null) {
            writer.init(this);
        }
        reactor.sockets().add(this);
    }

    @Override
    public void setReadable(boolean readable) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isReadable() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected boolean insideWrite(Object buf) {
        // todo: optimize
        return writeQueue.offer(buf);
    }

    @Override
    protected void start0() {
        CompletionQueue cq = uring.cq();
        writeHandler.userdata = cq.nextPermanentHandlerId();
        cq.register(writeHandler.userdata, writeHandler);

        readHandler.userdata = cq.nextPermanentHandlerId();
        cq.register(readHandler.userdata, readHandler);

        if (!clientSide) {
            readHandler.addRequest();
        }

        resetFlushed();
    }

    @Override
    protected void close0() throws IOException {
        super.close0();

        if (linuxSocket != null) {
            linuxSocket.close();
        }

//        reactor.offer(() -> {
//            if (readHandler.userdata != 0) {
//                uring.cq().removeHandler(readHandler.userdata);
//            }
//
//            if (writeHandler.userdata != 0) {
//                uring.cq().removeHandler(writeHandler.userdata);
//            }
//        });
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

                reactor.offer(readHandler::addRequest);

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

    // In the future we could add a WriteHandler that is optimized
    // for when a Writer is set and bypasses the the whole ioVector ceremony
    // But the Writer API needs to harden a bit first.
    static final class WriteHandler implements CompletionHandler {

        private final IOBuffer sndBuffer;
        private final AtomicReference<Thread> flushThread;
        private final SubmissionQueue sq;
        private final IOVector ioVector;
        private final LinuxSocket linuxSocket;
        private final Writer writer;
        private final Queue writeQueue;
        private final IOUringAsyncSocket socket;
        private final Metrics metrics;
        private final NetworkScheduler networkScheduler;
        private long userdata;
        private boolean writerClean = true;

        WriteHandler(IOUringAsyncSocket.Builder builder, IOUringAsyncSocket socket) {
            this.socket = socket;
            this.flushThread = socket.flushThread;
            this.sq = builder.uring.sq();
            this.ioVector = builder.ioVector;
            this.linuxSocket = builder.linuxSocket;
            this.writer = builder.writer;
            this.writeQueue = builder.writeQueue;
            this.networkScheduler = builder.networkScheduler;
            this.metrics = builder.metrics;

            if (writer != null) {
                try {
                    // should go through an allocator
                    // if it goes through an allocator we need to ensure that
                    // the iovector doesn't release it
                    this.sndBuffer = new IOBuffer(builder.linuxSocket.getSendBufferSize(), true);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                // hack
                writer.dst = sndBuffer.byteBuffer();
            } else {
                this.sndBuffer = null;
            }
        }

        public void addRequest() {
            try {
                if (flushThread.get() == null) {
                    throw new IllegalStateException("Channel should be in scheduled state");
                }

                int index = sq.nextIndex();
                if (index < 0) {
                    throw new IllegalStateException("No space in submission queue");
                }

                long sqeAddr = sq.sqesAddr + ((long) index * SIZEOF_SQE);

                if (writer == null) {
                    ioVector.populate(writeQueue);
                } else {
                    writerClean = writer.onWrite();
                    sndBuffer.flip();
                    // todo: result

                    // add it if isn't added already
                    ioVector.offer(sndBuffer);
                }

                if (ioVector.cnt() == 1) {
                    // There is just one item in the ioVecArray, so instead of doing a vectorized write,
                    // we do a regular write.
                    ByteBuffer buffer = ioVector.get(0).byteBuffer();

                    // OP_SEND is faster than OP_WRITE.
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_SEND);
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                    UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, linuxSocket.fd());
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, addressOf(buffer) + buffer.position());
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, buffer.remaining());
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
                } else {
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_WRITEV);
                    UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
                    UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, linuxSocket.fd());
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, ioVector.addr());
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, ioVector.cnt());
                    UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
                    UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
                }
            } catch (Exception e) {
                socket.close("Closing socket due to write problem.", e);
            }
        }

        @Override
        public void completeRequest(int res, int flags, long userdata) {
            //System.out.println(IOUringAsyncSocket.this + " CompletionHandler_OP_WRITEV.handle");
            try {
                if (res >= 0) {
                    metrics.incBytesWritten(res);
                    metrics.incWrites();
                    //System.out.println(IOUringAsyncSocket.this + " written " + res);

                    boolean sndBufferClean = true;
                    if (sndBuffer != null) {
                        ioVector.clear();
                        sndBuffer.position(sndBuffer.position() + res);
                        sndBufferClean = !sndBuffer.byteBuffer().hasRemaining();
                        compactOrClear(sndBuffer.byteBuffer());
                    } else {
                        ioVector.compact(res);
                    }

                    // todo: this will not compact the underlying bytebuffer.

                    if (writerClean && sndBufferClean && ioVector.isEmpty() && writeQueue.isEmpty()) {
                        socket.resetFlushed();
                    } else {
                        // todo: we don't need to
                        networkScheduler.schedule(socket);
                    }
                } else if (res == -EAGAIN) {
                    System.out.println("EAGAIN");
                    // TODO: Can this lead to spinning?
                    // Deal with spurious EAGAIN; so we just reschedule the socket to be written.
                    // todo: return value
                    networkScheduler.schedule(socket);
                } else {
                    if (ioVector.cnt() == 1) {
                        throw newCQEFailedException(
                                "Failed to write data to the socket.", "write(2)", IORING_OP_SEND, -res);
                    } else {
                        throw newCQEFailedException(
                                "Failed to write data to the socket.", "writev(3p)", IORING_OP_WRITEV, -res);
                    }
                }
            } catch (Exception e) {
                socket.close("Closing socket due to write problem.", e);
            }
        }
    }

    private static final class ReadHandler implements CompletionHandler {

        private final Metrics metrics;
        private final ByteBuffer rcvBuff;
        private final SubmissionQueue sq;
        private final LinuxSocket linuxSocket;
        private final IOUringAsyncSocket socket;
        private final Reader reader;
        private final IOUringEventloop eventloop;
        private final long rcvBuffAddress;
        private long userdata;

        private ReadHandler(IOUringAsyncSocket.Builder builder, IOUringAsyncSocket socket) {
            this.socket = socket;
            this.reader = builder.reader;
            this.metrics = builder.metrics;
            this.linuxSocket = builder.linuxSocket;
            this.eventloop = (IOUringEventloop) builder.reactor.eventloop();
            this.sq = builder.uring.sq();
            this.rcvBuff = ByteBuffer.allocateDirect(builder.options.get(SO_RCVBUF));
            this.rcvBuffAddress = addressOf(rcvBuff);
        }

        // todo: boolean return
        private void addRequest() {
            int pos = rcvBuff.position();
            long address = rcvBuffAddress + pos;
            int length = rcvBuff.remaining();
            if (length == 0) {
                throw new RuntimeException("Calling sq_addRead with 0 length for the read buffer");
            }

            int index = sq.nextIndex();
            if (index < 0) {
                throw new RuntimeException("No space in submission queue");
            }

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
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata);
        }

        @Override
        public void completeRequest(int res, int flags, long userdata) {
            // System.out.println(IOUringAsyncSocket.this + " CompletionHandler_OP_READ.handle");
            try {
                if (res > 0) {
                    int bytesRead = res;
                    LAST_READ_TIME_NANOS.setOpaque(socket, eventloop.taskStartNanos());
                    metrics.incReads();
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
                    addRequest();
                } else if (res == 0) {
                    // 0 indicates end of stream.
                    // https://man7.org/linux/man-pages/man2/recv.2.html
                    socket.close("Socket closed by peer.", null);
                } else if (res == -ECONNRESET) {
                    // https://man7.org/linux/man-pages/man2/recv.2.html
                    socket.close("Socket reset by peer.", null);
                } else {
                    throw newCQEFailedException(
                            "Failed to read data from the socket.", "recv(2)", IORING_OP_RECV, -res);
                }

                // TODO: It could be that we run into an EAGAIN or EWOULDBLOCK.

                //System.out.println(IOUringAsyncSocket.this + " bytes read:" + res);
            } catch (Exception e) {
                socket.close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity",
            "checkstyle:returncount",
            "checkstyle:SimplifyBooleanReturn"})
    public static class IOUringOptions implements Options {

        private final LinuxSocket nativeSocket;

        IOUringOptions(LinuxSocket nativeSocket) {
            this.nativeSocket = nativeSocket;
        }

        @Override
        public boolean isSupported(Option option) {
            if (TCP_NODELAY.equals(option)) {
                return true;
            } else if (SO_RCVBUF.equals(option)) {
                return true;
            } else if (SO_SNDBUF.equals(option)) {
                return true;
            } else if (SO_KEEPALIVE.equals(option)) {
                return true;
            } else if (SO_REUSEADDR.equals(option)) {
                return true;
            } else if (TCP_KEEPCOUNT.equals(option)) {
                return true;
            } else if (TCP_KEEPINTERVAL.equals(option)) {
                return true;
            } else if (TCP_KEEPIDLE.equals(option)) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public <T> T get(Option<T> option) {
            checkNotNull(option, "option");

            try {
                if (TCP_NODELAY.equals(option)) {
                    return (T) (Boolean) nativeSocket.isTcpNoDelay();
                } else if (SO_RCVBUF.equals(option)) {
                    return (T) (Integer) nativeSocket.getReceiveBufferSize();
                } else if (SO_SNDBUF.equals(option)) {
                    return (T) (Integer) nativeSocket.getSendBufferSize();
                } else if (SO_KEEPALIVE.equals(option)) {
                    return (T) (Boolean) nativeSocket.isKeepAlive();
                } else if (SO_REUSEADDR.equals(option)) {
                    return (T) (Boolean) nativeSocket.isReuseAddress();
                } else if (TCP_KEEPCOUNT.equals(option)) {
                    return (T) (Integer) nativeSocket.getTcpKeepaliveProbes();
                } else if (TCP_KEEPINTERVAL.equals(option)) {
                    return (T) (Integer) nativeSocket.getTcpKeepaliveIntvl();
                } else if (TCP_KEEPIDLE.equals(option)) {
                    return (T) (Integer) nativeSocket.getTcpKeepAliveTime();
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to getOption [" + option.name() + "]", e);
            }
        }

        @SuppressWarnings("checkstyle:CyclomaticComplexity")
        @Override
        public <T> boolean set(Option<T> option, T value) {
            checkNotNull(option, "option");
            checkNotNull(value, "value");

            try {
                if (TCP_NODELAY.equals(option)) {
                    nativeSocket.setTcpNoDelay((Boolean) value);
                    return true;
                } else if (SO_RCVBUF.equals(option)) {
                    nativeSocket.setReceiveBufferSize((Integer) value);
                    return true;
                } else if (SO_SNDBUF.equals(option)) {
                    nativeSocket.setSendBufferSize((Integer) value);
                    return true;
                } else if (SO_KEEPALIVE.equals(option)) {
                    nativeSocket.setKeepAlive((Boolean) value);
                    return true;
                } else if (SO_REUSEADDR.equals(option)) {
                    nativeSocket.setReuseAddress((Boolean) value);
                    return true;
                } else if (TCP_KEEPCOUNT.equals(option)) {
                    nativeSocket.setTcpKeepAliveProbes((Integer) value);
                    return true;
                } else if (TCP_KEEPIDLE.equals(option)) {
                    nativeSocket.setTcpKeepAliveTime((Integer) value);
                    return true;
                } else if (TCP_KEEPINTERVAL.equals(option)) {
                    nativeSocket.setTcpKeepaliveIntvl((Integer) value);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to setOption [" + option.name() + "] with value [" + value + "]", e);
            }
        }
    }

    /**
     * A {@link IOUringAsyncSocket} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends AsyncSocket.Builder {

        public LinuxSocket linuxSocket;
        public IOVector ioVector;
        public IOUring uring;

        Builder(IOUringAcceptRequest acceptRequest) {
            if (acceptRequest == null) {
                this.linuxSocket = LinuxSocket.openTcpIpv4Socket();
                this.clientSide = true;
            } else {
                this.linuxSocket = acceptRequest.linuxSocket;
                this.clientSide = false;
            }
            this.options = new IOUringOptions(linuxSocket);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(linuxSocket, "nativeSocket");
            checkNotNull(uring, "uring");

            if (writer == null) {
                if (ioVector == null) {
                    ioVector = new IOVector(IOV_MAX);
                }
            } else {
                checkNull(ioVector, "ioVector");
                ioVector = new IOVector(1);
            }
        }

        @Override
        protected AsyncSocket construct() {
            if (Thread.currentThread() == reactor.eventloopThread()) {
                return new IOUringAsyncSocket(this);
            } else {
                return reactor.submit(() -> new IOUringAsyncSocket(Builder.this)).join();
            }
        }
    }
}
