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
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

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
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

// TODO: In the future add padding to get isolated state separated from state accessed by other threads.
@SuppressWarnings({"checkstyle:TrailingComment",
        "checkstyle:MemberName",
        "checkstyle:TypeName",
        "checkstyle:MethodName",
        "checkstyle:VisibilityModifier",
        "checkstyle:ExecutableStatementCount",
        "checkstyle:DeclarationOrder"})
public final class IOUringAsyncSocket extends AsyncSocket {

    // Ensure JNI is initialized as soon as this class is loaded
    static {
        IOUringLibrary.ensureAvailable();
    }

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final IOUringEventloop eventloop;
    private final SubmissionQueue sq;

    private final CompletionHandler_OP_READ handler_OP_READ;
    private final long userdata_OP_READ;

    private final CompletionHandler_OP_WRITE handler_OP_WRITE;
    private final long userdata_OP_WRITE;

    private final CompletionHandler_OP_WRITEV handler_op_WRITEV;
    private final long userdata_OP_WRITEV;

    private final LinuxSocket linuxSocket;

    // ======================================================
    // For the reading side of the socket
    // ======================================================
    private final ByteBuffer rcvBuff;

    private final IOVector ioVector;
    final SubmissionHandler_WRITE submissionHandler_write = new SubmissionHandler_WRITE();

    private IOUringAsyncSocket(Builder builder) {
        super(builder);

        this.ioVector = builder.ioVector;
        this.linuxSocket = builder.nativeSocket;
        if (!clientSide) {
            this.localAddress = linuxSocket.getLocalAddress();
            this.remoteAddress = linuxSocket.getRemoteAddress();
        }

        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.sq = eventloop.sq;
        this.rcvBuff = ByteBuffer.allocateDirect(options.get(Options.SO_RCVBUF));

        this.handler_OP_READ = new CompletionHandler_OP_READ();
        this.userdata_OP_READ = eventloop.nextPermanentHandlerId();
        eventloop.handlers.put(userdata_OP_READ, handler_OP_READ);

        this.handler_OP_WRITE = new CompletionHandler_OP_WRITE();
        this.userdata_OP_WRITE = eventloop.nextPermanentHandlerId();
        eventloop.handlers.put(userdata_OP_WRITE, handler_OP_WRITE);

        this.handler_op_WRITEV = new CompletionHandler_OP_WRITEV();
        this.userdata_OP_WRITEV = eventloop.nextPermanentHandlerId();
        eventloop.handlers.put(userdata_OP_WRITEV, handler_op_WRITEV);

        // todo: on closing of the socket we need to deregister the event handlers.

        reader.init(this);
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
    protected boolean insideWrite(IOBuffer buf) {
        // todo: optimize
        return writeQueue.offer(buf);
    }

    @Override
    protected void start0() {
        if (!clientSide) {
            sq_addRead();
        }

        resetFlushed();
    }

    @Override
    protected void close0() throws IOException {
        super.close0();
        //todo: also think about releasing the resources like IOBuffers

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
        UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata_OP_READ);
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

    // todo: Doesn't need to be runnable any longer since not run on reactor.
    class SubmissionHandler_WRITE implements Runnable {

        @Override
        public void run() {
            try {
                if (flushThread.get() == null) {
                    throw new IllegalStateException("Channel should be in scheduled state");
                }

                int index = sq.nextIndex();
                if (index < 0) {
                    throw new IllegalStateException("No space in submission queue");
                }

                long sqeAddr = sq.sqesAddr + ((long) index * SIZEOF_SQE);
                ioVector.populate(writeQueue);

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
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
            }
        }
    }

    private class CompletionHandler_OP_WRITEV implements CompletionHandler {

        @Override
        public void handle(int res, int flags, long userdata) {
            //System.out.println(IOUringAsyncSocket.this + " CompletionHandler_OP_WRITEV.handle");
            try {
                if (res >= 0) {
                    metrics.incBytesWritten(res);
                    metrics.incWrites();
                    //System.out.println(IOUringAsyncSocket.this + " written " + res);
                    ioVector.compact(res);

                    if (ioVector.isEmpty() && writeQueue.isEmpty()) {
                        resetFlushed();
                    } else {
                        // todo: we don't need to
                        networkScheduler.schedule(IOUringAsyncSocket.this);
                    }
                } else if (res == -EAGAIN) {
                    System.out.println("EAGAIN");
                    // TODO: Can this lead to spinning?
                    // Deal with spurious EAGAIN; so we just reschedule the socket to be written.
                    // todo: return value
                    networkScheduler.schedule(IOUringAsyncSocket.this);
                } else {
                    throw newCQEFailedException("Failed to write data to the socket.", "writev(3p)", IORING_OP_WRITEV, -res);
                }
            } catch (Exception e) {
                close("Closing due to socket write problem.", e);
            }
        }
    }

    // todo: since this is copy of CompletionHandler_OP_WRITEV, do we need the first?
    private class CompletionHandler_OP_WRITE implements CompletionHandler {

        @Override
        public void handle(int res, int flags, long userdata) {
            //System.out.println(IOUringAsyncSocket.this + " CompletionHandler_OP_WRITE.handle");
            try {
                if (res >= 0) {
                    metrics.incBytesWritten(res);
                    metrics.incWrites();
                    // System.out.println(IOUringAsyncSocket.this + " written " + res);
                    ioVector.compact(res);

                    if (writeQueue.isEmpty() && ioVector.isEmpty()) {
                        resetFlushed();
                    } else {
                        networkScheduler.schedule(IOUringAsyncSocket.this);
                    }
                } else if (res == -EAGAIN) {
                    // Deal with spurious EAGAIN; so we just reschedule the socket to be written.
                    // todo: return value
                    networkScheduler.schedule(IOUringAsyncSocket.this);
                } else {
                    throw newCQEFailedException("Failed to write data to the socket.", "write(2)", IORING_OP_SEND, -res);
                }
            } catch (Exception e) {
                close("Closing due to socket write problem.", e);
            }
        }
    }

    private class CompletionHandler_OP_READ implements CompletionHandler {

        @Override
        public void handle(int res, int flags, long userdata) {
            // System.out.println(IOUringAsyncSocket.this + " CompletionHandler_OP_READ.handle");
            try {
                if (res > 0) {
                    int bytesRead = res;
                    LAST_READ_TIME_NANOS.setOpaque(IOUringAsyncSocket.this, eventloop.taskStartNanos());
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

                //System.out.println(IOUringAsyncSocket.this + " bytes read:" + res);
            } catch (Exception e) {
                close("Closing IOUringAsyncSocket due to exception", e);
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
            if (Options.TCP_NODELAY.equals(option)) {
                return true;
            } else if (Options.SO_RCVBUF.equals(option)) {
                return true;
            } else if (Options.SO_SNDBUF.equals(option)) {
                return true;
            } else if (Options.SO_KEEPALIVE.equals(option)) {
                return true;
            } else if (Options.SO_REUSEADDR.equals(option)) {
                return true;
            } else if (Options.TCP_KEEPCOUNT.equals(option)) {
                return true;
            } else if (Options.TCP_KEEPINTERVAL.equals(option)) {
                return true;
            } else if (Options.TCP_KEEPIDLE.equals(option)) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public <T> T get(Option<T> option) {
            checkNotNull(option, "option");

            try {
                if (Options.TCP_NODELAY.equals(option)) {
                    return (T) (Boolean) nativeSocket.isTcpNoDelay();
                } else if (Options.SO_RCVBUF.equals(option)) {
                    return (T) (Integer) nativeSocket.getReceiveBufferSize();
                } else if (Options.SO_SNDBUF.equals(option)) {
                    return (T) (Integer) nativeSocket.getSendBufferSize();
                } else if (Options.SO_KEEPALIVE.equals(option)) {
                    return (T) (Boolean) nativeSocket.isKeepAlive();
                } else if (Options.SO_REUSEADDR.equals(option)) {
                    return (T) (Boolean) nativeSocket.isReuseAddress();
                } else if (Options.TCP_KEEPCOUNT.equals(option)) {
                    return (T) (Integer) nativeSocket.getTcpKeepaliveProbes();
                } else if (Options.TCP_KEEPINTERVAL.equals(option)) {
                    return (T) (Integer) nativeSocket.getTcpKeepaliveIntvl();
                } else if (Options.TCP_KEEPIDLE.equals(option)) {
                    return (T) (Integer) nativeSocket.getTcpKeepAliveTime();
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to getOption [" + option.name() + "]", e);
            }
        }

        @SuppressWarnings("checkstyle:CyclomaticComplexity")
        @Override
        public <T> boolean set(Option<T> option, T value) {
            checkNotNull(option, "option");
            checkNotNull(value, "value");

            try {
                if (Options.TCP_NODELAY.equals(option)) {
                    nativeSocket.setTcpNoDelay((Boolean) value);
                    return true;
                } else if (Options.SO_RCVBUF.equals(option)) {
                    nativeSocket.setReceiveBufferSize((Integer) value);
                    return true;
                } else if (Options.SO_SNDBUF.equals(option)) {
                    nativeSocket.setSendBufferSize((Integer) value);
                    return true;
                } else if (Options.SO_KEEPALIVE.equals(option)) {
                    nativeSocket.setKeepAlive((Boolean) value);
                    return true;
                } else if (Options.SO_REUSEADDR.equals(option)) {
                    nativeSocket.setReuseAddress((Boolean) value);
                    return true;
                } else if (Options.TCP_KEEPCOUNT.equals(option)) {
                    nativeSocket.setTcpKeepAliveProbes((Integer) value);
                    return true;
                } else if (Options.TCP_KEEPIDLE.equals(option)) {
                    nativeSocket.setTcpKeepAliveTime((Integer) value);
                    return true;
                } else if (Options.TCP_KEEPINTERVAL.equals(option)) {
                    nativeSocket.setTcpKeepaliveIntvl((Integer) value);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to setOption [" + option.name() + "] with value [" + value + "]", e);
            }
        }
    }

    public static class Builder extends AsyncSocket.Builder {

        public LinuxSocket nativeSocket;
        public IOVector ioVector;

        Builder(IOUringAcceptRequest acceptRequest) {
            if (acceptRequest == null) {
                this.nativeSocket = LinuxSocket.openTcpIpv4Socket();
                this.clientSide = true;
            } else {
                this.nativeSocket = acceptRequest.linuxSocket;
                this.clientSide = false;
            }
            this.options = new IOUringOptions(nativeSocket);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(nativeSocket, "nativeSocket");

            if (ioVector == null) {
                ioVector = new IOVector(IOV_MAX);
            }
        }

        @Override
        protected AsyncSocket doBuild() {
            if (Thread.currentThread() == reactor.eventloopThread()) {
                return new IOUringAsyncSocket(this);
            } else {
                CompletableFuture<IOUringAsyncSocket> future = new CompletableFuture<>();
                reactor.execute(() -> {
                    try {
                        IOUringAsyncSocket socket = new IOUringAsyncSocket(Builder.this);
                        future.complete(socket);
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                        throw sneakyThrow(e);
                    }
                });

                return future.join();
            }
        }
    }
}
