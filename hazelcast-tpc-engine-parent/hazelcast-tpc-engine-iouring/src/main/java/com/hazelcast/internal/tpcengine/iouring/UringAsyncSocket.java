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


import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.NetworkScheduler;
import com.hazelcast.internal.tpcengine.util.Option;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.TYPE_SOCKET;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.encodeUserdata;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCQEFailedException;
import static com.hazelcast.internal.tpcengine.iouring.Linux.EAGAIN;
import static com.hazelcast.internal.tpcengine.iouring.Linux.ECONNREFUSED;
import static com.hazelcast.internal.tpcengine.iouring.Linux.ECONNRESET;
import static com.hazelcast.internal.tpcengine.iouring.Linux.IOV_MAX;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_SOCKADDR_STORAGE;
import static com.hazelcast.internal.tpcengine.iouring.SocketAddressUtil.memsetSocketAddrIn;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_CONNECT;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_RECV;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_SEND;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_SHUTDOWN;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_WRITEV;
import static com.hazelcast.internal.tpcengine.iouring.Uring.opcodeToString;
import static com.hazelcast.internal.tpcengine.net.AsyncSocket.Options.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNull;

@SuppressWarnings({"checkstyle:DeclarationOrder"})
public final class UringAsyncSocket extends AsyncSocket {

    // Ensure JNI is initialized as soon as this class is loaded
    static {
        UringLibrary.ensureAvailable();
    }

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final LinuxSocket linuxSocket;
    final Handler handler;
    private final Uring uring;

    private UringAsyncSocket(Builder builder) {
        super(builder);
        this.uring = builder.uring;
        this.linuxSocket = builder.linuxSocket;
        if (!clientSide) {
            this.localAddress = linuxSocket.getLocalAddress();
            this.remoteAddress = linuxSocket.getRemoteAddress();
        }
        this.handler = new Handler(builder, this);
        reader.init(this);
        if (writer != null) {
            writer.init(this);
        }
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
    protected boolean insideWrite(Object msg) {
        // todo: optimize
        return writeQueue.offer(msg);
    }

    @Override
    protected void start00() {
        ((UringNetworkScheduler) networkScheduler).register(handler);

        if (!clientSide) {
            handler.prepareRead();
        }

        resetFlushed();
    }

    @Override
    protected void close0() throws IOException {
        super.close0();

        if (eventloopThread == Thread.currentThread()) {
            close00();
        } else {
            //todo: return
            reactor.offer(new Runnable() {
                @Override
                public void run() {
                    close00();
                }
            });
        }
    }

    // guaranteed to run from the eventloop thread.
    private void close00() {
        if (started) {
            if (linuxSocket.trySetClosed()) {
                handler.prepareShutdown();
            }
        } else {
            linuxSocket.close();
        }
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress address) {
        checkNotNull(address, "address");

        if (logger.isFineEnabled()) {
            logger.fine("Connect to address:" + address);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        // todo: return value
        reactor.offer(new Runnable() {
            @Override
            public void run() {
                handler.prepareConnect(future, address);
            }
        });

        return future;
    }

    @SuppressWarnings({"checkstyle:MemberName", "checkstyle:ExecutableStatementCount"})
    static final class Handler {

        private final Eventloop eventloop;
        private final LinuxSocket linuxSocket;
        private final AtomicReference<Thread> flushThread;
        private final SubmissionQueue submissionQueue;
        private final CompletionQueue completionQueue;
        private final IOVector ioVector;
        private final Writer writer;
        private final Queue writeQueue;
        private final UringAsyncSocket socket;
        private final Metrics metrics;
        private final NetworkScheduler networkScheduler;
        private final ByteBuffer sndByteBuffer;
        private final IOBuffer sndBuff;
        private boolean writerClean = true;
        private final ByteBuffer rcvBuff;
        private final long rcvBuffAddr;
        private final Reader reader;
        private boolean closing;
        private int pending;
        private CompletableFuture<Void> connectFuture;
        private ByteBuffer addressBuffer;
        private long addressPtr;
        int handlerIndex;

        Handler(UringAsyncSocket.Builder builder, UringAsyncSocket socket) {
            this.socket = socket;
            this.eventloop = socket.reactor.eventloop();
            this.flushThread = socket.flushThread;
            this.submissionQueue = builder.uring.submissionQueue();
            this.completionQueue = builder.uring.completionQueue();
            this.ioVector = builder.ioVector;
            this.linuxSocket = builder.linuxSocket;
            this.writer = builder.writer;
            this.writeQueue = builder.writeQueue;
            this.networkScheduler = builder.networkScheduler;
            this.metrics = builder.metrics;
            this.reader = builder.reader;
            this.rcvBuff = ByteBuffer.allocateDirect(builder.options.get(SO_RCVBUF));
            this.rcvBuffAddr = addressOf(rcvBuff);

            if (writer != null) {
                try {
                    // should go through an allocator
                    // if it goes through an allocator we need to ensure that
                    // the iovector doesn't release it
                    this.sndBuff = new IOBuffer(builder.linuxSocket.getSendBufferSize(), true);
                } catch (IOException e) {
                    // todo: we need to deal with closing the underling socket.
                    throw new UncheckedIOException(e);
                }
                this.sndByteBuffer = sndBuff.byteBuffer();
            } else {
                this.sndBuff = null;
                this.sndByteBuffer = null;
            }
        }

        private void prepareConnect(CompletableFuture<Void> future, SocketAddress address) {
            try {
                if (closing) {
                    future.completeExceptionally(
                            new IOException("Failed to connect to " + address + " because socket is closed."));
                    return;
                }

                this.connectFuture = future;

                addressBuffer = ByteBuffer.allocateDirect(SIZEOF_SOCKADDR_STORAGE);
                addressBuffer.order(ByteOrder.nativeOrder());
                addressPtr = addressOf(addressBuffer);

                memsetSocketAddrIn((InetSocketAddress) address, addressPtr);

                long userdata = encodeUserdata(TYPE_SOCKET, IORING_OP_CONNECT, handlerIndex);
                submissionQueue.prepareConnect(linuxSocket.fd(), addressPtr, SIZEOF_SOCKADDR_STORAGE, userdata);
                pending++;
            } catch (Exception e) {
                socket.close("Closing socket due to connect problem.", e);
            }
        }

        private void completeConnect(int res) {
            if (res == 0) {
                socket.remoteAddress = linuxSocket.getRemoteAddress();
                socket.localAddress = linuxSocket.getLocalAddress();

                if (socket.logger.isInfoEnabled()) {
                    socket.logger.info("Connected from " + socket.localAddress + "->" + socket.remoteAddress);
                }

                prepareRead();
                connectFuture.complete(null);
            } else if (res == -ECONNREFUSED) {
                connectFuture.completeExceptionally(new IOException("Connection refused."));
            } else {
                UncheckedIOException e = newCQEFailedException(
                        "Failed to connect.", "connect(2)", IORING_OP_CONNECT, -res);
                connectFuture.completeExceptionally(e);
                throw e;
            }
        }

        private void prepareRead() {
            try {
                if (closing) {
                    return;
                }

                int pos = rcvBuff.position();
                long address = rcvBuffAddr + pos;
                int length = rcvBuff.remaining();
                if (length == 0) {
                    throw new IllegalStateException(
                            "Can't call read when there is no space in the rcvBuff.");
                }

                long userdata = encodeUserdata(TYPE_SOCKET, IORING_OP_RECV, handlerIndex);
                submissionQueue.prepareRecv(linuxSocket.fd(), address, length, userdata);
                pending++;
            } catch (Exception e) {
                socket.close("Closing socket due to read problem.", e);
            }
        }

        private void completeRead(int res) {
            if (res > 0) {
                int bytesRead = res;
                LAST_READ_TIME_NANOS.setOpaque(socket, eventloop.taskStartNanos());
                metrics.incReads();
                metrics.incBytesRead(bytesRead);
                //System.out.println(socket + " bytes read:" + res);
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
                prepareRead();
            } else if (res == 0) {
                System.out.println("Socket closed by peer");
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
        }

        void prepareWrite() {
            try {
                if (closing) {
                    return;
                }

                if (flushThread.get() == null) {
                    throw new IllegalStateException("Channel should be in scheduled state");
                }

                if (writer == null) {
                    ioVector.populate(writeQueue);
                } else {
                    writerClean = writer.onWrite(sndByteBuffer);
                    sndBuff.flip();
                    // add it if isn't added already
                    ioVector.offer(sndBuff);
                }

                if (ioVector.cnt() == 1) {
                    // There is just one item in the ioVecArray, so instead of doing a vectorized write,
                    // we do a regular write.
                    ByteBuffer buffer = ioVector.get(0).byteBuffer();
                    long addr = addressOf(buffer) + buffer.position();
                    int length = buffer.remaining();
                    long userdata = encodeUserdata(TYPE_SOCKET, IORING_OP_SEND, handlerIndex);
                    submissionQueue.prepareSend(linuxSocket.fd(), addr, length, userdata);
                } else {
                    long userdata = encodeUserdata(TYPE_SOCKET, IORING_OP_WRITEV, handlerIndex);
                    submissionQueue.prepareWritev(linuxSocket.fd(), ioVector.addr(), ioVector.cnt(), userdata);
                }
                pending++;
            } catch (Exception e) {
                socket.close("Closing socket due to write problem.", e);
            }
        }

        private void completeWrite(int res) {
            if (res >= 0) {
                metrics.incBytesWritten(res);
                metrics.incWrites();
                //System.out.println(socket + " written " + res);

                boolean sndBufferClean = true;
                if (sndBuff != null) {
                    ioVector.clear();
                    sndBuff.position(sndBuff.position() + res);
                    sndBufferClean = !sndBuff.byteBuffer().hasRemaining();
                    compactOrClear(sndBuff.byteBuffer());
                } else {
                    ioVector.compact(res);
                }

                boolean clean = writerClean
                        && sndBufferClean
                        && ioVector.isEmpty()
                        && writeQueue.isEmpty();
                if (clean) {
                    socket.resetFlushed();
                } else {
                    networkScheduler.schedule(socket);
                }
            } else if (res == -EAGAIN) {
                // try again.
                prepareWrite();
            } else {
                if (ioVector.cnt() == 1) {
                    throw newCQEFailedException(
                            "Failed to write data to the socket.", "send(2)", IORING_OP_SEND, -res);
                } else {
                    throw newCQEFailedException(
                            "Failed to write data to the socket.", "writev(3p)", IORING_OP_WRITEV, -res);
                }
            }
        }

        private void prepareClose() {
            try {
                if (!closing) {
                    throw new IllegalArgumentException("Handler should be in closing state");
                }

                long userdata = encodeUserdata(TYPE_SOCKET, IORING_OP_CLOSE, handlerIndex);
                submissionQueue.prepareClose(linuxSocket.fd(), userdata);

                pending++;
            } catch (Exception e) {
                // todo: better message
                socket.close("Failed to close the socket", e);
            }
        }

        private void completeClose(int res) {
            if (res != 0) {
                System.out.println("Closing of socket failed with " + Linux.strerror(-res));

                // todo: won't do anything because the close flag is already set.
                linuxSocket.close();
            }
        }

        /**
         * To close a socket, first a shutdown is needed. This will start with the
         * FIN/RST on the socket and will flush out any call on the socket that
         * are pending with an error. Without the shutdown, but calling a close directly,
         * then pending calls will not terminate. Because they keep a ref counter on
         * the socket incremented, the socket will not close and no FIN/RST message
         * is send.
         */
        private void prepareShutdown() {
            try {
                if (closing) {
                    return;
                }
                closing = true;

                long userdata = encodeUserdata(TYPE_SOCKET, IORING_OP_SHUTDOWN, handlerIndex);
                submissionQueue.prepareShutdown(linuxSocket.fd(), userdata);
                pending++;
            } catch (Exception e) {
                // todo: won't do anything because the close flag is already set.
                socket.close("Failed to close the socket in an orderly fashion.", e);
            }
        }

        private void completeShutdown(int res) {
            if (res != 0) {
                System.out.println("Shutdown of socket failed with " + Linux.strerror(-res));
                // todo: won't do anything because of close flag already set.
                linuxSocket.close();
            }

            prepareClose();
        }

        public void complete(byte opcode, int res) {
            //System.out.println(socket + " completing " + opcodeToString(opcode) + " res:" + res);

            try {
                switch (opcode) {
                    case IORING_OP_SEND:
                        completeWrite(res);
                        break;
                    case IORING_OP_WRITEV:
                        completeWrite(res);
                        break;
                    case IORING_OP_RECV:
                        completeRead(res);
                        break;
                    case IORING_OP_CLOSE:
                        completeClose(res);
                        break;
                    case IORING_OP_SHUTDOWN:
                        completeShutdown(res);
                        break;
                    case IORING_OP_CONNECT:
                        completeConnect(res);
                        break;
                    default:
                        throw new IllegalArgumentException("Unhandled opcode:" + opcodeToString(opcode));
                }

                pending--;

                if (closing && pending == 0) {
                    ((UringNetworkScheduler) socket.networkScheduler).unregister(this);
                    socket.reactor.sockets().remove(socket);
                }
            } catch (Exception e) {
                e.printStackTrace();

                //todo: error message
                socket.close("Closing socket due to write problem.", e);
            }
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity",
            "checkstyle:returncount",
            "checkstyle:SimplifyBooleanReturn"})
    public static class UringOptions implements Options {

        private final LinuxSocket nativeSocket;

        UringOptions(LinuxSocket nativeSocket) {
            this.nativeSocket = nativeSocket;
        }

        @Override
        public boolean isSupported(Option option) {
            if (TCP_QUICKACK.equals(option)) {
                return true;
            } else if (TCP_NODELAY.equals(option)) {
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
                } else if (TCP_QUICKACK.equals(option)) {
                    return (T) (Boolean) nativeSocket.isTcpQuickAck();
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
                } else if (TCP_QUICKACK.equals(option)) {
                    nativeSocket.setTcpQuickAck((Boolean) value);
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
     * A {@link UringAsyncSocket} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends AsyncSocket.Builder {

        public LinuxSocket linuxSocket;
        public IOVector ioVector;
        public Uring uring;

        Builder(UringAsyncServerSocket.AcceptRequest acceptRequest) {
            if (acceptRequest == null) {
                this.linuxSocket = LinuxSocket.createNonBlockingTcpIpv4Socket();
                this.clientSide = true;
            } else {
                this.linuxSocket = acceptRequest.linuxSocket;
                this.clientSide = false;
            }
            this.options = new UringOptions(linuxSocket);
        }

        @Override
        public void close() throws Exception {
            closeQuietly(linuxSocket);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(linuxSocket, "nativeSocket");
            checkNotNull(uring, "uring");

            if (linuxSocket.isBlocking()) {
                throw new IllegalArgumentException("linuxSocket can't be blocking");
            }

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
                return new UringAsyncSocket(this);
            } else {
                return reactor.submit(() -> new UringAsyncSocket(Builder.this)).join();
            }
        }
    }
}
