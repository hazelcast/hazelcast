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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.util.Option;
import jdk.net.ExtendedSocketOptions;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpcengine.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNull;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * Nio implementation of the {@link AsyncSocket}.
 */
public final class NioAsyncSocket extends AsyncSocket {

    final Handler handler;
    private final SocketChannel socketChannel;
    private final SelectionKey key;
    private final IOVector ioVector;
    private boolean connecting;
    private volatile CompletableFuture<Void> connectFuture;
    /**
     * This flag only has meaning within the eventloop thread.
     * <p>
     * When the eventloop thread writes a packet using the unsafe write, we want
     * to prevent writing that packet to the expensive writeQueue. We want to
     * write the packet to the IOVector instead if there is space. The problem
     * is that without additional care it can lead to reordering of packets.
     * <p>
     * So imagine the IOVector was full and as a consequence packet P1 is written to the
     * writeQueue. And write to the socket completes and some space is freed up in the
     * IOVector. If there would be write of packet P2, it can be placed in the
     * IOVector, since there is space. But now we have an ordering problem because P2
     * overtakes P1.
     * <p>
     * This flag will prevent that. So as long as no packets have been written to the
     * writeQueue, you can keep writing to the IOVector directly. But once a write
     * has been made to the writeQueue, ioVectorWriteAllowed is to to false and
     * all further writes need to be done to the writeQueue until the writeQueue has
     * been fully drained. Once it is drained, the ioVectorWriteAllowed is set to true
     * again and packets can be written directly to the IOVector.
     * <p>
     * For any packet send by a thread different than the eventloop thread, the writeQueue
     * needs to be used. Ordering of packets send by other threads and the eventloop thread
     * isn't an issue since they are concurrent and any order goes.
     * <p>
     * What might be an issue is starvation when the eventloop thread keeps filling up the
     * IOVector and the writeQueue filled by other threads isn't drained.
     */
    private boolean ioVectorWriteAllowed;

    private NioAsyncSocket(Builder builder) throws IOException {
        super(builder);

        this.socketChannel = builder.socketChannel;
        if (!clientSide) {
            this.localAddress = socketChannel.getLocalAddress();
            this.remoteAddress = socketChannel.getRemoteAddress();
        }
        this.ioVector = builder.ioVector;
        this.ioVectorWriteAllowed = ioVector != null;
        this.handler = new Handler(builder, this);
        this.key = handler.key;
        reader.init(this);
        if (writer != null) {
            writer.init(this);
        }
    }

    @Override
    public void setReadable(boolean readable) {
        if (currentThread() == eventloopThread) {
            setReadable0(readable);
        } else {
            reactor.submit(() -> setReadable0(readable)).join();
        }
    }

    private void setReadable0(boolean readable) {
        if (readable) {
            // Signal that we are interested in OP_READ events.
            key.interestOpsOr(OP_READ);
        } else {
            // Signal that we are not interesting in OP_READ events.
            // So even if data is received or still available on the socket,
            // we will not get further events.
            key.interestOpsAnd(~OP_READ);
        }

        // todo: doesn't make sense since we are on the eventloop
        // We are not running on the eventloop thread. We need to notify the
        // reactor because a change in the interest set isn't picked up while
        // the reactor is waiting on the selectionKey
        reactor.wakeup();
    }

    @Override
    public boolean isReadable() {
        return (key.interestOps() & OP_READ) != 0;
    }

    @Override
    protected void start00() {
        if (!clientSide) {
            // on the server side we immediately start reading.
            key.interestOps(key.interestOps() | OP_READ);
            // and on the server side we can immediately start sending
            resetFlushed();
        }
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress address) {
        checkNotNull(address, "address");

        if (logger.isInfoEnabled()) {
            logger.info("Connecting to address:" + address);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        if (currentThread() == eventloopThread) {
            connect0(address, future);
        } else {
            reactor.execute(() -> connect0(address, future));
        }

        return future;
    }

    private void connect0(SocketAddress address, CompletableFuture<Void> future) {
        try {
            if (!started) {
                throw new IllegalStateException(this + " can't connect when socket not yet started");
            }

            if (connecting) {
                throw new IllegalStateException(this + " is already trying to connect");
            }

            assert flushThread.get() == reactor.eventloopThread();

            connecting = true;
            connectFuture = future;
            key.interestOps(key.interestOps() | OP_CONNECT);
            if (socketChannel.connect(address)) {
                // We got lucky, the connection was immediately established which can
                // happen with local connections.
                onConnectFinished();
            }
        } catch (Throwable e) {
            future.completeExceptionally(e);
            throw sneakyThrow(e);
        }
    }

    private void onConnectFinished() throws IOException {
        assert connecting;
        assert flushThread.get() == reactor.eventloopThread();

        remoteAddress = socketChannel.getRemoteAddress();
        localAddress = socketChannel.getLocalAddress();
        if (logger.isInfoEnabled()) {
            logger.info("Connection established " + NioAsyncSocket.this);
        }

        key.interestOps(key.interestOps() | OP_READ);
        connectFuture.complete(null);
        connectFuture = null;

        // From this point on, the socket is willing to send data.
        resetFlushed();
    }

    @Override
    protected boolean insideWrite(Object msg) {
        if (ioVectorWriteAllowed) {
            if (ioVector.offer((IOBuffer) msg)) {
                return true;
            } else {
                ioVectorWriteAllowed = false;
                return writeQueue.offer(msg);
            }
        } else {
            return writeQueue.offer(msg);
        }
    }

    @Override
    protected void close0() throws IOException {
        super.close0();

        closeQuietly(socketChannel);
        key.cancel();
        reactor.sockets().remove(this);
    }

    @SuppressWarnings("java:S125")
    static final class Handler implements NioHandler, Runnable {
        // Todo: the way the rcvBuffer is used isn't memory efficient because every socket
        // will get its own buffer. If the rcvBuffer is guaranteed to be drained, a single
        // rcvBuffer can be shared between all sockets from the same reactor. So you get a
        // significant memory saving and it will also help with improved cache utilization
        // etc.
        private final ByteBuffer rcvBuffer;
        // Same goes for sndBuffer. Having dedicated sndBuffer per socket is expensive.
        private final ByteBuffer sndBuffer;
        private final Metrics metrics;
        private final SocketChannel socketChannel;
        private final NioEventloop eventloop;
        private final IOVector ioVector;
        private final SelectionKey key;
        private final Reader reader;
        private final Writer writer;
        private final Queue writeQueue;
        private final NioAsyncSocket socket;

        private Handler(Builder builder, NioAsyncSocket socket) throws IOException {
            this.socket = socket;
            this.metrics = socket.metrics();
            this.socketChannel = socket.socketChannel;
            this.eventloop = (NioEventloop) socket.eventloop;
            this.ioVector = socket.ioVector;
            this.reader = socket.reader;
            this.writer = socket.writer;
            this.writeQueue = socket.writeQueue;
            int rcvBufferSize = builder.socketChannel.socket().getReceiveBufferSize();
            this.rcvBuffer = builder.receiveBufferIsDirect
                    ? ByteBuffer.allocateDirect(rcvBufferSize)
                    : ByteBuffer.allocate(rcvBufferSize);

            if (ioVector == null) {
                int sndBufferSize = builder.socketChannel.socket().getSendBufferSize();
                this.sndBuffer = builder.receiveBufferIsDirect
                        ? ByteBuffer.allocateDirect(sndBufferSize)
                        : ByteBuffer.allocate(sndBufferSize);
            } else {
                this.sndBuffer = null;
            }
            this.key = socketChannel.register(builder.selector, 0, this);
        }

        @Override
        public void run() {
            try {
                handleWrite();
            } catch (Throwable cause) {
                close(null, cause);

                if (!(cause instanceof Exception)) {
                    // Anything that isn't an exception needs to be propagated.
                    throw sneakyThrow(cause);
                }
            }
        }

        @Override
        public void close(String reason, Throwable cause) {
            if (cause instanceof EOFException) {
                // The stacktrace of an EOFException isn't important. It just means that the
                // Exception is closed by the remote side.
                socket.close(reason != null ? reason : cause.getMessage(), null);
            } else {
                socket.close(reason, cause);
            }
        }

        @Override
        public void handle() throws IOException {
            if (!key.isValid()) {
                throw new CancelledKeyException();
            }

            int readyOps = key.readyOps();

            if ((readyOps & OP_READ) != 0) {
                handleRead();
            }

            if ((readyOps & OP_WRITE) != 0) {
                handleWrite();
            }

            if ((readyOps & OP_CONNECT) != 0) {
                handleConnect();
            }
        }

        private void handleRead() throws IOException {
            metrics.incReads();

            int read = socketChannel.read(rcvBuffer);
            // System.out.println(NioAsyncSocket.this + " bytes read: " + read);

            if (read == -1) {
                throw new EOFException("Socket closed by peer");
            }

            // todo: Need to revise.
            LAST_READ_TIME_NANOS.setOpaque(socket, eventloop.taskStartNanos());

            metrics.incBytesRead(read);

            rcvBuffer.flip();
            // todo: do we want to schedule this as a task or run it directly?
            // Because currently it is running as consequence of an I/O event
            // and not managed as a task.
            reader.onRead(rcvBuffer);
            compactOrClear(rcvBuffer);
        }

        // todo: temp notes.
        // In netty there is logic in the NioSocketChannel.doWrite
        // 1) all the collected messages are IOBUffers, do a writev.
        // 2) if there is only 1 message and it an IOBuffer, then do a write
        // 3) if at least one of the messages is a non IOBuffer, then fallback
        // to a normal write.
        private void handleWrite() throws IOException {
            metrics.incWrites();

            long bytesWritten;
            boolean clean;
            if (writer == null) {
                // the writeQueue is guaranteed to have only IOBuffers
                // if the writer isn't set.
                ioVector.populate(writeQueue);

                if (writeQueue.isEmpty()) {
                    socket.ioVectorWriteAllowed = true;
                }

                int ioVectorLength = ioVector.length();
                ByteBuffer[] srcs = ioVector.array();
                bytesWritten = ioVectorLength == 1
                        ? socketChannel.write(srcs[0])
                        : socketChannel.write(srcs, 0, ioVectorLength);
                ioVector.compact(bytesWritten);
                clean = ioVector.isEmpty();
            } else {
                boolean writerClean = writer.onWrite(sndBuffer);
                sndBuffer.flip();
                bytesWritten = socketChannel.write(sndBuffer);
                boolean sndBufferClean = !sndBuffer.hasRemaining();
                clean = writerClean && sndBufferClean;
                compactOrClear(sndBuffer);
            }

            metrics.incBytesWritten(bytesWritten);
            //System.out.println(socket + " bytes written:" + bytesWritten);

            if (clean) {
                // everything got written
                int interestOps = key.interestOps();

                // clear the OP_WRITE flag if it was set
                if ((interestOps & OP_WRITE) != 0) {
                    key.interestOps(interestOps & ~OP_WRITE);
                }

                socket.resetFlushed();
            } else {
                // not everything got written, therefor we need to register
                // for the OP_WRITE so that we get an event as soon as space
                // is available in the send buffer of the socket.
                key.interestOps(key.interestOps() | OP_WRITE);
            }
        }

        // Is called when side of the socket that initiates the connect
        // gets the event that the connection is completed.
        private void handleConnect() {
            try {
                assert socket.flushThread.get() != null;

                if (!socketChannel.finishConnect()) {
                    throw new IllegalStateException();
                }
                socket.onConnectFinished();
            } catch (Throwable e) {
                if (socket.connectFuture != null) {
                    socket.connectFuture.completeExceptionally(e);
                }
                throw sneakyThrow(e);
            }
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount", "java:S3776"})
    public static class NioOptions implements Options {

        private final SocketChannel socketChannel;

        NioOptions(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        private static SocketOption toSocketOption(Option option) {
            // Could be made more efficient by putting it in a static map.
            if (TCP_NODELAY.equals(option)) {
                return StandardSocketOptions.TCP_NODELAY;
            } else if (TCP_QUICKACK.equals(option)) {
                return ExtendedSocketOptions.TCP_QUICKACK;
            } else if (SO_RCVBUF.equals(option)) {
                return StandardSocketOptions.SO_RCVBUF;
            } else if (SO_SNDBUF.equals(option)) {
                return StandardSocketOptions.SO_SNDBUF;
            } else if (SO_KEEPALIVE.equals(option)) {
                return StandardSocketOptions.SO_KEEPALIVE;
            } else if (SO_REUSEADDR.equals(option)) {
                return StandardSocketOptions.SO_REUSEADDR;
            } else if (TCP_KEEPCOUNT.equals(option)) {
                return ExtendedSocketOptions.TCP_KEEPCOUNT;
            } else if (TCP_KEEPINTERVAL.equals(option)) {
                return ExtendedSocketOptions.TCP_KEEPINTERVAL;
            } else if (TCP_KEEPIDLE.equals(option)) {
                return ExtendedSocketOptions.TCP_KEEPIDLE;
            } else {
                return null;
            }
        }

        @Override
        public boolean isSupported(Option option) {
            checkNotNull(option, "option");

            return isSupported(toSocketOption(option));
        }

        private boolean isSupported(SocketOption socketOption) {
            return socketOption != null && socketChannel.supportedOptions().contains(socketOption);
        }

        @Override
        public <T> T get(Option<T> option) {
            checkNotNull(option, "option");

            try {
                SocketOption socketOption = toSocketOption(option);
                if (isSupported(socketOption)) {
                    return (T) socketChannel.getOption(socketOption);
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public <T> boolean set(Option<T> option, T value) {
            checkNotNull(option, "option");
            checkNotNull(value, "value");

            try {
                SocketOption socketOption = toSocketOption(option);
                if (isSupported(socketOption)) {
                    socketChannel.setOption(socketOption, value);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * A {@link NioAsyncSocket} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends AsyncSocket.Builder {
        public SocketChannel socketChannel;
        public Selector selector;
        public boolean receiveBufferIsDirect = true;
        public IOVector ioVector;

        public Builder(NioAsyncServerSocket.AcceptRequest acceptRequest) {
            try {
                if (acceptRequest == null) {
                    this.socketChannel = SocketChannel.open();
                    this.clientSide = true;
                } else {
                    this.socketChannel = acceptRequest.socketChannel;
                    this.clientSide = false;
                }
                this.socketChannel.configureBlocking(false);
                this.options = new NioOptions(socketChannel);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws Exception {
            closeQuietly(socketChannel);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(socketChannel, "socketChannel");
            checkNotNull(selector, "selector");

            if (writer == null) {
                if (ioVector == null) {
                    ioVector = new IOVector();
                }
            } else {
                checkNull(ioVector, "ioVector");
            }
        }

        @SuppressWarnings("java:S1181")
        @Override
        protected AsyncSocket construct() {
            if (currentThread() == reactor.eventloopThread()) {
                try {
                    return new NioAsyncSocket(Builder.this);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                return reactor.submit(() -> new NioAsyncSocket(Builder.this)).join();
            }
        }
    }
}
