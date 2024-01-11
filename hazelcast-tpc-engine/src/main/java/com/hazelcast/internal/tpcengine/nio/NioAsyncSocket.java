/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.tpcengine.net.AsyncSocketMetrics;
import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.internal.tpcengine.net.AsyncSocketWriter;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.allocateBuffer;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * Nio implementation of the {@link AsyncSocket}.
 */
@SuppressWarnings({"checkstyle:DeclarationOrder", "checkstyle:VisibilityOrder", "checkstyle:MethodCount", "java:S1181"})
public final class NioAsyncSocket extends AsyncSocket {

    private final NioAsyncSocketOptions options;
    private final AtomicReference<Thread> flushThread = new AtomicReference<>(currentThread());
    private final MpmcArrayQueue writeQueue;
    private final Handler handler;
    private final SocketChannel socketChannel;
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private final IOVector ioVector;
    private final AsyncSocketReader reader;
    private final CircularQueue localTaskQueue;
    private final AsyncSocketWriter writer;
    private boolean ioVectorWriteAllowed;

    // only accessed from eventloop thread
    private boolean started;
    // only accessed from eventloop thread
    private boolean connecting;
    private volatile CompletableFuture<Void> connectFuture;

    @SuppressWarnings("checkstyle:executablestatementcount")
    NioAsyncSocket(NioAsyncSocketBuilder builder) {
        super(builder.clientSide);

        assert currentThread() == builder.reactor.eventloopThread();

        try {
            this.reactor = builder.reactor;
            this.localTaskQueue = builder.reactor.eventloop().localTaskQueue;
            this.options = builder.options;
            this.eventloopThread = reactor.eventloopThread();
            this.socketChannel = builder.socketChannel;
            if (!clientSide) {
                this.localAddress = socketChannel.getLocalAddress();
                this.remoteAddress = socketChannel.getRemoteAddress();
            }
            this.writeQueue = new MpmcArrayQueue<>(builder.writeQueueCapacity);
            this.handler = new Handler(builder);
            this.key = socketChannel.register(reactor.selector, 0, handler);
            this.reader = builder.reader;
            reader.init(this);
            this.writer = builder.writer;
            if (writer != null) {
                this.writer.init(this, writeQueue);
                this.ioVector = null;
            } else {
                this.ioVector = new IOVector();
            }
            this.ioVectorWriteAllowed = ioVector != null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public AsyncSocketOptions options() {
        return options;
    }

    @Override
    public NioReactor reactor() {
        return reactor;
    }

    @Override
    public void setReadable(boolean readable) {
        if (currentThread() == eventloopThread) {
            setReadable0(readable);
        } else {
            CompletableFuture future = new CompletableFuture();
            reactor.execute(() -> {
                try {
                    setReadable0(readable);
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw sneakyThrow(t);
                }
            });

            future.join();
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

        // We are not running on the eventloop thread. We need to notify the
        // reactor because a change in the interest set isn't picked up while
        // the reactor is waiting on the selectionKey
        reactor.wakeup();
    }

    @Override
    public boolean isReadable() {
        if (currentThread() == eventloopThread) {
            return isReadable0();
        } else {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    future.complete(isReadable0());
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw sneakyThrow(t);
                }
            });

            return future.join();
        }
    }

    private boolean isReadable0() {
        return (key.interestOps() & OP_READ) != 0;
    }

    @Override
    public void start() {
        if (currentThread() == reactor.eventloopThread()) {
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

        assert flushThread.get() == reactor.eventloopThread();

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

        key.interestOps(OP_READ);

        connectFuture.complete(null);
        connectFuture = null;

        // From this point on, the socket is willing to send data.
        resetFlushed();
    }

    @SuppressWarnings("java:S1135")
    @Override
    public void flush() {
        Thread currentThread = currentThread();

        if (flushThread.get() != null) {
            // the socket is already flushed, we are done.
            return;
        }

        // The socket is not flushed, so we are going to try to flush it.
        if (!flushThread.compareAndSet(null, currentThread)) {
            // A different thread triggered a flush, we are done.
            return;
        }

        if (currentThread == eventloopThread) {
            localTaskQueue.add(handler);
        } else {
            reactor.offer(handler);
        }
    }

    @SuppressWarnings({"java:S3398", "java:S1066"})
    private void resetFlushed() {
        flushThread.set(null);

        if (!writeQueue.isEmpty()) {
            if (flushThread.compareAndSet(null, currentThread())) {
                reactor.offer(handler);
            }
        }
    }

    @Override
    public boolean write(Object msg) {
        checkNotNull(msg, "msg");

        if (writer == null && !(msg instanceof IOBuffer)) {
            throw new IllegalArgumentException("Message needs to be an IOBuffer if no writer is configured.");
        }

        if (writeQueue.add(msg)) {
            return true;
        } else {
            // lets trigger a flush since the writeQueue is full.
            flush();
            return false;
        }
    }

    @Override
    public boolean writeAndFlush(Object msg) {
        boolean result = write(msg);
        flush();
        return result;
    }

    @Override
    public boolean unsafeWriteAndFlush(Object msg) {
        checkNotNull(msg, "msg");

        if (writer == null && !(msg instanceof IOBuffer)) {
            throw new IllegalArgumentException(
                    "Only accepting IOBuffers if writer isn't set.");
        }

        Thread currentThread = currentThread();
        if (currentThread != eventloopThread) {
            throw new IllegalStateException(
                    "insideWriteAndFlush can only be made from eventloop thread, "
                            + "found " + currentThread);
        }

        boolean triggeredFlush;

        Thread currentFlushThread = flushThread.get();
        if (currentFlushThread == null) {
            // the socket isn't flushed, lets try to flush it.
            triggeredFlush = flushThread.compareAndSet(null, currentThread);
            // At this point we know for sure that the socket was flushed; either
            // by the current thread or by a different one.
        } else {
            // the socket was already flushed
            triggeredFlush = false;
        }

        boolean offered = unsafeWrite(msg);

        if (triggeredFlush && offered) {
            reactor.execute(handler);
        }

        return offered;
    }

    private boolean unsafeWrite(Object msg) {
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
        closeQuietly(socketChannel);
        key.cancel();
        super.close0();
    }

    @SuppressWarnings("java:S125")
    private final class Handler implements NioHandler, Runnable {
        private final ByteBuffer rcvBuffer;
        private final AsyncSocketMetrics metrics = NioAsyncSocket.this.metrics;
        private final ByteBuffer sndBuffer;

        private Handler(NioAsyncSocketBuilder builder) throws SocketException {
            this.rcvBuffer = allocateBuffer(builder.directBuffers, builder.options.get(SO_RCVBUF));

            if (ioVector == null) {
                this.sndBuffer = allocateBuffer(builder.directBuffers, builder.options.get(SO_SNDBUF));
            } else {
                this.sndBuffer = null;
            }
        }

        @Override
        public void run() {
            try {
                handleWrite();
            } catch (Throwable e) {
                close(null, e);
                throw sneakyThrow(e);
            }
        }

        @Override
        public void close(String reason, Throwable cause) {
            if (cause instanceof EOFException) {
                // The stacktrace of an EOFException isn't important. It just means that the
                // Exception is closed by the remote side.
                NioAsyncSocket.this.close(reason != null ? reason : cause.getMessage(), null);
            } else {
                NioAsyncSocket.this.close(reason, cause);
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
            metrics.incReadEvents();

            int read = socketChannel.read(rcvBuffer);
            //System.out.println(NioAsyncSocket.this + " bytes read: " + read);

            if (read == -1) {
                throw new EOFException("Socket closed by peer");
            }

            metrics.incBytesRead(read);
            rcvBuffer.flip();
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
            metrics.incWriteEvents();

            // todo: Netty has a write spin option we need to investigate.

            long bytesWritten;
            boolean clean;
            if (writer == null) {
                // the writeQueue is guaranteed to have only IOBuffers
                // if the writer isn't set.
                ioVector.populate(writeQueue);

                if (writeQueue.isEmpty()) {
                    ioVectorWriteAllowed = true;
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

                resetFlushed();
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
                assert flushThread.get() != null;

                if (!socketChannel.finishConnect()) {
                    throw new IllegalStateException();
                }
                onConnectFinished();
            } catch (Throwable e) {
                if (connectFuture != null) {
                    connectFuture.completeExceptionally(e);
                }
                throw sneakyThrow(e);
            }
        }
    }
}
