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
import com.hazelcast.internal.tpcengine.net.AsyncSocketMetrics;
import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
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
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

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
    private final MpmcArrayQueue<IOBuffer> writeQueue;
    private final Handler handler;
    private final SocketChannel socketChannel;
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private final IOVector ioVector = new IOVector();
    private final boolean regularSchedule;
    private final boolean writeThrough;
    private final AsyncSocketReader reader;
    private final CircularQueue localTaskQueue;

    // only accessed from eventloop thread
    private boolean started;
    // only accessed from eventloop thread
    private boolean connecting;
    private volatile CompletableFuture<Void> connectFuture;

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
            this.writeThrough = builder.writeThrough;
            this.regularSchedule = builder.regularSchedule;
            this.writeQueue = new MpmcArrayQueue<>(builder.writeQueueCapacity);
            this.handler = new Handler(builder);
            this.key = socketChannel.register(reactor.selector, 0, handler);
            this.reader = builder.reader;
            reader.init(this);
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

        key.interestOps(key.interestOps() | OP_READ);
        connectFuture.complete(null);
        connectFuture = null;

        // From this point on, the socket is willing to send data.
        resetFlushed();
    }

    @SuppressWarnings("java:S1135")
    @Override
    public void flush() {
        Thread currentThread = currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                localTaskQueue.add(handler);
            } else if (writeThrough) {
                handler.run();
            } else if (regularSchedule) {
                // todo: return value
                reactor.offer(handler);
            } else {
                key.interestOps(key.interestOps() | OP_WRITE);
                // we need to call the select wakeup because the interest set will only take
                // effect after a select operation.
                reactor.wakeup();
            }
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
    public boolean write(IOBuffer buf) {
        return writeQueue.add(buf);
    }

    @Override
    public boolean writeAll(Collection<IOBuffer> bufs) {
        return writeQueue.addAll(bufs);
    }

    @Override
    public boolean writeAndFlush(IOBuffer buf) {
        boolean result = write(buf);
        flush();
        return result;
    }

    @Override
    public boolean unsafeWriteAndFlush(IOBuffer buf) {
        Thread currentFlushThread = flushThread.get();
        Thread currentThread = currentThread();

        assert currentThread == eventloopThread;

        boolean result;
        if (currentFlushThread == null) {
            if (flushThread.compareAndSet(null, currentThread)) {
                localTaskQueue.add(handler);
                if (ioVector.offer(buf)) {
                    result = true;
                } else {
                    result = writeQueue.offer(buf);
                }
            } else {
                result = writeQueue.offer(buf);
            }
        } else if (currentFlushThread == eventloopThread) {
            if (ioVector.offer(buf)) {
                result = true;
            } else {
                result = writeQueue.offer(buf);
            }
        } else {
            result = writeQueue.offer(buf);
            flush();
        }
        return result;
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

        private Handler(NioAsyncSocketBuilder builder) throws SocketException {
            int receiveBufferSize = builder.socketChannel.socket().getReceiveBufferSize();
            this.rcvBuffer = builder.directBuffers
                    ? ByteBuffer.allocateDirect(receiveBufferSize)
                    : ByteBuffer.allocate(receiveBufferSize);
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

        private void handleWrite() throws IOException {
            // typically this method is called with the flushThread being set.
            // but in case of cancellation of the key, this method is also
            // called without the flushThread being set.
            // So we can't do an assert flushThread!=null.

            metrics.incWriteEvents();

            ioVector.populate(writeQueue);

            ByteBuffer[] srcs = ioVector.array();
            int length = ioVector.length();
            long written = length == 1
                    ? socketChannel.write(srcs[0])
                    : socketChannel.write(srcs, 0, length);

            ioVector.compact(written);

            metrics.incBytesWritten(written);
            //System.out.println(NioAsyncSocket.this + " bytes written:" + written);

            if (ioVector.isEmpty()) {
                // everything got written
                int interestOps = key.interestOps();

                // clear the OP_WRITE flag if it was set
                if ((interestOps & OP_WRITE) != 0) {
                    key.interestOps(interestOps & ~OP_WRITE);
                }

                resetFlushed();
            } else {
                // We need to register for the OP_WRITE because not everything got written
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
