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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.AsyncSocketOptions;
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.util.CircularQueue;
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

import static com.hazelcast.internal.tpc.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpc.util.BufferUtil.upcast;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * Nio implementation of the {@link AsyncSocket}.
 */
@SuppressWarnings({"checkstyle:DeclarationOrder", "checkstyle:VisibilityOrder", "checkstyle:MethodCount", "java:S1181"})
public final class NioAsyncSocket extends AsyncSocket {

    private final NioAsyncSocketOptions options;
    private final AtomicReference<Thread> flushThread = new AtomicReference<>();
    private final MpmcArrayQueue<IOBuffer> unflushedBufs;
    private final Handler handler;
    private final SocketChannel socketChannel;
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private final IOVector ioVector = new IOVector();
    private final boolean regularSchedule;
    private final boolean writeThrough;
    private final ReadHandler readHandler;
    private final CircularQueue localTaskQueue;

    // only accessed from eventloop thread
    private boolean started;
    // only accessed from eventloop thread
    private boolean connect;
    private CompletableFuture<Void> connectFuture;

    NioAsyncSocket(NioAsyncSocketBuilder builder) {
        super(builder.clientSide);

        assert Thread.currentThread() == builder.reactor.eventloopThread();

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
            this.unflushedBufs = new MpmcArrayQueue<>(builder.unflushedBufsCapacity);
            this.handler = new Handler(builder);
            this.key = socketChannel.register(reactor.selector, 0, handler);
            this.readHandler = builder.readHandler;
            readHandler.init(this);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public AsyncSocketOptions options() {
        return options;
    }

    /**
     * Returns the SelectionKey. Only for testing purposes.
     *
     * @return the selection key.
     */
    public SelectionKey key() {
        return key;
    }

    @Override
    public NioReactor reactor() {
        return reactor;
    }

    /**
     * Returns the underlying SocketChannel. Only for testing purposes.
     *
     * @return the SocketChannel.
     */
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public void setReadable(boolean readable) {
        if (Thread.currentThread() == eventloopThread) {
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
            key.interestOps(key.interestOps() | OP_READ);
        } else {
            key.interestOps(key.interestOps() & ~OP_READ);
        }
    }

    @Override
    public boolean isReadable() {
        if (Thread.currentThread() == eventloopThread) {
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
            // on the server side we immediately start reading.
            key.interestOps(key.interestOps() | OP_READ);
        }
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress address) {
        checkNotNull(address, "address");

        if (logger.isInfoEnabled()) {
            logger.info("Connecting to address:" + address);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        if (Thread.currentThread() == eventloopThread) {
            connect0(address, future);
        } else {
            if (reactor == null) {
                throw new IllegalStateException("Can't connect, NioAsyncSocket isn't activated.");
            }

            reactor.execute(() -> connect0(address, future));
        }

        return future;
    }

    private void connect0(SocketAddress address, CompletableFuture<Void> future) {
        try {
            if (!started) {
                throw new IllegalStateException(this + " can't connect when socket not yet started");
            }

            if (connect) {
                throw new IllegalStateException(this + " is already trying to connect");
            }
            connect = true;
            connectFuture = future;
            key.interestOps(key.interestOps() | OP_CONNECT);
            socketChannel.connect(address);
        } catch (Throwable e) {
            future.completeExceptionally(e);
            throw sneakyThrow(e);
        }
    }

    @SuppressWarnings("java:S1135")
    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
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

        if (!unflushedBufs.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                reactor.offer(handler);
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
        boolean result = write(buf);
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
                localTaskQueue.add(handler);
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
        closeQuietly(socketChannel);
        key.cancel();
        super.close0();
    }

    @SuppressWarnings("java:S125")
    private final class Handler implements NioHandler, Runnable {
        private final ByteBuffer receiveBuffer;

        private Handler(NioAsyncSocketBuilder builder) throws SocketException {
            int receiveBufferSize = builder.socketChannel.socket().getReceiveBufferSize();
            this.receiveBuffer = builder.receiveBufferIsDirect
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
            readEvents.inc();

            int read = socketChannel.read(receiveBuffer);
            //System.out.println(NioAsyncSocket.this + " bytes read: " + read);

            if (read == -1) {
                throw new EOFException("Remote socket closed!");
            } else {
                bytesRead.inc(read);
                upcast(receiveBuffer).flip();
                readHandler.onRead(receiveBuffer);
                compactOrClear(receiveBuffer);
            }
        }

        private void handleWrite() throws IOException {
            assert flushThread.get() != null;

            writeEvents.inc();

            ioVector.populate(unflushedBufs);

            long written = ioVector.write(socketChannel);

            bytesWritten.inc(written);
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

        private void handleConnect() {
            try {
                socketChannel.finishConnect();
                remoteAddress = socketChannel.getRemoteAddress();
                localAddress = socketChannel.getLocalAddress();
                if (logger.isInfoEnabled()) {
                    logger.info("Connection established " + NioAsyncSocket.this);
                }

                key.interestOps(key.interestOps() | OP_READ);
                connectFuture.complete(null);
            } catch (Throwable e) {
                connectFuture.completeExceptionally(e);
                throw sneakyThrow(e);
            } finally {
                connectFuture = null;
            }
        }
    }
}
