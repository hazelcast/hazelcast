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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import jdk.net.ExtendedSocketOptions;
import org.jctools.queues.MpmcArrayQueue;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpc.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;
import static com.hazelcast.internal.tpc.util.ReflectionUtil.findStaticFieldValue;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * Nio implementation of the {@link AsyncSocket}.
 */
@SuppressWarnings({"checkstyle:DeclarationOrder", "checkstyle:VisibilityOrder", "checkstyle:MethodCount"})
public final class NioAsyncSocket extends AsyncSocket {

    private static final int DEFAULT_UNFLUSHED_BUFS_CAPACITY = 2 << 16;

    private static final SocketOption<Integer> TCP_KEEPCOUNT
            = findStaticFieldValue(ExtendedSocketOptions.class, "TCP_KEEPCOUNT");
    private static final SocketOption<Integer> TCP_KEEPIDLE
            = findStaticFieldValue(ExtendedSocketOptions.class, "TCP_KEEPIDLE");
    private static final SocketOption<Integer> TCP_KEEPINTERVAL
            = findStaticFieldValue(ExtendedSocketOptions.class, "TCP_KEEPINTERVAL");

    private static final AtomicBoolean TCP_KEEPCOUNT_PRINTED = new AtomicBoolean();
    private static final AtomicBoolean TCP_KEEPIDLE_PRINTED = new AtomicBoolean();
    private static final AtomicBoolean TCP_KEEPINTERVAL_PRINTED = new AtomicBoolean();

    private int unflushedBufsCapacity = DEFAULT_UNFLUSHED_BUFS_CAPACITY;

    // concurrent
    private final AtomicReference<Thread> flushThread = new AtomicReference<>();
    private MpmcArrayQueue<IOBuffer> unflushedBufs;
    private final NioAsyncSocketHandler handler = new NioAsyncSocketHandler();
    private CompletableFuture<Void> connectFuture;

    // immutable state
    private final SocketChannel socketChannel;
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private final Selector selector;


    // ======================================================
    // reading side of the socket.
    // ======================================================
    private ByteBuffer receiveBuffer;

    // ======================================================
    // writing side of the socket.
    // ======================================================
    private final IOVector ioVector = new IOVector();
    private boolean regularSchedule = true;
    private boolean writeThrough;
    private boolean started;
    private boolean connect;

    public NioAsyncSocket(NioReactor reactor) {
        try {
            this.reactor = reactor;
            this.eventloopThread = reactor.eventloopThread();
            this.selector = reactor.selector;
            this.socketChannel = SocketChannel.open();
            this.socketChannel.configureBlocking(false);
            this.clientSide = true;
            this.key = socketChannel.register(selector, 0, handler);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public NioAsyncSocket(NioReactor reactor, NioAcceptRequest openRequest) {
        try {
            this.reactor = reactor;
            this.eventloopThread = reactor.eventloopThread();
            this.selector = reactor.selector;
            this.socketChannel = openRequest.getSocketChannel();
            this.socketChannel.configureBlocking(false);
            this.localAddress = socketChannel.getLocalAddress();
            this.remoteAddress = socketChannel.getRemoteAddress();
            this.clientSide = false;
            this.key = socketChannel.register(selector, 0, handler);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the SelectionKey. This method only exists for testing/debugging purposes.
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

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    public void setUnflushedBufsCapacity(int unflushedBufsCapacity) {
        this.unflushedBufsCapacity = checkPositive(unflushedBufsCapacity, "unflushedBufsCapacity");
    }

    public void setRegularSchedule(boolean regularSchedule) {
        this.regularSchedule = regularSchedule;
    }

    public void setWriteThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
    }

    @Override
    public void setTcpKeepAliveTime(int keepAliveTime) {
        checkPositive(keepAliveTime, "keepAliveTime");

        if (TCP_KEEPIDLE == null) {
            if (TCP_KEEPIDLE_PRINTED.compareAndSet(false, true)) {
                logger.warning("Ignoring NioAsyncSocket.setTcpKeepAliveTime. "
                        + "Please upgrade to Java 11+ or configure tcp_keepalive_time in the kernel. "
                        + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                        + "If this isn't dealt with, idle connections could be closed prematurely.");
            }
        } else {
            try {
                socketChannel.setOption(TCP_KEEPIDLE, keepAliveTime);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public int getTcpKeepAliveTime() {
        if (TCP_KEEPIDLE == null) {
            return 0;
        } else {
            try {
                return socketChannel.getOption(TCP_KEEPIDLE);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void setTcpKeepaliveIntvl(int keepaliveIntvl) {
        checkPositive(keepaliveIntvl, "keepaliveIntvl");

        if (TCP_KEEPINTERVAL == null) {
            if (TCP_KEEPINTERVAL_PRINTED.compareAndSet(false, true)) {
                logger.warning("Ignoring NioAsyncSocket.setTcpKeepaliveIntvl. "
                        + "Please upgrade to Java 11+ or configure tcp_keepalive_intvl in the kernel. "
                        + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                        + "If this isn't dealt with, idle connections could be closed prematurely.");
            }
        } else {
            try {
                socketChannel.setOption(TCP_KEEPINTERVAL, keepaliveIntvl);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public int getTcpKeepaliveIntvl() {
        if (TCP_KEEPINTERVAL == null) {
            return 0;
        } else {
            try {
                return socketChannel.getOption(TCP_KEEPINTERVAL);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void setTcpKeepAliveProbes(int keepAliveProbes) {
        checkPositive(keepAliveProbes, "keepAliveProbes");

        if (TCP_KEEPCOUNT == null) {
            if (TCP_KEEPCOUNT_PRINTED.compareAndSet(false, true)) {
                logger.warning("Ignoring NioAsyncSocket.setTcpKeepAliveProbes. "
                        + "Please upgrade to Java 11+ or configure tcp_keepalive_probes in the kernel: "
                        + "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. "
                        + "If this isn't dealt with, idle connections could be closed prematurely.");
            }
        } else {
            try {
                socketChannel.setOption(TCP_KEEPCOUNT, keepAliveProbes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public int getTcpKeepaliveProbes() {
        if (TCP_KEEPCOUNT == null) {
            return 0;
        } else {
            try {
                return socketChannel.getOption(TCP_KEEPCOUNT);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void setKeepAlive(boolean keepAlive) {
        try {
            socketChannel.setOption(SO_KEEPALIVE, keepAlive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isKeepAlive() {
        try {
            return socketChannel.getOption(SO_KEEPALIVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isTcpNoDelay() {
        try {
            return socketChannel.getOption(TCP_NODELAY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setTcpNoDelay(boolean tcpNoDelay) {
        try {
            socketChannel.setOption(TCP_NODELAY, tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        try {
            return socketChannel.getOption(SO_RCVBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReceiveBufferSize(int size) {
        try {
            socketChannel.setOption(SO_RCVBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getSendBufferSize() {
        try {
            return socketChannel.getOption(SO_SNDBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setSendBufferSize(int size) {
        try {
            socketChannel.setOption(SO_SNDBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        if (key == null) {
            throw new UncheckedIOException(new IOException(this + " has no key"));
        }

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
        if (key == null) {
            throw new UncheckedIOException(new IOException(this + " has no key"));
        }

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

        if (readHandler == null) {
            throw new IllegalStateException("Can't activate " + this + ": readhandler isn't set");
        }

        this.unflushedBufs = new MpmcArrayQueue<>(unflushedBufsCapacity);
        this.receiveBuffer = ByteBuffer.allocateDirect(getReceiveBufferSize());
        if (clientSide) {
            // on the client side we immediately start reading.
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
            if (connect) {
                throw new IllegalStateException(this + " is already trying to connect");
            }
            connect = true;

            connectFuture = future;
            key.interestOps(key.interestOps() | OP_CONNECT);
            socketChannel.connect(address);
        } catch (Throwable e) {
            connectFuture.completeExceptionally(e);
            throw sneakyThrow(e);
        }
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                reactor.localTaskQueue.add(handler);
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
                reactor.localTaskQueue.add(handler);
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

    private class NioAsyncSocketHandler implements NioHandler, Runnable {

        @Override
        public void run() {
            try {
                handleWrite();
            } catch (Throwable e) {
                close(null, e);
                sneakyThrow(e);
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
            //System.out.println(this + " bytes read: " + read);

            if (read == -1) {
                throw new EOFException("Remote socket closed!");
            } else {
                bytesRead.inc(read);
                receiveBuffer.flip();
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
            //System.out.println(this + " bytes written:" + written);

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
