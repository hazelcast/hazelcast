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
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import jdk.net.ExtendedSocketOptions;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpc.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpc.util.Preconditions.checkInstanceOf;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;
import static com.hazelcast.internal.tpc.util.ReflectionUtil.findStaticFieldValue;
import static com.hazelcast.internal.tpc.util.Util.closeResource;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_LINGER;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * Nio implementation of the {@link AsyncSocket}.
 */
@SuppressWarnings({"checkstyle:DeclarationOrder", "checkstyle:VisibilityOrder"})
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

    /**
     * Opens a NioAsyncSocket.
     * <p/>
     * To prevent coupling, it is better to use the {@link Eventloop#openAsyncSocket()}.
     *
     * @return the opened NioAsyncSocket.
     */
    public static NioAsyncSocket open() {
        return new NioAsyncSocket();
    }

    private int unflushedBufsCapacity = DEFAULT_UNFLUSHED_BUFS_CAPACITY;

    // concurrent
    private final AtomicReference<Thread> flushThread = new AtomicReference<>();
    private MpmcArrayQueue<IOBuffer> unflushedBufs;
    private final EventLoopHandler eventLoopHandler = new EventLoopHandler();
    private CompletableFuture<AsyncSocket> connectFuture;

    // immutable state
    private final SocketChannel socketChannel;
    private NioEventloop eventloop;
    private Thread eventloopThread;
    private SelectionKey key;
    private Selector selector;

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

    private NioAsyncSocket() {
        try {
            this.socketChannel = SocketChannel.open();
            this.socketChannel.configureBlocking(false);
            this.clientSide = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    NioAsyncSocket(SocketChannel socketChannel) {
        try {
            this.socketChannel = socketChannel;
            this.socketChannel.configureBlocking(false);
            this.localAddress = socketChannel.getLocalAddress();
            this.remoteAddress = socketChannel.getRemoteAddress();
            this.clientSide = false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public NioEventloop eventloop() {
        return eventloop;
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
                logger.warning("Ignoring NioAsyncSocket.setTcpKeepAliveTime. " +
                        "Please upgrade to Java 11+ or configure tcp_keepalive_time in the kernel. " +
                        "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. " +
                        "If this isn't dealt with, idle connections could be closed prematurely.");
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
                logger.warning("Ignoring NioAsyncSocket.setTcpKeepaliveIntvl. " +
                        "Please upgrade to Java 11+ or configure tcp_keepalive_intvl in the kernel. " +
                        "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. " +
                        "If this isn't dealt with, idle connections could be closed prematurely.");
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
                logger.warning("Ignoring NioAsyncSocket.setTcpKeepAliveProbes. " +
                        "Please upgrade to Java 11+ or configure tcp_keepalive_probes in the kernel: " +
                        "For more info see https://tldp.org/HOWTO/html_single/TCP-Keepalive-HOWTO/. " +
                        "If this isn't dealt with, idle connections could be closed prematurely.");
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
    public void setSoLinger(int soLinger) {
        try {
            socketChannel.setOption(SO_LINGER, soLinger);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getSoLinger() {
        try {
            return socketChannel.getOption(SO_LINGER);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
    public void activate(Eventloop eventloop) {
        if (this.eventloop != null) {
            throw new IllegalStateException(this + " already has been activated");
        }

        if (readHandler == null) {
            throw new IllegalStateException("Can't activate " + this + ": readhandler isn't set");
        }

        this.eventloop = checkInstanceOf(NioEventloop.class, eventloop, "evenloop");
        this.eventloopThread = this.eventloop.eventloopThread();
        this.unflushedBufs = new MpmcArrayQueue<>(unflushedBufsCapacity);
        if (!this.eventloop.registerClosable(NioAsyncSocket.this)) {
            throw new IllegalStateException("Can't activate NioAsynSocket: Eventloop is shutdown");
        }

        // todo: if something fails within this task, you just get a logged exception and that is it.
        boolean offered = this.eventloop.offer(() -> {
            selector = this.eventloop.selector;
            receiveBuffer = ByteBuffer.allocateDirect(getReceiveBufferSize());
            if (!clientSide) {
                try {
                    key = socketChannel.register(selector, OP_READ, eventLoopHandler);
                } catch (ClosedChannelException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });

        if (!offered) {
            throw new RuntimeException("Failed to activate " + this);
        }
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                eventloop.localRunQueue.add(eventLoopHandler);
            } else if (writeThrough) {
                eventLoopHandler.run();
            } else if (regularSchedule) {
                // todo: return value
                eventloop.offer(eventLoopHandler);
            } else {
                key.interestOps(key.interestOps() | OP_WRITE);
                eventloop.wakeup();
            }
        }
    }

    private void resetFlushed() {
        flushThread.set(null);

        if (!unflushedBufs.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                eventloop.offer(eventLoopHandler);
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
                eventloop.localRunQueue.add(eventLoopHandler);
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
        closeResource(socketChannel.socket());

        if (eventloop != null) {
            eventloop.deregisterCloseable(this);
        }
    }

    @Override
    public CompletableFuture<AsyncSocket> connect(SocketAddress address) {
        checkNotNull(address, "address");
        if (eventloop == null) {
            throw new IllegalStateException("Can't connect, NioAsyncSocket isn't activated.");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Connect to address:" + address);
        }

        CompletableFuture<AsyncSocket> future = new CompletableFuture<>();
        eventloop.offer(() -> {
            try {
                key = socketChannel.register(selector, OP_CONNECT, eventLoopHandler);
                connectFuture = future;
                socketChannel.connect(address);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return future;
    }

    private class EventLoopHandler implements NioSelectedKeyListener, Runnable {

        @Override
        public void run() {
            try {
                handleWriteReady();
            } catch (Exception e) {
                handleException(e);
            }
        }

        @Override
        public void handleException(Exception e) {
            logger.warning("Closing NioAsyncSocket due to exception", e);
            close();
        }

        @Override
        public void handle(SelectionKey key) throws IOException {
            int readyOp = key.readyOps();

            if (key.isValid() && (readyOp & OP_READ) != 0) {
                handleReadReady();
            }

            if (key.isValid() && (readyOp & OP_WRITE) != 0) {
                handleWriteReady();
            }

            if (key.isValid() && (readyOp & OP_CONNECT) != 0) {
                handleConnectReady();
            }
        }

        private void handleReadReady() throws IOException {
            readEvents.inc();

            int read = socketChannel.read(receiveBuffer);
            //System.out.println(this + " bytes read: " + bytesRead);
            if (read == -1) {
                close();
            } else {
                bytesRead.inc(read);
                receiveBuffer.flip();
                readHandler.onRead(receiveBuffer);
                compactOrClear(receiveBuffer);
            }
        }

        private void handleWriteReady() throws IOException {
            assert flushThread.get() != null;

            handleWriteCnt.inc();

            ioVector.populate(unflushedBufs);
            long written = ioVector.write(socketChannel);

            bytesWritten.inc(written);
            //System.out.println(this + " bytes written:" + written);

            if (ioVector.isEmpty()) {
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

        private void handleConnectReady() throws IOException {
            try {
                socketChannel.finishConnect();
                remoteAddress = socketChannel.getRemoteAddress();
                localAddress = socketChannel.getLocalAddress();
                if (logger.isInfoEnabled()) {
                    logger.info("Connection established " + NioAsyncSocket.this);
                }

                socketChannel.register(selector, OP_READ, this);
                connectFuture.complete(NioAsyncSocket.this);
            } catch (IOException e) {
                connectFuture.completeExceptionally(e);
                throw e;
            } finally {
                connectFuture = null;
            }
        }
    }
}
