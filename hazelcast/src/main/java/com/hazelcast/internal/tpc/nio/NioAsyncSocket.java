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
import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.net.StandardSocketOptions.*;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * Nio version of the {@link AsyncSocket}.
 */
public final class NioAsyncSocket extends AsyncSocket {

    public static NioAsyncSocket open() {
        return new NioAsyncSocket();
    }

    private int unflushedBufsCapacity = 65536;

    // immutable state
    private final SocketChannel socketChannel;
    private final boolean clientSide;
    private NioEventloop eventloop;
    private Thread eventloopThread;
    private SelectionKey key;
    private Selector selector;

    // ======================================================
    // reading side of the socket.
    // ======================================================
    private ByteBuffer receiveBuffer;
    private NioAsyncReadHandler readHandler;

    // ======================================================
    // writing side of the socket.
    // ======================================================
    public final IOVector ioVector = new IOVector();
    private boolean regularSchedule = true;
    private boolean writeThrough;

    //  concurrent
    public final AtomicReference<Thread> flushThread = new AtomicReference<>();
    public MpmcArrayQueue<IOBuffer> unflushedBufs;
    private CompletableFuture<AsyncSocket> connectFuture;
    private final EventLoopHandler eventLoopHandler = new EventLoopHandler();

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

    @Override
    public void readHandler(ReadHandler h) {
        this.readHandler = (NioAsyncReadHandler) checkNotNull(h);
        this.readHandler.init(this);
    }

    public void setUnflushedBufsCapacity(int unflushedBufsCapacity) {
        this.unflushedBufsCapacity = checkPositive("unflushedBufsCapacity", unflushedBufsCapacity);
    }

    public void setRegularSchedule(boolean regularSchedule) {
        this.regularSchedule = regularSchedule;
    }

    public void setWriteThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
    }

    @Override
    public void soLinger(int soLinger) {
        try {
            socketChannel.setOption(SO_LINGER, soLinger);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int soLinger() {
        try {
            return socketChannel.getOption(SO_LINGER);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void keepAlive(boolean keepAlive) {
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
    public void tcpNoDelay(boolean tcpNoDelay) {
        try {
            socketChannel.setOption(TCP_NODELAY, tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int receiveBufferSize() {
        try {
            return socketChannel.getOption(SO_RCVBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void receiveBufferSize(int size) {
        try {
            socketChannel.setOption(SO_RCVBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int sendBufferSize() {
        try {
            return socketChannel.getOption(SO_SNDBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void sendBufferSize(int size) {
        try {
            socketChannel.setOption(SO_SNDBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void activate(Eventloop l) {
        if (this.eventloop != null) {
            throw new IllegalStateException("Can't activate an already activated AsyncSocket");
        }

        NioEventloop eventloop = (NioEventloop) checkNotNull(l);
        this.eventloop = eventloop;
        this.eventloopThread = eventloop.eventloopThread();
        this.unflushedBufs = new MpmcArrayQueue<>(unflushedBufsCapacity);

        if (!eventloop.registerResource(NioAsyncSocket.this)) {
            throw new IllegalStateException("Can't activate NioAsynSocket, eventloop not active");
        }

        eventloop.offer(() -> {
            selector = eventloop.selector;
            receiveBuffer = ByteBuffer.allocateDirect(receiveBufferSize());

            if (!clientSide) {
                try {
                    key = socketChannel.register(selector, OP_READ, eventLoopHandler);
                } catch (ClosedChannelException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
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
                if (ioVector.add(buf)) {
                    result = true;
                } else {
                    result = unflushedBufs.add(buf);
                }
            } else {
                result = unflushedBufs.add(buf);
            }
        } else if (currentFlushThread == eventloopThread) {
            if (ioVector.add(buf)) {
                result = true;
            } else {
                result = unflushedBufs.add(buf);
            }
        } else {
            result = unflushedBufs.add(buf);
            flush();
        }
        return result;
    }

    @Override
    protected void doClose() {
        if (eventloop != null) {
            eventloop.deregisterResource(this);
        }
        closeResource(socketChannel);
    }

    @Override
    public CompletableFuture<AsyncSocket> connect(SocketAddress address) {
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
            logger.warning(e);
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

            ioVector.fill(unflushedBufs);
            long written = ioVector.write(socketChannel);

            bytesWritten.inc(written);
            //System.out.println(this + " bytes written:" + written);

            if (ioVector.isEmpty()) {
                int interestOps = key.interestOps();
                if ((interestOps & OP_WRITE) != 0) {
                    key.interestOps(interestOps & ~OP_WRITE);
                }

                resetFlushed();
            } else {
                key.interestOps(key.interestOps() | OP_WRITE);
            }
        }

        private void handleConnectReady() throws IOException {
            try {
                socketChannel.finishConnect();
                remoteAddress = socketChannel.getRemoteAddress();
                localAddress = socketChannel.getLocalAddress();
                if (logger.isInfoEnabled()) {
                    logger.info("Connected from " + localAddress + "->" + remoteAddress);
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
