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

package com.hazelcast.tpc.engine.epoll;

import com.hazelcast.tpc.engine.AsyncSocket;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.ReadHandler;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import io.netty.channel.epoll.LinuxSocket;
import io.netty.channel.epoll.Native;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;


// add padding around Nio channel
public final class EpollAsyncSocket extends AsyncSocket {

    private Thread eventloopThread;

    public static EpollAsyncSocket open() {
        return new EpollAsyncSocket();
    }

    private final boolean clientSide;
    // immutable state
    private LinuxSocket socket;
    private EpollEventloop eventloop;
    private boolean writeThrough;

    // ======================================================
    // reading side of the channel.
    // ======================================================
    private ByteBuffer receiveBuffer;

    // ======================================================
    // writing side of the channel.
    // ======================================================
    // private
    private final IOVector ioVector = new IOVector();
    private int unflushedBufsCapacity = 65536;

    //  concurrent
    public final AtomicReference<Thread> flushThread = new AtomicReference<>();
    public MpmcArrayQueue<IOBuffer> unflushedBufs;
    //public final ConcurrentLinkedQueue<Frame> unflushedFrames = new ConcurrentLinkedQueue<>();

    private int flags = Native.EPOLLET;

    private EpollReadHandler readHandler;
    private final EventLoopHandler eventLoopHandler = new EventLoopHandler();

    private EpollAsyncSocket() {
        this.socket = LinuxSocket.newSocketStream();
        this.clientSide = true;
    }

    private EpollAsyncSocket(LinuxSocket socket) {
        this.socket = socket;
        this.clientSide = false;
        this.localAddress = socket.localAddress();
        this.remoteAddress = socket.remoteAddress();
    }

    @Override
    public EpollEventloop eventloop() {
        return eventloop;
    }

    @Override
    public void readHandler(ReadHandler readHandler) {
        this.readHandler = (EpollReadHandler) checkNotNull(readHandler);
    }

    @Override
    public void soLinger(int soLinger) {
        try {
            socket.setSoLinger(soLinger);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int soLinger() {
        try {
            return socket.getSoLinger();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void keepAlive(boolean keepAlive) {
        try {
            socket.setKeepAlive(keepAlive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isKeepAlive() {
        try {
            return socket.isKeepAlive();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isTcpNoDelay() {
        try {
            return socket.isTcpNoDelay();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void tcpNoDelay(boolean tcpNoDelay) {
        try {
            socket.setTcpNoDelay(tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    @Override
    public int receiveBufferSize() {
        try {
            return socket.getReceiveBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void receiveBufferSize(int size) {
        try {
            socket.setReceiveBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int sendBufferSize() {
        try {
            return socket.getSendBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void sendBufferSize(int size) {
        try {
            socket.setSendBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void setFlag(int flag) throws IOException {
        if (!isFlagSet(flag)) {
            flags |= flag;
            modifyEvents();
        }
    }

    void clearFlag(int flag) throws IOException {
        if (isFlagSet(flag)) {
            flags &= ~flag;
            modifyEvents();
        }
    }

    @Override
    public void activate(Eventloop l) {
        if (this.eventloop != null) {
            throw new IllegalStateException("Can't activate an already activated AsyncSocket");
        }

        EpollEventloop eventloop = (EpollEventloop) checkNotNull(l);
        this.eventloop = eventloop;
        this.eventloopThread = eventloop.eventloopThread();
        this.unflushedBufs = new MpmcArrayQueue<>(unflushedBufsCapacity);

        if (!eventloop.registerResource(EpollAsyncSocket.this)) {
            throw new IllegalStateException("Can't activate socket, eventloop is not running");
        }

        eventloop.execute(() -> {
            //selector = eventloop.selector;
            receiveBuffer = ByteBuffer.allocateDirect(receiveBufferSize());


//            if (!clientSide) {
//                key = socketChannel.register(selector, OP_READ, NioAsyncSocket.this);
//            }
        });
    }


    private void modifyEvents() throws IOException {
//        if (socket.isOpen() && isRegistered()) {
//            ((EpollEventLoop) eventLoop()).modify(this);
//        }

        if (socket.isOpen()) {
            try {
                System.out.println("reactor.epollFd.intValue:" + eventloop.epollFd.intValue());
                System.out.println("socket.intValue:" + socket.intValue());
                System.out.println("flags:" + flags);
                Native.epollCtlMod(eventloop.epollFd.intValue(), socket.intValue(), flags);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    boolean isFlagSet(int flag) {
        return (flags & flag) != 0;
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                eventloop.localRunQueue.add(eventLoopHandler);
            } else if (writeThrough) {
                eventLoopHandler.run();
            } else {
                eventloop.execute(eventLoopHandler);
            }
        }
    }

    public void resetFlushed() {
        flushThread.set(null);

        if (!unflushedBufs.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                eventloop.execute(eventLoopHandler);
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
    public void close() {
        if (closed.compareAndSet(false, true)) {
            System.out.println("Closing  " + this);

            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            eventloop.deregisterResource(this);
        }
    }

    @Override
    public CompletableFuture<AsyncSocket> connect(SocketAddress address) {
        CompletableFuture<AsyncSocket> future = new CompletableFuture();
        try {
            System.out.println("ConnectRequest address:" + address);

            if (!socket.connect(address)) {
                future.completeExceptionally(new RuntimeException("Failed to connect to " + address));
            } else {
                eventloop.execute(() -> {
                    try {

                        if (!eventloop.registerResource(EpollAsyncSocket.this)) {
                            throw new IllegalStateException();
                        }
                        logger.info("Socket listening at " + address);
                        future.complete(EpollAsyncSocket.this);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private class EventLoopHandler implements Runnable {
        @Override
        public void run() {
            try {
                handleWriteReady();
            } catch (Exception e) {
                e.printStackTrace();
                close();
            }
        }

        public void handleRead() throws IOException {
            readEvents.inc();
            int read = socket.read(receiveBuffer, receiveBuffer.position(), receiveBuffer.remaining());
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
            if (flushThread.get() == null) {
                throw new RuntimeException("Channel is not in flushed state");
            }
            handleWriteCnt.inc();

            ioVector.fill(unflushedBufs);
            long written = ioVector.write(socket);

            bytesWritten.inc(written);
            //System.out.println(getName() + " bytes written:" + written);

            //       SelectionKey key = channel.key;
//            if (ioVector.isEmpty()) {
//                int interestOps = key.interestOps();
//                if ((interestOps & OP_WRITE) != 0) {
//                    key.interestOps(interestOps & ~OP_WRITE);
//                }

            resetFlushed();
//            } else {
//                System.out.println("Didn't manage to write everything." + channel);
//                key.interestOps(key.interestOps() | OP_WRITE);
//            }

        }

    }
}
