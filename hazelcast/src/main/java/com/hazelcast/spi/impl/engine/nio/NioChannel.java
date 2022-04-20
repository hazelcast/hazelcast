package com.hazelcast.spi.impl.engine.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.Channel;
import com.hazelcast.spi.impl.engine.SocketConfig;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;


// todo: add padding around Nio channel
public abstract class NioChannel extends Channel implements NioSelectedKeyListener {

    // immutable state
    protected SocketChannel socketChannel;
    public NioReactor reactor;
    protected SelectionKey key;
    private Selector selector;
    private ILogger logger;

    // ======================================================
    // reading side of the channel.
    // ======================================================
    protected ByteBuffer receiveBuffer;

    // ======================================================
    // writing side of the channel.
    // ======================================================
    // private
    public final IOVector ioVector = new IOVector();
    public boolean regularSchedule = true;
    public boolean writeThrough;

    //  concurrent
    public final AtomicReference<Thread> flushThread = new AtomicReference<>();
    public final MpmcArrayQueue<Frame> unflushedFrames = new MpmcArrayQueue<>(4096);

    private CompletableFuture connectFuture;


    @Override
    public void flush() {
//        if (flushThread.get() != null) {
//            return;
//        }
//
//        if (flushThread.compareAndSet(null, Thread.currentThread())) {
//            reactor.schedule(this);
//        }

        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == reactor) {
                reactor.dirtyChannels.add(this);
            } else if (writeThrough) {
                handleWrite();
            } else if (regularSchedule) {
                reactor.schedule(this);
            } else {
                key.interestOps(key.interestOps() | OP_WRITE);
                reactor.wakeup();
            }
        }
    }

    public void resetFlushed() {
        flushThread.set(null);

        if (!unflushedFrames.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                reactor.schedule(this);
            }
        }
    }

    @Override
    public void write(Frame frame) {
        unflushedFrames.add(frame);
    }

    @Override
    public void writeAll(Collection<Frame> frames) {
        unflushedFrames.addAll(frames);
    }

    @Override
    public void writeAndFlush(Frame frame) {
        write(frame);
        flush();
    }

    @Override
    public void unsafeWriteAndFlush(Frame frame) {
        Thread currentFlushThread = flushThread.get();
        Thread currentThread = Thread.currentThread();

        assert currentThread == reactor;

        if (currentFlushThread == null) {
            if (flushThread.compareAndSet(null, currentThread)) {
                reactor.dirtyChannels.add(this);
                if (!ioVector.add(frame)) {
                    unflushedFrames.add(frame);
                }
            } else {
                unflushedFrames.add(frame);
            }
        } else if (currentFlushThread == reactor) {
            if (!ioVector.add(frame)) {
                unflushedFrames.add(frame);
            }
        } else {
            unflushedFrames.add(frame);
            flush();
        }
    }

    @Override
    public void handle(SelectionKey key) {
        int readyOp = key.readyOps();

        if (key.isValid() && (readyOp & OP_READ) != 0) {
            handleRead();
        }

        if (key.isValid() && (readyOp & OP_WRITE) != 0) {
            handleWrite();
        }

        if (key.isValid() && (readyOp & OP_CONNECT) != 0) {
            handleConnectable();
        }
    }

    protected void handleAccepted(NioReactor reactor,
                                  SocketChannel socketChannel,
                                  SocketConfig socketConfig) {
        try {

            this.reactor = reactor;
            this.selector = reactor.selector;
            this.logger = reactor.logger;
            this.socketConfig = socketConfig;
            this.socketChannel = socketChannel;
            this.remoteAddress = socketChannel.getRemoteAddress();
            this.localAddress = socketChannel.getLocalAddress();

            System.out.println("Accepted " + remoteAddress + "->" + localAddress);

            this.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
            configureSocket();
            socketChannel.configureBlocking(false);
            this.key = socketChannel.register(selector, OP_READ, this);
            reactor.registeredChannels.add(this);
        } catch (IOException e) {
            e.printStackTrace();
            close();
        }
    }

    protected void connect(CompletableFuture<Channel> future,
                           SocketAddress address,
                           NioReactor reactor) {
        try {
            this.reactor = reactor;
            this.selector = reactor.selector;
            this.logger = reactor.logger;
            this.connectFuture = future;
            this.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
            this.socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            configureSocket();
            this.key = socketChannel.register(selector, OP_CONNECT, this);
            System.out.println("Connect to address:" + address);
            socketChannel.connect(address);
        } catch (IOException e) {
            future.completeExceptionally(e);
            e.printStackTrace();
            close();
        }
    }

    private void handleConnectable() {
        try {
            reactor.registeredChannels.add(this);

            this.remoteAddress = socketChannel.getRemoteAddress();
            this.localAddress = socketChannel.getLocalAddress();
            logger.info("Channel established " + localAddress + "->" + remoteAddress);

            socketChannel.finishConnect();

            key.interestOps(OP_READ);
            connectFuture.complete(this);
        } catch (IOException e) {
            connectFuture.completeExceptionally(e);
            e.printStackTrace();
            close();
        } finally {
            connectFuture = null;
        }
    }

    public final void handleRead() {
        try {
            readEvents.inc();
            int read = socketChannel.read(receiveBuffer);
            //System.out.println(this + " bytes read: " + bytesRead);
            if (read == -1) {
                close();
            } else {
                bytesRead.inc(read);
                receiveBuffer.flip();
                handleRead(receiveBuffer);
                compactOrClear(receiveBuffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
    }

    public abstract void handleRead(ByteBuffer receiveBuffer);

    @Override
    public final void handleWrite() {
        try {
            assert flushThread.get() != null;

            handleWriteCnt.inc();

            ioVector.fill(unflushedFrames);
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
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
    }

    private void configureSocket() throws SocketException {
        Socket socket = socketChannel.socket();
        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
    }

    @Override
    public void close() {
        closeResource(socketChannel);
        reactor.removeChannel(this);
    }
}
