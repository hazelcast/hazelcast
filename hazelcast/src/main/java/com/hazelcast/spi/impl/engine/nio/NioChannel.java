package com.hazelcast.spi.impl.engine.nio;

import com.hazelcast.spi.impl.engine.Channel;
import com.hazelcast.spi.impl.engine.SocketConfig;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;


// todo: add padding around Nio channel
public abstract class NioChannel extends Channel implements NioSelectedKeyListener {

    // immutable state
    protected SocketChannel socketChannel;
    public NioReactor reactor;
    protected boolean regularSchedule = false;
    protected SelectionKey key;
    protected boolean writeThrough;
    private Selector selector;
    // ======================================================
    // reading side of the channel.
    // ======================================================
    protected ByteBuffer receiveBuffer;

    // ======================================================
    // writing side of the channel.
    // ======================================================
    // private
    public final IOVector ioVector = new IOVector();

    //  concurrent
    public final AtomicReference<Thread> flushThread = new AtomicReference<>();
    public final MpmcArrayQueue<Frame> unflushedFrames = new MpmcArrayQueue<>(4096);

    protected void configure(NioReactor reactor,
                             SocketChannel socketChannel,
                             SocketConfig socketConfig) throws IOException {
        this.reactor = reactor;
        this.selector = reactor.selector;
        this.socketChannel = socketChannel;
        this.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);

        Socket socket = socketChannel.socket();
        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);

        String id = socket.getLocalAddress() + "->" + socket.getRemoteSocketAddress();
        System.out.println(reactor.getName() + " " + id + " tcpNoDelay: " + socket.getTcpNoDelay());
        System.out.println(reactor.getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(reactor.getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

    protected void onConnectionEstablished() throws IOException {
        this.remoteAddress = socketChannel.getRemoteAddress();
        this.localAddress = socketChannel.getLocalAddress();
        this.key = socketChannel.register(selector, OP_READ, this);
    }

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
        if (key.isValid() && key.isReadable()) {
            handleRead();
        }

        if (key.isValid() && key.isWritable()) {
            handleWrite();
        }
    }

    public final void handleRead() {
        try {
            readEvents.inc();
            int read = socketChannel.read(receiveBuffer);
            //System.out.println(this + " bytes read: " + bytesRead);
            if (read == -1) {
                close();
                return;
            }

            bytesRead.inc(read);
            receiveBuffer.flip();

            handleRead(receiveBuffer);

            compactOrClear(receiveBuffer);
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
    }

    public abstract void handleRead(ByteBuffer receiveBuffer);

    @Override
    public final void handleWrite() {
        try {
            if (flushThread.get() == null) {
                throw new RuntimeException("Channel is not in flushed state");
            }
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

    @Override
    public void close() {
        closeResource(socketChannel);
        reactor.removeChannel(this);
    }
}
