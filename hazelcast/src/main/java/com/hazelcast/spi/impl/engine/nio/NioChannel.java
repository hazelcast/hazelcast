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
    public NioEventloop eventloop;
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
            if (currentThread == eventloop) {
                eventloop.dirtyChannels.add(this);
            } else if (writeThrough) {
                try {
                    handleWrite();
                }catch (IOException e){
                    handleException(e);
                }
            } else if (regularSchedule) {
                eventloop.schedule(this);
            } else {
                key.interestOps(key.interestOps() | OP_WRITE);
                eventloop.wakeup();
            }
        }
    }

    @Override
    public void handleException(Exception e) {
        e.printStackTrace();
        close();
    }

    public void resetFlushed() {
        flushThread.set(null);

        if (!unflushedFrames.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                eventloop.schedule(this);
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

        assert currentThread == eventloop;

        if (currentFlushThread == null) {
            if (flushThread.compareAndSet(null, currentThread)) {
                eventloop.dirtyChannels.add(this);
                if (!ioVector.add(frame)) {
                    unflushedFrames.add(frame);
                }
            } else {
                unflushedFrames.add(frame);
            }
        } else if (currentFlushThread == eventloop) {
            if (!ioVector.add(frame)) {
                unflushedFrames.add(frame);
            }
        } else {
            unflushedFrames.add(frame);
            flush();
        }
    }

    @Override
    public void handle(SelectionKey key) throws IOException{
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

    protected void handleAccepted(NioEventloop eventloop,
                                  SocketChannel socketChannel,
                                  SocketConfig socketConfig) throws IOException {
        this.eventloop = eventloop;
        this.selector = eventloop.selector;
        this.logger = eventloop.logger;
        this.socketConfig = socketConfig;
        this.socketChannel = socketChannel;
        this.remoteAddress = socketChannel.getRemoteAddress();
        this.localAddress = socketChannel.getLocalAddress();

        System.out.println("Accepted " + remoteAddress + "->" + localAddress);

        this.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
        configureSocket();
        socketChannel.configureBlocking(false);
        this.key = socketChannel.register(selector, OP_READ, this);
        eventloop.registeredChannels.add(this);
    }

    protected void connect(CompletableFuture<Channel> future,
                           SocketAddress address,
                           NioEventloop eventloop) throws IOException {
        this.eventloop = eventloop;
        this.selector = eventloop.selector;
        this.logger = eventloop.logger;
        this.connectFuture = future;
        this.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
        this.socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        configureSocket();
        this.key = socketChannel.register(selector, OP_CONNECT, this);
        System.out.println("Connect to address:" + address);
        socketChannel.connect(address);
    }

    private void handleConnectable() throws IOException {
        try {
            eventloop.registeredChannels.add(this);

            this.remoteAddress = socketChannel.getRemoteAddress();
            this.localAddress = socketChannel.getLocalAddress();
            logger.info("Channel established " + localAddress + "->" + remoteAddress);

            socketChannel.finishConnect();

            key.interestOps(OP_READ);
            connectFuture.complete(this);
        } catch (IOException e) {
            connectFuture.completeExceptionally(e);
            throw e;
        } finally {
            connectFuture = null;
        }
    }

    public final void handleRead() throws IOException {
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
    }

    public abstract void handleRead(ByteBuffer receiveBuffer);

    @Override
    public final void handleWrite() throws IOException {
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
        eventloop.removeChannel(this);
    }
}
