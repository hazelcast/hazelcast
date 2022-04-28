package com.hazelcast.spi.impl.engine.nio;

import com.hazelcast.spi.impl.engine.AsyncSocket;
import com.hazelcast.spi.impl.engine.Eventloop;
import com.hazelcast.spi.impl.engine.ReadHandler;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;


// todo: add padding around Nio channel
public final class NioAsyncSocket extends AsyncSocket implements NioSelectedKeyListener {

    public static NioAsyncSocket open() {
        return new NioAsyncSocket();
    }

    private final boolean clientSide;
    private NioReadHandler readHandler;
    // immutable state
    private SocketChannel socketChannel;
    public NioEventloop eventloop;
    private SelectionKey key;
    private Selector selector;

    // ======================================================
    // reading side of the channel.
    // ======================================================
    private ByteBuffer receiveBuffer;

    // ======================================================
    // writing side of the channel.
    // ======================================================
    // private
    public final IOVector ioVector = new IOVector();
    private boolean regularSchedule = true;
    private boolean writeThrough;

    //  concurrent
    public final AtomicReference<Thread> flushThread = new AtomicReference<>();
    public final MpmcArrayQueue<Frame> unflushedFrames = new MpmcArrayQueue<>(4096);
    private CompletableFuture<AsyncSocket> connectFuture;

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

    public SocketChannel socketChannel(){
        return socketChannel;
    }

    @Override
    public void setReadHandler(ReadHandler h) {
        this.readHandler = (NioReadHandler) checkNotNull(h);
        this.readHandler.init(this);
    }

    @Override
    public void activate(Eventloop l) {
        NioEventloop eventloop = (NioEventloop) checkNotNull(l);
        this.eventloop = eventloop;
        eventloop.execute(() -> {
            selector = eventloop.selector;
            eventloop.registeredAsyncSockets.add(NioAsyncSocket.this);
            receiveBuffer = ByteBuffer.allocateDirect(getReceiveBufferSize());

            if (!clientSide) {
                System.out.println(" server side has been registered for read");
                key = socketChannel.register(selector, OP_READ, NioAsyncSocket.this);
            }
        });
    }

    public void setRegularSchedule(boolean regularSchedule) {
        this.regularSchedule = regularSchedule;
    }

    public void setWriteThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
    }

    @Override
    public boolean isTcpNoDelay() {
        try {
            return socketChannel.getOption(StandardSocketOptions.TCP_NODELAY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setTcpNoDelay(boolean tcpNoDelay) {
        try {
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        try {
            return socketChannel.getOption(StandardSocketOptions.SO_RCVBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReceiveBufferSize(int size) {
        try {
            socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getSendBufferSize() {
        try {
            return socketChannel.getOption(StandardSocketOptions.SO_SNDBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setSendBufferSize(int size) {
        try {
            socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloop) {
                eventloop.dirtySockets.add(this);
            } else if (writeThrough) {
                try {
                    handleWriteReady();
                } catch (IOException e) {
                    handleException(e);
                }
            } else if (regularSchedule) {
                eventloop.execute(this);
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
                eventloop.execute(this);
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
                eventloop.dirtySockets.add(this);
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

    public void handleReadReady() throws IOException {
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

    @Override
    public void handleWriteReady() throws IOException {
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

    public void handleConnectReady() throws IOException {
        try {
            remoteAddress = socketChannel.getRemoteAddress();
            localAddress = socketChannel.getLocalAddress();
            logger.info("Channel established " + localAddress + "->" + remoteAddress);
            socketChannel.finishConnect();
            socketChannel.register(selector, OP_READ, this);
            connectFuture.complete(this);
        } catch (IOException e) {
            connectFuture.completeExceptionally(e);
            throw e;
        } finally {
            connectFuture = null;
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            //todo: probably offload this to the event loop.
            closeResource(socketChannel);
            eventloop.removeSocket(this);
        }
    }

    @Override
    public CompletableFuture<AsyncSocket> connect(SocketAddress address) {
        System.out.println("Connect to address:" + address);
        CompletableFuture<AsyncSocket> future = new CompletableFuture<>();
        eventloop.execute(() -> {
            key = socketChannel.register(selector, OP_CONNECT, NioAsyncSocket.this);
            connectFuture = future;
            socketChannel.connect(address);
        });
        return future;
    }
}
