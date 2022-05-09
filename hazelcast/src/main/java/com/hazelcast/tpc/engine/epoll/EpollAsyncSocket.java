package com.hazelcast.tpc.engine.epoll;

import com.hazelcast.tpc.engine.AsyncSocket;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.ReadHandler;
import com.hazelcast.tpc.engine.EventloopTask;
import com.hazelcast.tpc.engine.frame.Frame;
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
    private int unflushedFramesCapacity = 65536;

    //  concurrent
    public final AtomicReference<Thread> flushThread = new AtomicReference<>();
    public MpmcArrayQueue<Frame> unflushedFrames;
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
    public EpollEventloop getEventloop() {
        return eventloop;
    }


    @Override
    public void setReadHandler(ReadHandler readHandler) {
        this.readHandler = (EpollReadHandler) checkNotNull(readHandler);
    }

    @Override
    public void setSoLinger(int soLinger) {
        try {
            socket.setSoLinger(soLinger);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getSoLinger() {
        try {
            return socket.getSoLinger();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setKeepAlive(boolean keepAlive) {
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
    public void setTcpNoDelay(boolean tcpNoDelay) {
        try {
            socket.setTcpNoDelay(tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    @Override
    public int getReceiveBufferSize() {
        try {
            return socket.getReceiveBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReceiveBufferSize(int size) {
        try {
            socket.setReceiveBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getSendBufferSize() {
        try {
            return socket.getSendBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setSendBufferSize(int size) {
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
        EpollEventloop eventloop = (EpollEventloop) checkNotNull(l);
        this.eventloop = eventloop;
        this.unflushedFrames = new MpmcArrayQueue<>(unflushedFramesCapacity);

        if (!eventloop.registerSocket(EpollAsyncSocket.this)) {
            throw new IllegalStateException("Can't activate socket, eventloop is not running");
        }

        eventloop.execute(() -> {
            //selector = eventloop.selector;
            receiveBuffer = ByteBuffer.allocateDirect(getReceiveBufferSize());


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
            if (currentThread == eventloop) {
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

        if (!unflushedFrames.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                eventloop.execute(eventLoopHandler);
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
                eventloop.localRunQueue.add(eventLoopHandler);
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

            eventloop.deregisterSocket(this);
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

                        if (!eventloop.registerSocket(EpollAsyncSocket.this)) {
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

    private class EventLoopHandler implements EventloopTask {
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

            ioVector.fill(unflushedFrames);
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
