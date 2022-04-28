package io.netty.channel.epoll;

import com.hazelcast.spi.impl.engine.AsyncSocket;
import com.hazelcast.spi.impl.engine.Eventloop;
import com.hazelcast.spi.impl.engine.ReadHandler;
import com.hazelcast.spi.impl.engine.SocketConfig;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static io.netty.channel.epoll.Native.EPOLLIN;


// add padding around Nio channel
public final class EpollAsyncSocket extends AsyncSocket {

    public static EpollAsyncSocket open() {
        return new EpollAsyncSocket();
    }

    // immutable state
    protected LinuxSocket socket;
    public EpollEventloop eventloop;
    protected boolean writeThrough;

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
    //public final ConcurrentLinkedQueue<Frame> unflushedFrames = new ConcurrentLinkedQueue<>();

    protected int flags = Native.EPOLLET;

    private EpollReadHandler readHandler;


    @Override
    public void activate(Eventloop eventloop) {

    }

    @Override
    public void setReadHandler(ReadHandler readHandler) {

    }

    @Override
    public CompletableFuture<AsyncSocket> connect(SocketAddress address) {
        return null;
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
                eventloop.dirtySockets.add(this);
            } else if (writeThrough) {
                try {
                    handleWriteReady();
                } catch (IOException e) {
                    handleException(e);
                }
            } else {
                eventloop.execute(this);
            }
        }
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
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            eventloop.removeSocket(this);
        }
    }

    @Override
    public void handleException(Exception e) {
        e.printStackTrace();
        close();
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


    @Override
    public void handleWriteReady() throws IOException {
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

    public void configure(EpollEventloop eventloop, LinuxSocket socket, SocketConfig socketConfig) throws IOException {
        this.eventloop = eventloop;
        this.socket = socket;

        receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);

        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
        //socket.setTcpQuickAck(socketConfig.tcpQuickAck);

        String id = socket.localAddress() + "->" + socket.remoteAddress();
        System.out.println(eventloop.getName() + " " + id + " tcpNoDelay: " + socket.isTcpNoDelay());
        System.out.println(eventloop.getName() + " " + id + " tcpQuickAck: " + socket.isTcpQuickAck());
        System.out.println(eventloop.getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(eventloop.getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

    public void onConnectionEstablished() throws IOException {
        remoteAddress = socket.remoteAddress();
        localAddress = socket.localAddress();

        System.out.println("Connection established");

        setFlag(EPOLLIN);
    }

    @Override
    public String toString() {
        return "EpollAsyncSocket(" + localAddress + "->" + remoteAddress + ")";
    }
}
