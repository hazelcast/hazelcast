package io.netty.channel.epoll;

import com.hazelcast.spi.impl.engine.Channel;
import com.hazelcast.spi.impl.engine.SocketConfig;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static io.netty.channel.epoll.Native.EPOLLIN;


// add padding around Nio channel
public abstract class EpollChannel extends Channel {

    // immutable state
    protected LinuxSocket socket;
    public EpollReactor reactor;
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
                System.out.println("reactor.epollFd.intValue:" + reactor.epollFd.intValue());
                System.out.println("socket.intValue:" + socket.intValue());
                System.out.println("flags:" + flags);
                Native.epollCtlMod(reactor.epollFd.intValue(), socket.intValue(), flags);
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
            if (currentThread == reactor) {
                reactor.dirtyChannels.add(this);
            } else if (writeThrough) {
                try {
                    handleWrite();
                } catch (IOException e) {
                    handleException(e);
                }
            } else {
                reactor.schedule(this);
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
    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        reactor.removeChannel(this);
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
            onRead(receiveBuffer);
            compactOrClear(receiveBuffer);
        }
    }

    public abstract void onRead(ByteBuffer receiveBuffer);

    @Override
    public void handleWrite() throws IOException {
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

    public void configure(EpollReactor reactor, LinuxSocket socket, SocketConfig socketConfig) throws IOException {
        this.reactor = reactor;
        this.socket = socket;

        receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);

        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
        //socket.setTcpQuickAck(socketConfig.tcpQuickAck);

        String id = socket.localAddress() + "->" + socket.remoteAddress();
        System.out.println(reactor.getName() + " " + id + " tcpNoDelay: " + socket.isTcpNoDelay());
        System.out.println(reactor.getName() + " " + id + " tcpQuickAck: " + socket.isTcpQuickAck());
        System.out.println(reactor.getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(reactor.getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

    public void onConnectionEstablished() throws IOException {
        remoteAddress = socket.remoteAddress();
        localAddress = socket.localAddress();

        System.out.println("Connection established");

        setFlag(EPOLLIN);
    }



}
