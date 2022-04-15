package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.engine.Channel;
import com.hazelcast.spi.impl.engine.SocketConfig;
import com.hazelcast.spi.impl.engine.frame.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.Buffer;
import io.netty.channel.unix.IovArray;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.incubator.channel.uring.Native.IORING_OP_READ;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITEV;

public abstract class IOUringChannel extends Channel implements CompletionListener {
    protected LinuxSocket socket;
    public IOUringReactor reactor;

    // ======================================================
    // For the reading side of the channel
    // ======================================================
    protected ByteBuf receiveBuff;
    protected IOUringSubmissionQueue sq;

    // ======================================================
    // for the writing side of the channel.
    // ======================================================
    // concurrent state
    public AtomicReference<Thread> flushThread = new AtomicReference<>();
    public final MpmcArrayQueue<Frame> unflushedFrames = new MpmcArrayQueue<>(4096);
    //public final ConcurrentLinkedQueue<Frame> unflushedFrames = new ConcurrentLinkedQueue<>();

    // isolated state.
    public IovArray iovArray;
    public IOVector ioVector = new IOVector();

    public void configure(IOUringReactor reactor, SocketConfig socketConfig, LinuxSocket socket) throws IOException {
        this.reactor = reactor;
        this.receiveBuff = reactor.allocator.directBuffer(socketConfig.receiveBufferSize);
        this.socket = socket;
        ByteBuf iovArrayBuffer = reactor.iovArrayBufferAllocator.directBuffer(1024 * IovArray.IOV_SIZE);
        this.iovArray = new IovArray(iovArrayBuffer);
        this.sq = reactor.sq;

        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
        socket.setTcpQuickAck(socketConfig.tcpQuickAck);
        String id = socket.localAddress() + "->" + socket.remoteAddress();
        System.out.println(reactor.getName() + " " + id + " tcpNoDelay: " + socket.isTcpNoDelay());
        System.out.println(reactor.getName() + " " + id + " tcpQuickAck: " + socket.isTcpQuickAck());
        System.out.println(reactor.getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(reactor.getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

    public void onConnectionEstablished() throws IOException {
        this.remoteAddress = socket.remoteAddress();
        this.localAddress = socket.localAddress();
        sq_addRead();
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == reactor) {
                reactor.dirtyChannels.add(this);
            } else {
                reactor.schedule(this);
            }
        }
    }

    // called by the Reactor.
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
        // can be optimized
        unflushedFrames.add(frame);
    }

    @Override
    public void writeAndFlush(Frame frame) {
        unflushedFrames.add(frame);
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
        //todo: also think about releasing the resources like frame buffers
        // perhaps add a one time close check

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
    public void handleWrite() {
        try {
            if (flushThread.get() == null) {
                throw new RuntimeException("Channel should be in flushed state");
            }

            ioVector.fill(unflushedFrames);

            int frameCount = ioVector.size();
            if (frameCount == 1) {
                ByteBuffer buffer = ioVector.get(0).byteBuffer();
                sq.addWrite(socket.intValue(),
                        Buffer.memoryAddress(buffer),
                        buffer.position(),
                        buffer.limit(),
                        (short) 0);
            } else {
                int offset = iovArray.count();
                ioVector.fillIoArray(iovArray);

                sq.addWritev(socket.intValue(),
                        iovArray.memoryAddress(offset),
                        iovArray.count() - offset,
                        (short) 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
    }

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        if (res >= 0) {
            if (op == IORING_OP_READ) {
                readEvents.inc();
                bytesRead.inc(res);
                //System.out.println("handle_IORING_OP_READ fd:" + fd + " bytes read: " + res);
                receiveBuff.writerIndex(receiveBuff.writerIndex() + res);
                onRead(receiveBuff);
                receiveBuff.discardReadBytes();
                // we want to read more data.
                sq_addRead();
            } else if (op == IORING_OP_WRITE) {
                //System.out.println("handle_IORING_OP_WRITE fd:" + fd + " bytes written: " + res);
                ioVector.compact(res);
                resetFlushed();
            } else if (op == IORING_OP_WRITEV) {
                //System.out.println("handle_IORING_OP_WRITEV fd:" + fd + " bytes written: " + res);
                ioVector.compact(res);
                iovArray.clear();
                resetFlushed();
            }
        } else {
            System.out.println("Problem: handle_IORING_OP_READ res:" + res);
        }
    }

    public abstract void onRead(ByteBuf receiveBuffer);

    public void sq_addRead() {
        //System.out.println("sq_addRead writerIndex:" + b.writerIndex() + " capacity:" + b.capacity());
        sq.addRead(socket.intValue(),
                receiveBuff.memoryAddress(),
                receiveBuff.writerIndex(),
                receiveBuff.capacity(),
                (short) 0);
    }
}
