package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.engine.AsyncSocket;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.ReadHandler;
import com.hazelcast.tpc.engine.frame.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.Buffer;
import io.netty.channel.unix.IovArray;
import io.netty.incubator.channel.uring.IOUringSubmissionQueue;
import io.netty.incubator.channel.uring.LinuxSocket;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static io.netty.incubator.channel.uring.Native.IORING_OP_READ;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITEV;

public final class IOUringAsyncSocket extends AsyncSocket implements CompletionListener {

    private final boolean clientSide;

    public static IOUringAsyncSocket open() {
        return new IOUringAsyncSocket();
    }

    protected LinuxSocket socket;
    public IOUringEventloop eventloop;

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
    private IOUringReadHandler readHandler;

    private IOUringAsyncSocket() {
        try {
            this.socket = LinuxSocket.newSocketStream(false);
            socket.setBlocking();
            this.clientSide = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    IOUringAsyncSocket(LinuxSocket socket) {
        try {
            this.socket = socket;
            socket.setBlocking();
            this.remoteAddress = socket.remoteAddress();
            this.localAddress = socket.localAddress();
            this.clientSide = false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public LinuxSocket socket() {
        return socket;
    }

    @Override
    public void activate(Eventloop l) {
        IOUringEventloop eventloop = (IOUringEventloop) l;
        this.eventloop = eventloop;
        this.eventloop.execute(() -> {
            ByteBuf iovArrayBuffer = eventloop.iovArrayBufferAllocator.directBuffer(1024 * IovArray.IOV_SIZE);
            iovArray = new IovArray(iovArrayBuffer);
            sq = eventloop.sq;
            eventloop.completionListeners.put(socket.intValue(), IOUringAsyncSocket.this);
            receiveBuff = eventloop.allocator.directBuffer(getReceiveBufferSize());

            sq_addRead();
        });
    }

    @Override
    public void setReadHandler(ReadHandler readHandler) {
        this.readHandler = (IOUringReadHandler) checkNotNull(readHandler);
        this.readHandler.init(this);
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


    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloop) {
                eventloop.dirtySockets.add(this);
            } else {
                eventloop.execute(this);
            }
        }
    }

    // called by the Reactor.
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
        // can be optimized
        unflushedFrames.add(frame);
    }

    @Override
    public void writeAll(Collection<Frame> frames) {
        unflushedFrames.addAll(frames);
    }

    @Override
    public void writeAndFlush(Frame frame) {
        unflushedFrames.add(frame);
        flush();
    }

    @Override
    public void handleException(Exception e) {
        e.printStackTrace();
        close();
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
            // todo: offloading.

            //todo: also think about releasing the resources like frame buffers
            // perhaps add a one time close check

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
    public void handleWriteReady() {
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
    }

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        if (res >= 0) {
            if (op == IORING_OP_READ) {
                readEvents.inc();
                bytesRead.inc(res);
                //System.out.println("handle_IORING_OP_READ fd:" + fd + " bytes read: " + res);
                receiveBuff.writerIndex(receiveBuff.writerIndex() + res);
                readHandler.onRead(receiveBuff);
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

    public void sq_addRead() {
        //System.out.println("sq_addRead writerIndex:" + b.writerIndex() + " capacity:" + b.capacity());
        sq.addRead(socket.intValue(),
                receiveBuff.memoryAddress(),
                receiveBuff.writerIndex(),
                receiveBuff.capacity(),
                (short) 0);
    }

    @Override
    public CompletableFuture<AsyncSocket> connect(SocketAddress address) {
        CompletableFuture<AsyncSocket> future = new CompletableFuture();

        try {
            //System.out.println(getName() + " connectRequest to address:" + address);

            if (socket.connect(address)) {
                this.remoteAddress = socket.remoteAddress();
                this.localAddress = socket.localAddress();

                eventloop.execute(() -> sq_addRead());

                logger.info("Socket connected to " + address);
                future.complete(this);
            } else {
                future.completeExceptionally(new IOException("Could not connect to " + address));
            }
        } catch (Exception e) {
            e.printStackTrace();
            future.completeExceptionally(e);
        }

        return future;
    }
}
