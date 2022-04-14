package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.requestservice.FrameHandler;
import com.hazelcast.spi.impl.reactor.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.spi.impl.reactor.frame.Frame.FLAG_OP_RESPONSE;
import static java.nio.channels.SelectionKey.OP_WRITE;


// add padding around Nio channel
public abstract class NioChannel extends Channel {

    // immutable state
    protected SocketChannel socketChannel;
    public NioReactor reactor;
    protected SelectionKey key;
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

    public abstract void handleRead(ByteBuffer receiveBuffer);

    public void handleRead() {
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

    @Override
    public void handleWrite() {
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
