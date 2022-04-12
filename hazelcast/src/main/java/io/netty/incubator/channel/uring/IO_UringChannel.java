package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.frame.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.IovArray;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class IO_UringChannel extends Channel {
    protected LinuxSocket socket;
    public IO_UringReactor reactor;

    // ======================================================
    // For the reading side of the channel
    // ======================================================
    protected ByteBuf receiveBuff;
    protected Frame inboundFrame;

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
}
