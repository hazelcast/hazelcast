package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.CircularQueue;
import com.hazelcast.spi.impl.reactor.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.IovArray;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.channel.unix.Limits.IOV_MAX;

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
    public AtomicBoolean flushed = new AtomicBoolean(false);
    public final MpmcArrayQueue<Frame> unflushedFrames = new MpmcArrayQueue<>(4096);
    //public final ConcurrentLinkedQueue<Frame> unflushedFrames = new ConcurrentLinkedQueue<>();

    // isolated state.
    public IovArray iovArray;
    public CircularQueue<Frame> flushedFrames = new CircularQueue<>(IOV_MAX);

    @Override
    public void flush() {
        if (!flushed.get() && flushed.compareAndSet(false, true)) {
            reactor.schedule(this);
        }
    }

    // called by the Reactor.
    public void resetFlushed() {
        flushed.set(false);

        if (!unflushedFrames.isEmpty()) {
            if (flushed.compareAndSet(false, true)) {
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
        //todo: can be optimized

        unflushedFrames.add(frame);
        flush();
    }

    @Override
    public void unsafeWriteAndFlush(Frame frame) {
        writeAndFlush(frame);
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
