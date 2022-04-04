package io.netty.incubator.channel.uring;

import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Frame;
import io.netty.buffer.ByteBuf;


import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class IO_UringChannel extends Channel {
    public final ConcurrentLinkedQueue<Frame> pending = new ConcurrentLinkedQueue<>();
    public LinuxSocket socket;
    public IO_UringReactor reactor;
    public ByteBuf receiveBuff;
    public ByteBuffer current;
    public ByteBuffer readBuffer;
    public ByteBuf[] writeBufs;
    public boolean[] writeBufsInUse;
    public AtomicBoolean scheduled = new AtomicBoolean(false);
    public Frame inboundFrame;

    @Override
    public void flush() {
        if (!scheduled.get() && scheduled.compareAndSet(false, true)) {
            reactor.schedule(this);
        }
    }

    // called by the Reactor.
    public void unschedule() {
        scheduled.set(false);

        if (current == null && pending.isEmpty()) {
            return;
        }

        if (scheduled.compareAndSet(false, true)) {
            reactor.schedule(this);
        }
    }

    @Override
    public void write(Frame frame) {
        pending.add(frame);
    }

    @Override
    public void writeAndFlush(Frame frame) {
        pending.add(frame);
        flush();
    }
}
