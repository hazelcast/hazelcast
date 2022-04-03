package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioChannel extends Channel {

    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();
    public ByteBuffer readBuffer;
    public SocketChannel socketChannel;
    public NioReactor reactor;
    public final PacketIOHelper packetReader = new PacketIOHelper();
    public ByteBuffer[] writeBuffs = new ByteBuffer[4096];
    public int writeBuffLen = 0;
    public AtomicBoolean scheduled = new AtomicBoolean();

    @Override
    public void flush() {
        if (!scheduled.get() && scheduled.compareAndSet(false, true)) {
            reactor.schedule(this);
        }
    }

    // called by the Reactor.
    public void unschedule() {
        scheduled.set(false);

        // todo: it could be that there are byte buffers we didn't manage to write
        // so we would end up with dirty work being undetected
        if (pending.isEmpty()) {
            return;
        }

        if (scheduled.compareAndSet(false, true)) {
            reactor.schedule(this);
        }
    }

    @Override
    public void write(ByteBuffer buffer) {
        pending.add(buffer);
    }

    @Override
    public void writeAndFlush(ByteBuffer buffer) {
        write(buffer);
        flush();
    }
}
