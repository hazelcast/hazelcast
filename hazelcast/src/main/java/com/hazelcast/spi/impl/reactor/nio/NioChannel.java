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
    public ByteBuffer[] buffs = new ByteBuffer[4096];
    public int buffsLen = 0;
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

    public void addBuffer(ByteBuffer buffer){
        //todo: we could add growing or size constraint.
        buffs[buffsLen] = buffer;
        buffsLen++;
    }

    public void discardWrittenBuffers() {
        int toIndex = 0;
        int length = buffsLen;
        for (int pos = 0; pos < length; pos++) {
            if (buffs[pos].hasRemaining()) {
                if (pos == 0) {
                    // the first one is not empty, we are done
                    break;
                } else {
                    buffs[toIndex] = buffs[pos];
                    buffs[pos] = null;
                    toIndex++;
                }
            } else {
                buffsLen--;
                buffs[pos] = null;
            }
        }
    }
}
