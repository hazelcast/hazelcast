package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Frame;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioChannel extends Channel {

    public final ConcurrentLinkedQueue<Frame> pending = new ConcurrentLinkedQueue<>();
    public ByteBuffer receiveBuffer;
    public SocketChannel socketChannel;
    public NioReactor reactor;
    public ByteBuffer[] buffs = new ByteBuffer[4096];
    public Frame[] frames = new Frame[buffs.length];
    public int buffsLen = 0;
    public AtomicBoolean scheduled = new AtomicBoolean();
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
    public void write(Frame frame) {
        pending.add(frame);
    }

    @Override
    public void writeAndFlush(Frame frame) {
        write(frame);
        flush();
    }

    public void addFrame(Frame frame) {
        //todo: we could add growing or size constraint.
        buffs[buffsLen] = frame.getByteBuffer();
        frames[buffsLen] = frame;
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

                    frames[toIndex] = frames[pos];
                    frames[pos] = null;

                    toIndex++;
                }
            } else {
                buffsLen--;
                buffs[pos] = null;

                frames[pos].release();
                frames[pos] = null;
            }
        }
    }
}
