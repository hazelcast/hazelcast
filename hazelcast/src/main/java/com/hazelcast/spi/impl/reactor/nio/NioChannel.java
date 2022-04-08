package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Frame;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;


// add padding around Nio channel
public class NioChannel extends Channel {

    // immutable state
    protected SocketChannel socketChannel;
    protected NioReactor reactor;

    // ======================================================
    // reading side of the channel.
    // ======================================================
    protected Frame inboundFrame;
    protected ByteBuffer receiveBuffer;

    // ======================================================
    // writing side of the channel.
    // ======================================================
    // private
    protected ByteBuffer[] buffs = new ByteBuffer[4096];
    protected Frame[] flushedFrames = new Frame[buffs.length];
    protected int buffsLen = 0;

    //  concurrent
    protected AtomicReference<Thread> flushThread = new AtomicReference<>();
    protected final ConcurrentLinkedQueue<Frame> unflushedFrames = new ConcurrentLinkedQueue<>();

    @Override
    public void flush() {
        if (flushThread.get() != null) {
            return;
        }

        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == reactor) {
                boolean offered = reactor.dirtyChannels.offer(this);
                if (!offered) {
                    throw new RuntimeException("overload");//todo
                }
            } else {
                reactor.schedule(this);
            }
        }
    }

    // called by the Reactor.
    public void resetFlushed() {
        flushThread.set(null);

        // todo: it could be that there are byte buffers we didn't manage to write
        // so we would end up with dirty work being undetected
        if (unflushedFrames.isEmpty()) {
            return;
        }

        if (flushThread.compareAndSet(null, reactor)) {
            reactor.schedule(this);
        }
    }

    @Override
    public void write(Frame frame) {
        if (Thread.currentThread() == reactor) {
            addFlushedFrame(frame);
        } else {
            unflushedFrames.add(frame);
        }
    }

    @Override
    public void writeAndFlush(Frame frame) {
        // Ideally we want to directly write to the
      //  Thread currentFlushThread = flushThread.get();
//        if (currentFlushThread == reactor) {
//            addFlushedFrame(frame);
//        } else {
            write(frame);
            flush();
        //}
    }

    public void addFlushedFrame(Frame frame) {
        //todo: we could add growing or size constraint.
        buffs[buffsLen] = frame.byteBuffer();
        flushedFrames[buffsLen] = frame;
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

                    flushedFrames[toIndex] = flushedFrames[pos];
                    flushedFrames[pos] = null;

                    toIndex++;
                }
            } else {
                buffsLen--;
                buffs[pos] = null;

                flushedFrames[pos].release();
                flushedFrames[pos] = null;
            }
        }
    }
}
