package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Frame;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.closeResource;


// add padding around Nio channel
public class NioChannel extends Channel {

    // immutable state
    protected SocketChannel socketChannel;
    protected NioReactor reactor;
    protected SelectionKey key;
    protected boolean writeThrough;

    // ======================================================
    // reading side of the channel.
    // ======================================================
    protected Frame inboundFrame;
    protected ByteBuffer receiveBuffer;

    // ======================================================
    // writing side of the channel.
    // ======================================================
    // private
    protected final IOVector ioVector = new IOVector();

    //  concurrent
    protected final AtomicReference<Thread> flushThread = new AtomicReference<>();
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
                assert offered;
            } else if (writeThrough) {
                reactor.handleWrite(this);
            } else {
                reactor.schedule(this);
            }
        }
    }

    public void resetFlushed() {
        flushThread.set(null);

        if (unflushedFrames.isEmpty()) {
            return;
        }

        if (flushThread.compareAndSet(null, Thread.currentThread())) {
            reactor.schedule(this);
        }
    }

    @Override
    public void write(Frame frame) {
        Thread currentThread = Thread.currentThread();
        if (currentThread == reactor && currentThread == flushThread.get()) {
            if (!ioVector.add(frame)) {
                unflushedFrames.add(frame);
            }
        } else {
            unflushedFrames.add(frame);
        }
    }

    @Override
    public void writeAndFlush(Frame frame) {
        write(frame);
        flush();
    }


    @Override
    public void close() {
        closeResource(socketChannel);
        reactor.removeChannel(this);
    }
}
