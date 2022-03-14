package com.hazelcast.spi.impl.reactor;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Channel {

    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();

    public ByteBuffer readBuffer;
    public SocketChannel socketChannel;
    public Reactor reactor;

    public void enqueueAndFlush(ByteBuffer buffer) {
        pending.add(buffer);
        reactor.wakeup();
    }
}
