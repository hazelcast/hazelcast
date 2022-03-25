package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.reactor.nio.NioReactor;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class Channel {

    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();

    public Connection connection;
    public ByteBuffer readBuffer;
    public SocketChannel socketChannel;
    public NioReactor reactor;
    public ByteBuffer current;

    public void flush(){
        reactor.wakeup();
    }

    public void write(ByteBuffer buffer){
        checkNotNull(buffer);

        //System.out.println("write:"+buffer);

        pending.add(buffer);
    }

    public void writeAndFlush(ByteBuffer buffer) {
        write(buffer);
        reactor.taskQueue.add(this);
        flush();
    }

    public ByteBuffer next() {
        if (current == null) {
            current = pending.poll();
        } else {
            if (!current.hasRemaining()) {
                current = null;
            }
        }

        return current;
    }
}
