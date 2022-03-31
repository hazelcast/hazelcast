package io.netty.incubator.channel.uring;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class IO_UringChannel extends Channel {
    public final static List<IO_UringChannel> channels = new CopyOnWriteArrayList<>();


    public long prevPacketsRead = 0;
    public long prevBytesRead = 0;
    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();
    public Connection connection;
    public LinuxSocket socket;
    public IO_UringReactor reactor;
    public SocketAddress remoteAddress;
    public ByteBuf receiveBuff;
    public ByteBuffer current;
    public ByteBuf sendBufferBuff;
    public ByteBuffer readBuffer;
    public PacketIOHelper packetReader = new PacketIOHelper();
    public InetSocketAddress localAddress;
    public long packetsRead;
    public long bytesRead = 0;
    public long packetsWritten;
    public AtomicBoolean scheduled = new AtomicBoolean(false);

    public IO_UringChannel() {
        channels.add(this);
    }

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
    public void write(ByteBuffer buffer) {
        pending.add(buffer);
    }

    @Override
    public void writeAndFlush(ByteBuffer buffer) {
        write(buffer);
        flush();
    }

    public ByteBuffer next() {
        if (current == null) {
            current = pending.poll();
        } else {
            if (!current.hasRemaining()) {
                //buffersWritten++;
                current = null;
            }
        }

        return current;
    }
}
