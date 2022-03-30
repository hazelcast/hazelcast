package io.netty.incubator.channel.uring;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class IO_UringChannel extends Channel {

    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();

    public Connection connection;
    public LinuxSocket socket;
    public IO_UringReactor reactor;
    public SocketAddress remoteAddress;
    public ByteBuf readBuff;
    public ByteBuffer current;
    public ByteBuf writeBuff;
    public ByteBuffer readBuffer;
    public PacketIOHelper packetReader = new PacketIOHelper();
    public InetSocketAddress localAddress;
    public long packetsRead;
    public long bytesRead = 0;
    public long packetsWritten;
    public AtomicBoolean scheduled = new AtomicBoolean();

    @Override
    public void flush() {
        if (!scheduled.get() && scheduled.compareAndSet(false, true)) {
            reactor.schedule(this);
        }
    }

    // called by the Reactor.
    public void unschedule() {
        if (!pending.isEmpty()) {
            reactor.taskQueue.add(this);
            return;
        }

        scheduled.set(false);
        if (!pending.isEmpty()) {
            if (scheduled.compareAndSet(false, true)) {
                reactor.taskQueue.add(this);
            }
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
