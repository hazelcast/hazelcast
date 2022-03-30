package io.netty.incubator.channel.uring;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;
import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    @Override
    public void flush() {
        //todo: flush isn't needed when already scheduled for flushing
        reactor.wakeup();
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
