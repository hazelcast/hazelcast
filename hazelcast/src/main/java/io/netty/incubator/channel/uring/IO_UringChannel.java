package io.netty.incubator.channel.uring;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;
import io.netty.buffer.ByteBuf;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class IO_UringChannel extends Channel {

    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();

    public Connection connection;
    public LinuxSocket socket;
    public IO_UringReactor reactor;
    public long bytesRead = 0;
    public SocketAddress address;
    public ByteBuf readBuff;
    public ByteBuffer currentWriteBuff;
    public ByteBuf writeBuff;
    public ByteBuffer readBuffer;
    public PacketIOHelper packetIOHelper = new PacketIOHelper();

    @Override
    public void flush() {
        reactor.wakeup();
    }

    @Override
    public void write(ByteBuffer buffer) {
        checkNotNull(buffer);

        //System.out.println("write:"+buffer);

        pending.add(buffer);
    }

    @Override
    public void writeAndFlush(ByteBuffer buffer) {
        write(buffer);
        reactor.taskQueue.add(this);
        flush();
    }

    public ByteBuffer next() {
        if (currentWriteBuff == null) {
            currentWriteBuff = pending.poll();
        } else {
            if (!currentWriteBuff.hasRemaining()) {
                //buffersWritten++;
                currentWriteBuff = null;
            }
        }

        return currentWriteBuff;
    }
}
