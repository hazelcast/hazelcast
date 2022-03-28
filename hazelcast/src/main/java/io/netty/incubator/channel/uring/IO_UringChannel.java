package io.netty.incubator.channel.uring;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.PacketIOHelper;
import io.netty.buffer.ByteBuf;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class IO_UringChannel {

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

    public void flush() {
        reactor.wakeup();
    }

    public void write(ByteBuffer buffer) {
        checkNotNull(buffer);

        //System.out.println("write:"+buffer);

        pending.add(buffer);
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

    public void writeAndFlush(ByteBuffer buffer) {
        write(buffer);
        reactor.taskQueue.add(this);
        flush();
    }
}
