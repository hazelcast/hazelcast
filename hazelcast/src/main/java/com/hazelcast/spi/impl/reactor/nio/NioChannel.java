package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioChannel extends Channel {

    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();
    public Connection connection;
    public ByteBuffer readBuffer;
    public SocketChannel socketChannel;
    public NioReactor reactor;
    public long buffersWritten = 0;
    public long packetsRead = 0;
    public long bytesRead = 0;
    public long bytesWritten = 0;
    public final PacketIOHelper packetReader = new PacketIOHelper();
    public ByteBuffer[] writeBuffs = new ByteBuffer[128];
    public int writeBuffLen = 0;
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

    public String toDebugString() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        StringBuffer sb = new StringBuffer(dtf.format(now) + " " + this + " ");

        sb.append("pending=" + pending.size()).append(' ');
        sb.append("written=" + buffersWritten).append(' ');
        sb.append("read=" + packetsRead).append(' ');
        sb.append("bytes-written=" + bytesWritten).append(' ');
        sb.append("bytes-read=" + bytesRead).append(' ');
//        if(currentWriteBuff == null){
//            sb.append("currentWriteBuff=null");
//        }else{
//            sb.append(IOUtil.toDebugString("currentWriteBuff", currentWriteBuff));
//        }
        sb.append(" ");
        if (readBuffer == null) {
            sb.append("readBuff=null");
        } else {
            sb.append(IOUtil.toDebugString("readBuff", readBuffer));
        }
        return sb.toString();
    }
}
