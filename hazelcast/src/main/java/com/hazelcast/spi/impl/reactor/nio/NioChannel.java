package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.spi.impl.reactor.Channel;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class NioChannel extends Channel {

    public final static List<NioChannel> channels = new CopyOnWriteArrayList<>();
    public final static MonitorThread thread = new MonitorThread();

    static {
        thread.start();
    }

    public long prevPacketsRead;
    public boolean remoteAddress;
    public long prevBytesRead;

    public final static class MonitorThread extends Thread {
        public void run() {
            for (; ; ) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                for (NioChannel channel : channels) {
                    System.out.println("channel.scheduled:" + channel.scheduled
                            + " channel.pending:" + channel.pending.size()
                            + " channel.bytesWritten:" + channel.bytesWritten
                            + " channel.bytesRead:" + channel.bytesRead
                            + " reactor.wakeup-needed:" + channel.reactor.wakeupNeeded
                            + " reactor.task-queue:" + channel.reactor.runQueue.size());
                }
            }
        }
    }

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

    public NioChannel() {
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

        // todo: it could be that there are byte buffers we didn't manage to write
        // so we would end up with dirty work being undetected
        if (pending.isEmpty()) {
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
