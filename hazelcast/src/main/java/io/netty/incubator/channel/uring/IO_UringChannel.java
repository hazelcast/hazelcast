package io.netty.incubator.channel.uring;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.IOUtil;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class IO_UringChannel {

    public final ConcurrentLinkedQueue<ByteBuffer> pending = new ConcurrentLinkedQueue<>();

    public Connection connection;
    public ByteBuffer readBuff;
    public LinuxSocket socket;
    public IO_UringReactor reactor;
    public long buffersWritten = 0;
    public long packetsRead = 0;
    public long bytesRead = 0;
    public long bytesWritten = 0;

    public ByteBuffer[] writeBuffs = new ByteBuffer[128];
    public int writeBuffLen = 0;

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

//    public ByteBuffer next() {
//        if (currentWriteBuff == null) {
//            currentWriteBuff = pending.poll();
//        } else {
//            if (!currentWriteBuff.hasRemaining()) {
//                buffersWritten++;
//                currentWriteBuff = null;
//            }
//        }
//
//        return currentWriteBuff;
//    }

    public String toDebugString() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        StringBuffer sb = new StringBuffer(dtf.format(now)+" "+this +" ");

        sb.append("pending="+pending.size()).append(' ');
        sb.append("written="+buffersWritten).append(' ');
        sb.append("read="+packetsRead).append(' ');
        sb.append("bytes-written="+bytesWritten).append(' ');
        sb.append("bytes-read="+bytesRead).append(' ');
//        if(currentWriteBuff == null){
//            sb.append("currentWriteBuff=null");
//        }else{
//            sb.append(IOUtil.toDebugString("currentWriteBuff", currentWriteBuff));
//        }
        sb.append(" ");
        if(readBuff == null){
            sb.append("readBuff=null");
        }else{
            sb.append(IOUtil.toDebugString("readBuff", readBuff));
        }
        return sb.toString();
    }
}
