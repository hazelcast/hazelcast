package com.hazelcast.spi.impl.engine.iouring;

import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.table.impl.RandomLoadOp;
import io.netty.channel.unix.Buffer;

import io.netty.incubator.channel.uring.IOUringSubmissionQueue;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import net.smacke.jaydio.DirectIoLib;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static net.smacke.jaydio.OpenFlags.O_DIRECT;
import static net.smacke.jaydio.OpenFlags.O_RDONLY;

public class IOUringScheduler {

    public static final File dummyFile = new File("/run/media/pveentjer/Samsung 980 Pro/blob");

    private final IOUringEventloop loop;
    private final IOUringSubmissionQueue sq;
    private final Map<File, FileScheduler> requestsPerFileMap = new HashMap<>();
    private final int pageSize = DirectIoLib.getpagesize();

    public IOUringScheduler(IOUringEventloop loop) {
        this.loop = loop;
        this.sq = loop.sq;

        registerFile(dummyFile);
    }

    // todo: we can do actual registration on the rb.
    public void registerFile(File file) {
        try {
            int fd = DirectIoLib.open(file.getAbsolutePath(), O_DIRECT | O_RDONLY);
            if (fd < 0) {
                throw new RuntimeException("Can't open file [" + file.getAbsolutePath() + "]");
            }

            System.out.println("fileDescriptor:" + fd);

            FileScheduler requestsPerFile = new FileScheduler(fd);
            requestsPerFileMap.put(file, requestsPerFile);
            loop.completionListeners.put(fd, requestsPerFile);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class FileScheduler implements CompletionListener {
        final IntObjectMap<RandomLoadOp> completionListeners = new IntObjectHashMap<>(4096);
        final int fd;
        short counter = 0;
        ByteBuffer buffer = ByteBuffer.allocateDirect(DirectIoLib.getpagesize()*2);

        public FileScheduler(int fd) {
            this.fd = fd;
        }

        @Override
        public void handle(int fd, int res, int flags, byte op, short data) {
            if(res<0){
                System.out.println("Res:"+res);
            }

            RandomLoadOp randomLoadOp = completionListeners.remove(data);
            if (randomLoadOp != null) {
                randomLoadOp.handle_IORING_OP_READ(res, flags);
            }
        }

        void load(RandomLoadOp op, long offset, int length) {
            //Frame frame = op.response;

            //System.out.println("page size: "+DirectIoLib.getpagesize());

            //ByteBuffer buffer = frame.byteBuffer();

            short someId = counter++;
            completionListeners.put(someId, op);

            long bufferAddress = Buffer.memoryAddress(buffer);

            int bufferAddressMod = (int)(bufferAddress % pageSize);

           // System.out.println("buffer address:" + bufferAddress);

            offset = pageSize * ThreadLocalRandom.current().nextInt(10000);

            sq.addRead(fd, offset, bufferAddress + pageSize - bufferAddressMod, 0, pageSize, someId);
        }
    }

    public void scheduleLoad(File file, long offset, int length, RandomLoadOp op) {
        FileScheduler requestsPerFile = requestsPerFileMap.get(file);
        requestsPerFile.load(op, offset, length);
    }
}
