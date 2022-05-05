package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.util.CircularQueue;
import com.hazelcast.tpc.util.SlabAllocator;
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

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static net.smacke.jaydio.OpenFlags.O_DIRECT;
import static net.smacke.jaydio.OpenFlags.O_RDONLY;


public class StorageScheduler {

    public static final File dummyFile = new File("/run/media/pveentjer/Samsung 980 Pro/blob");

    private final SlabAllocator<IoRequest> reqAllocator;

    private final IOUringEventloop loop;
    private final IOUringSubmissionQueue sq;
    private final Map<File, FileScheduler> fileSchedulers = new HashMap<>();
    private final int pageSize = DirectIoLib.getpagesize();
    private final int maxConcurrency;
    private final CircularQueue<IoRequest> parkedQueue;
    private int concurrent;

    public StorageScheduler(IOUringEventloop loop, int maxConcurrency) {
        this.loop = loop;
        this.sq = loop.sq;
        this.maxConcurrency = checkPositive("maxConcurrency", maxConcurrency);
        this.reqAllocator = new SlabAllocator<>(maxConcurrency, IoRequest::new);
        this.parkedQueue = new CircularQueue<>(maxConcurrency);

        registerFile(dummyFile);
    }

    // todo: we can do actual registration on the rb.
    public void registerFile(File file) {
        checkNotNull(file);

        try {
            int fd = DirectIoLib.open(file.getAbsolutePath(), O_DIRECT | O_RDONLY);
            if (fd < 0) {
                throw new RuntimeException("Can't open file [" + file.getAbsolutePath() + "]");
            }

            System.out.println("fileDescriptor:" + fd);

            FileScheduler requestsPerFile = new FileScheduler(fd);
            fileSchedulers.put(file, requestsPerFile);
            loop.completionListeners.put(fd, requestsPerFile);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void unregisterFile(File file) {
        checkNotNull(file);
    }

    private class FileScheduler implements CompletionListener {
        private final IntObjectMap<IoRequest> requests = new IntObjectHashMap<>(maxConcurrency);
        private final int fd;
        private short counter = 0;
        private ByteBuffer buffer = ByteBuffer.allocateDirect(DirectIoLib.getpagesize() * 2);

        private FileScheduler(int fd) {
            this.fd = fd;
        }

        @Override
        public void handle(int fd, int res, int flags, byte op, short data) {
            if (res < 0) {
                System.out.println("Res:" + res);
            }

            IoRequest req = requests.remove(data);
            if (req != null) {
                try {
                    // todo: improved exception handling.
                    req.op.handle_IORING_OP_READ(res, flags);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                req.clear();
                reqAllocator.free(req);
            }

            req = parkedQueue.poll();
            if (req == null) {
                concurrent--;
            } else {
                FileScheduler fileScheduler = fileSchedulers.get(req.file);
                fileScheduler.load(req);
            }
        }

        private void load(IoRequest req) {

            //Frame frame = op.response;

            //System.out.println("page size: "+DirectIoLib.getpagesize());

            //ByteBuffer buffer = frame.byteBuffer();

            short someId;
            do {
                someId = counter++;
            } while (requests.containsKey((int) someId));

            requests.put((int) someId, req);

            long bufferAddress = Buffer.memoryAddress(buffer);

            int bufferAddressMod = (int) (bufferAddress % pageSize);

            // System.out.println("buffer address:" + bufferAddress);

            long offset = pageSize * ThreadLocalRandom.current().nextInt(10000);

            sq.addRead(fd, offset, bufferAddress + pageSize - bufferAddressMod, 0, pageSize, someId);
        }
    }

    public void scheduleLoad(File file, long offset, int length, RandomLoadOp op) {
        IoRequest ioRequest = reqAllocator.allocate();
        ioRequest.op = op;
        ioRequest.file = file;
        ioRequest.offset = offset;
        ioRequest.length = length;

        if (concurrent == maxConcurrency) {
            // there are too many requests, lets park it.
            parkedQueue.add(ioRequest);
        } else {
            // there are not too many requests, lets execute it.
            concurrent++;
            FileScheduler fileScheduler = fileSchedulers.get(file);
            fileScheduler.load(ioRequest);
        }
    }

    private static class IoRequest {
        File file;
        long offset;
        int length;
        RandomLoadOp op;

        void clear() {
            file = null;
            op = null;
        }
    }
}
