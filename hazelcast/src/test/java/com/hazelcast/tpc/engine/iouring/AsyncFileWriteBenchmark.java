package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.tpc.engine.AsyncFile;
import com.hazelcast.tpc.engine.iouring.IOUringEventloop.IOUringConfiguration;
import io.netty.channel.unix.Buffer;

import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import static com.hazelcast.tpc.engine.AsyncFile.pageSize;
import static com.hazelcast.tpc.util.Util.toPageAlignedAddress;

public class AsyncFileWriteBenchmark {

    public static long operationsPerThread = 1 * 1000 * 1000;
    public static int concurrencyPerThread = 64;
    public static int openFlags = AsyncFile.O_CREAT | AsyncFile.O_DIRECT | AsyncFile.O_WRONLY;
    public static int blockSize = 4096;
    public static int threadCount = 2;

    public static void main(String[] args) throws Exception {
        ThreadAffinity threadAffinity = new ThreadAffinity("1,3");

        long startMs = System.currentTimeMillis();

        List<CountDownLatch> latches = new ArrayList<>();
        for (int k = 0; k < threadCount; k++) {
            latches.add(run(threadAffinity));
        }

        for (CountDownLatch latch : latches) {
            latch.await();
        }

        long duration = System.currentTimeMillis() - startMs;
        long operations = threadCount * operationsPerThread;

        System.out.println((operations * 1000f / duration) + " IOPS");
        long dataSize = blockSize * operations;
        System.out.println("Bandwidth: " + (dataSize * 1000 / (duration * 1024 * 1024)) + " MB/s");
        System.exit(0);
    }

    public static CountDownLatch run(ThreadAffinity threadAffinity) throws ExecutionException, InterruptedException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * pageSize());
        long rawAddress = Buffer.memoryAddress(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        for (int k = 0; k < buffer.capacity() / 2; k++) {
            buffer.putChar('a');
        }

        IORequestScheduler requestScheduler = new IORequestScheduler(512);
        requestScheduler.registerStorageDevice("/mnt/testdrive1", 100, 512);

        IOUringConfiguration configuration = new IOUringConfiguration();
        //configuration.setFlags(IORING_SETUP_IOPOLL);
        configuration.setThreadAffinity(threadAffinity);
        configuration.setSpin(true);
        configuration.setIoRequestScheduler(requestScheduler);
        IOUringEventloop eventloop = new IOUringEventloop(configuration);
        eventloop.start();

        List<AsyncFile> files = new ArrayList<>();
        //files.add(initFile(eventloop, "/home/pveentjer"));
        files.add(createFile(eventloop, "/mnt/testdrive1"));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operationsPerThread / concurrencyPerThread;

        CountDownLatch completionLatch = new CountDownLatch(concurrencyPerThread);
        eventloop.execute(() -> {
            for (int k = 0; k < concurrencyPerThread; k++) {
                PWriteLoop loop = new PWriteLoop();
                loop.latch = completionLatch;
                loop.bufferAddress = bufferAddress;
                loop.sequentialOperations = sequentialOperations;
                loop.eventloop = eventloop;
                loop.file = files.get(k % files.size());
                eventloop.execute(loop);
            }
        });

        return completionLatch;
    }

    private static AsyncFile createFile(IOUringEventloop eventloop, String dir) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.execute(() -> {
            AsyncFile file = eventloop.unsafe().newAsyncFile(randomTmpFile(dir));
            file.open(openFlags)
                    .then((o, o2) -> file.fallocate(0, 0, 100 * 1024 * AsyncFile.pageSize())
                            .then((o1, o21) -> {
                                initFuture.complete(file);
                            }));
        });

        return initFuture.get();
    }

    private static class PWriteLoop implements Runnable, BiConsumer<Object, Throwable> {
        private long count;
        private AsyncFile file;
        private long bufferAddress;
        private long sequentialOperations;
        private CountDownLatch latch;
        private IOUringEventloop eventloop;

        @Override
        public void run() {
            if (count < sequentialOperations) {
                count++;

                long offset = ThreadLocalRandom.current().nextInt(10 * 1024) * blockSize;

                file.pwrite(offset, blockSize, bufferAddress)
                        .then(this)
                        .releaseOnComplete();
            } else {
                latch.countDown();
            }
        }

        @Override
        public void accept(Object o, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            eventloop.execute(this);
        }
    }

    private static String randomTmpFile(String dir) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        return dir + separator + uuid;
    }
}
