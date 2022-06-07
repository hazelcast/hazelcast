package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.tpc.engine.AsyncFile;
import com.hazelcast.tpc.engine.Promise;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.tpc.engine.AsyncFile.pageSize;
import static com.hazelcast.tpc.util.Util.toPageAlignedAddress;

public class AsyncFileReadBenchmark {

    public static long operations = 10*1000*1000;
    public static int concurrency = 32;
    public static int openFlags = AsyncFile.O_CREAT | AsyncFile.O_DIRECT | AsyncFile.O_RDONLY;
    public static int blockSize = 4096;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IOUringConfiguration configuration = new IOUringConfiguration();
        configuration.setThreadAffinity(new ThreadAffinity("1"));
        configuration.setSpin(true);
        IOUringEventloop eventloop = new IOUringEventloop(configuration);
        eventloop.start();

        List<AsyncFile> files = new ArrayList<>();
        //files.add(initFile(eventloop, "/home/pveentjer"));
        files.add(initFile(eventloop, "/mnt/testdrive1/readonly"));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operations / concurrency;

        CountDownLatch latch = new CountDownLatch(concurrency);
        eventloop.execute(new Runnable() {
            @Override
            public void run() {
                for (int k = 0; k < concurrency; k++) {
                    PReadLoop loop = new PReadLoop();
                    loop.latch = latch;
                    loop.file = files.get(k % files.size());
                    loop.sequentialOperations = sequentialOperations;
                    loop.eventloop = eventloop;
                    eventloop.execute(loop);
                }
            }
        });

        latch.await();
        long duration = System.currentTimeMillis() - startMs;
        System.out.println((operations * 1000f / duration) + " IOPS");
        long dataSize = blockSize * operations;
        System.out.println("Bandwidth: "+(dataSize*1000/(duration*1024*1024))+" MB/s");
        System.exit(0);
    }

    private static AsyncFile initFile(IOUringEventloop eventloop, String path) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.execute(() -> {
            AsyncFile file = eventloop.unsafe().newAsyncFile(path);
            file.open(openFlags).then(new BiConsumer() {
                @Override
                public void accept(Object o, Object o2) {
                    initFuture.complete(file);
                }
            });
        });

        return initFuture.get();
    }

    private static class PReadLoop implements Runnable, BiConsumer<Object, Throwable> {
        private long count;
        private AsyncFile file;
        private long sequentialOperations;
        private CountDownLatch latch;
        private IOUringEventloop eventloop;

        ByteBuffer buffer = ByteBuffer.allocateDirect(20 * pageSize());
        long rawAddress = Buffer.memoryAddress(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        @Override
        public void run() {
            if (count < sequentialOperations) {
                count++;
                long offset = ThreadLocalRandom.current().nextInt(10 * 1024) * blockSize;

                file.pread(offset, blockSize, bufferAddress).then(this).releaseOnComplete();
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


    private static String randomTmpFile() {
        String dir = "/home/pveentjer";
        //System.getProperty("java.io.tmpdir");
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();

        return dir + separator + uuid;
    }

}
