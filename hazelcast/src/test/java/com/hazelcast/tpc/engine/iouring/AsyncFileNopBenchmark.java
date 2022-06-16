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

public class AsyncFileNopBenchmark {

    public static final long operations = 200000000L;
    public static final int concurrency = 100;
    public static final int openFlags = AsyncFile.O_CREAT | AsyncFile.O_DIRECT | AsyncFile.O_WRONLY;

    public static void main(String[] args) throws Exception {
        IORequestScheduler requestScheduler = new IORequestScheduler(16384);
        requestScheduler.registerStorageDevice("/mnt/testdrive1", 100, 16384);

        IOUringConfiguration configuration = new IOUringConfiguration();
        configuration.setThreadAffinity(new ThreadAffinity("1"));
        configuration.setSpin(true);
        configuration.setIoRequestScheduler(requestScheduler);
        IOUringEventloop eventloop = new IOUringEventloop(configuration);
        eventloop.start();

        List<AsyncFile> files = new ArrayList<>();
        //files.add(initFile(eventloop, "/home/pveentjer"));
        files.add(initFile(eventloop, "/mnt/testdrive1"));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operations / concurrency;

        CountDownLatch latch = new CountDownLatch(concurrency);
        eventloop.execute(() -> {
            for (int k = 0; k < concurrency; k++) {
                NopLoop loop = new NopLoop();
                loop.latch = latch;
                loop.sequentialOperations = sequentialOperations;
                loop.eventloop = eventloop;
                loop.file = files.get(k % files.size());
                eventloop.execute(loop);
            }
        });

        latch.await();
        long duration = System.currentTimeMillis() - startMs;
        System.out.println((operations * 1000f / duration) + " IOPS");
        System.exit(0);
    }

    private static AsyncFile initFile(IOUringEventloop eventloop, String dir) throws ExecutionException, InterruptedException {
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

    private static class NopLoop implements Runnable, BiConsumer<Object, Throwable> {
        private long count;
        private AsyncFile file;
        private long sequentialOperations;
        private CountDownLatch latch;
        private IOUringEventloop eventloop;

        @Override
        public void run() {
            if (count < sequentialOperations) {
                count++;
                file.nop().then(this).releaseOnComplete();
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
