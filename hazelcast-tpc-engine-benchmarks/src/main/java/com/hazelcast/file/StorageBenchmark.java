/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.file;

import com.hazelcast.internal.tpcengine.Promise;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.AsyncFileMetrics;
import com.hazelcast.internal.tpcengine.file.StorageDeviceRegistry;
import com.hazelcast.internal.tpcengine.util.BufferUtil;
import com.hazelcast.internal.util.ThreadAffinity;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.text.CharacterIterator;
import java.text.DecimalFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpcengine.ReactorBuilder.newReactorBuilder;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_NOATIME;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDWR;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.allocateDirect;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A benchmark for storage devices that tests the AsyncFile on top of the io_uring integration.
 * <p>
 * For a FIO based benchmark so you can compare
 * fio --name=write_throughput --numjobs=1 --filesize=4M --time_based --runtime=60s --ramp_time=2s --ioengine=io_uring  --direct=1 --verify=0 --bs=4k --iodepth=64 --rw=write --group_reporting=1 --cpus_allowed=1
 * <p>
 * To see IOPS and bandwidth:
 * iostat -dx 1 /dev/nvme1n1p1  (and then use the appropriate drive)
 * <p>
 * <p>
 * future reference:
 * https://hackmd.io/@dingpf/HyL_1b0m5
 * <p>
 * Note:
 * The parameters for the StorageBenchmark resemble the parameters of FIO to make it easier to
 * set up a benchmark that compares the tools.
 * <p>
 * Results:
 * <p>
 * ------------------------
 * Random read iodepth=64
 * FIO: 521k IOPS fio --name=write_throughput --numjobs=1 --filesize=4M --time_based --runtime=60s --ramp_time=2s --ioengine=io_uring  --direct=1 --verify=0 --bs=4k --iodepth=64 --rw=randread --group_reporting=1 --cpus_allowed=1
 * HZ:  727K IOPS
 * <p>
 * ------------------------
 * Sequential read iodepth=64
 * FIO: 523k IOPS  fio --name=write_throughput --numjobs=1 --filesize=4M --time_based --runtime=60s --ramp_time=2s --ioengine=io_uring  --direct=1 --verify=0 --bs=4k --iodepth=64 --rw=read --group_reporting=1 --cpus_allowed=1
 * HZ:  708k IOPS
 * <p>
 * ------------------------
 * Random write iodepth=64
 * FIO:  243k IOPS fio --name=write_throughput --numjobs=1 --filesize=4M --time_based --runtime=60s --ramp_time=2s --ioengine=io_uring  --direct=1 --verify=0 --bs=4k --iodepth=64 --rw=randwrite --group_reporting=1 --cpus_allowed=1
 * HZ:   573k IOPS
 * <p>
 * ------------------------
 * Sequential write iodepth=64
 * FIO: 320k  IOPS fio --name=write_throughput --numjobs=1 --filesize=4M --time_based --runtime=60s --ramp_time=2s --ioengine=io_uring  --direct=1 --verify=0 --bs=4k --iodepth=64 --rw=write --group_reporting=1 --cpus_allowed=1
 * HZ:  548k IOPS
 */
public class StorageBenchmark {

    public static final int READWRITE_NOP = 1;
    public static final int READWRITE_WRITE = 2;
    public static final int READWRITE_READ = 3;
    public static final int READWRITE_RANDWRITE = 4;
    public static final int READWRITE_RANDREAD = 5;

    // Properties
    public long operationCount;
    // the number of threads.
    public int numJobs;
    public String affinity;
    public boolean spin;
    // the number of concurrent tasks in 1 thread.
    public int iodepth;
    public long fileSize;
    public int bs;
    public String drive;
    // the type of workload.
    public int readwrite;
    public boolean enableMonitor;
    public boolean deleteFilesOnExit;
    public boolean direct;
    // Can only be io_uring for now because nio doesn't support files.
    public ReactorType reactorType;
    public int fsync;
    public int fdatasync;

    private final List<String> filePaths = new ArrayList<>();
    private final List<AsyncFile> asyncFiles = Collections.synchronizedList(new ArrayList<>());
    private final StorageDeviceRegistry storageDeviceRegistry = new StorageDeviceRegistry();
    private final ArrayList<Reactor> reactors = new ArrayList<>();

    public static void main(String[] args) {
        StorageBenchmark benchmark = new StorageBenchmark();
        benchmark.operationCount = 10 * 1000 * 1000L;
        benchmark.affinity = "1";
        benchmark.numJobs = 1;
        benchmark.iodepth = 64;
        benchmark.fileSize = 4 * 1024 * 1024L;
        benchmark.bs = 4 * 1024;
        benchmark.drive = "/mnt/benchdrive1/";
        benchmark.readwrite = READWRITE_READ;
        benchmark.enableMonitor = true;
        benchmark.deleteFilesOnExit = true;
        benchmark.direct = true;
        benchmark.spin = false;
        benchmark.reactorType = ReactorType.IOURING;
        benchmark.fsync = 0;
        benchmark.fdatasync = 0;
        benchmark.run();
    }

    public void run() {
        if (fsync > 0 && fdatasync > 0) {
            System.out.println("fsync and fdatasync can't both be larger than 0");
        }

        try {
            setup();

            MonitorThread monitorThread = null;

            CountDownLatch completionLatch = new CountDownLatch(iodepth * numJobs);
            CountDownLatch startLatch = new CountDownLatch(1);
            for (int jobIndex = 0; jobIndex < numJobs; jobIndex++) {
                Reactor reactor = reactors.get(jobIndex);
                int finalReactorIndex = jobIndex;
                reactor.offer(() -> {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    String path = filePaths.get(finalReactorIndex % filePaths.size());
                    AsyncFile file = reactor.eventloop().newAsyncFile(path);
                    asyncFiles.add(file);
                    int openFlags = O_RDWR | O_NOATIME;
                    if (direct) {
                        openFlags |= O_DIRECT;
                    }

                    file.open(openFlags, PERMISSIONS_ALL).then((integer, throwable) -> {
                        if (throwable != null) {
                            throwable.printStackTrace();
                            System.exit(1);
                        }

                        for (int k = 0; k < iodepth; k++) {
                            reactor.offer(new IOTask(this, file, completionLatch, k));
                        }
                    });
                });
            }

            if (enableMonitor) {
                monitorThread = new MonitorThread();
                monitorThread.start();
            }

            System.out.println("Benchmark: started");
            startLatch.countDown();
            long startMs = System.currentTimeMillis();

            completionLatch.await();
            if (enableMonitor) {
                monitorThread.shutdown();
            }

            long durationMs = System.currentTimeMillis() - startMs;
            System.out.println("Benchmark: completed");

            printResults(durationMs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            teardown();
            System.exit(0);
        }
    }

    private void setup() throws Exception {
        long startMs = System.currentTimeMillis();
        System.out.println("Setup: starting");
        ThreadAffinity threadAffinity = affinity == null ? null : new ThreadAffinity(affinity);
        // storageDeviceRegistry.register(dir, 512, 512);

        for (int k = 0; k < numJobs; k++) {
            ReactorBuilder builder = newReactorBuilder(reactorType);
            //builder.setFlags(IORING_SETUP_IOPOLL);
            builder.setThreadAffinity(threadAffinity);
            builder.setSpin(spin);
            builder.setStorageDeviceRegistry(storageDeviceRegistry);

            Reactor reactor = builder.build();
            reactors.add(reactor);
            reactor.start();
        }

        setupFiles();

        long durationMs = System.currentTimeMillis() - startMs;
        System.out.println("Setup: done [duration=" + durationMs + " ms]");
    }


    private static class IOTask implements Runnable, BiConsumer<Integer, Throwable> {
        private long completed;
        private final AsyncFile file;
        private long operationCount;
        private final CountDownLatch completionLatch;
        private long block;
        private final long dst;
        private final ByteBuffer buffer;
        private final Random random = new Random();
        private final int bs;
        private final long blockCount;
        private final int readwrite;
        private int syncCounter;
        private final int syncInterval;
        private boolean fdatasync;

        public IOTask(StorageBenchmark benchmark, AsyncFile file, CountDownLatch completionLatch, int taskIndex) {
            this.readwrite = benchmark.readwrite;
            this.file = file;
            this.completionLatch = completionLatch;
            this.operationCount = benchmark.operationCount/benchmark.iodepth;
            this.bs = benchmark.bs;
            this.blockCount = benchmark.fileSize / bs;
            if (benchmark.fsync > 0) {
                this.syncInterval = benchmark.fsync;
                this.fdatasync = false;
            } else if (benchmark.fdatasync > 0) {
                this.syncInterval = benchmark.fdatasync;
                this.fdatasync = true;
            } else {
                this.syncInterval = 0;
            }

            this.block = (blockCount * taskIndex) / benchmark.iodepth;
            //System.out.println("block:"+block);
            // Setting up the buffer
            this.buffer = allocateDirect(bs, pageSize());
            this.dst = BufferUtil.addressOf(buffer);
            for (int c = 0; c < buffer.capacity() / 2; c++) {
                buffer.putChar('c');
            }
        }

        @Override
        public void run() {
            Promise<Integer> p;
            switch (readwrite) {
                case READWRITE_NOP: {
                    p = file.nop();
                }
                break;
                case READWRITE_WRITE: {
                    if (syncInterval > 0) {
                        if (syncCounter == 0) {
                            syncCounter = syncInterval;
                            p = fdatasync ? file.fdatasync() : file.fsync();
                            break;
                        } else {
                            syncCounter--;
                        }
                    }

                    long offset = block * bs;
                    block++;
                    if (block > blockCount) {
                        block = 0;
                    }
                    p = file.pwrite(offset, bs, dst);
                }
                break;
                case READWRITE_READ: {
                    long offset = block * bs;
                    block++;
                    if (block > blockCount) {
                        block = 0;
                    }
//
//                    if (completed < 100) {
//                        System.out.println(offset);
//                    }

                    p = file.pread(offset, bs, dst);
                }
                break;
                case READWRITE_RANDWRITE: {
                    if (syncInterval > 0) {
                        if (syncCounter == 0) {
                            syncCounter = syncInterval;
                            p = fdatasync ? file.fdatasync() : file.fsync();
                            break;
                        } else {
                            syncCounter--;
                        }
                    }

                    long nextBlock = nextLong(random, blockCount);
                    long offset = nextBlock * bs;
                    p = file.pwrite(offset, bs, dst);
                }
                break;
                case READWRITE_RANDREAD: {
                    long nextBlock = nextLong(random, blockCount);
                    long offset = nextBlock * bs;
                    p = file.pread(offset, bs, dst);
                }
                break;
                default:
                    throw new RuntimeException("Unknown workload");
            }

            p.then(this).releaseOnComplete();
        }

        @Override
        public void accept(Integer result, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            if (completed >= operationCount) {
                completionLatch.countDown();
            } else {
                completed++;
                run();
            }
        }
    }

    private void setupFiles() throws InterruptedException {
        int iodepth = 128;
        CountDownLatch completionLatch = new CountDownLatch(iodepth * numJobs);

        for (int jobIndex = 0; jobIndex < numJobs; jobIndex++) {
            Reactor reactor = reactors.get(jobIndex);

            reactor.offer(() -> {
                String path = randomTmpFile(drive);
                System.out.println("Creating file " + path);
                filePaths.add(path);
                AsyncFile file = reactor.eventloop().newAsyncFile(path);

                int openFlags = O_RDWR | O_NOATIME | O_DIRECT | O_CREAT;
                long startMs = System.currentTimeMillis();
                file.open(openFlags, PERMISSIONS_ALL).then((integer, throwable) -> {
                    if (throwable != null) {
                        throwable.printStackTrace();
                        System.exit(1);
                    }

                    AtomicInteger completed = new AtomicInteger(iodepth);
                    for (int k = 0; k < iodepth; k++) {
                        InitFileTask initFileTask = new InitFileTask(k, iodepth);
                        initFileTask.startMs = startMs;
                        initFileTask.completionLatch = completionLatch;
                        initFileTask.completed = completed;
                        initFileTask.file = file;
                        reactor.offer(initFileTask);
                    }
                });
            });
        }

        completionLatch.await();
    }

    private void teardown() {
        long startMs = System.currentTimeMillis();
        System.out.println("Teardown: starting");

        try {
            for (Reactor reactor : reactors) {
                reactor.shutdown();
            }

            for (Reactor reactor : reactors) {
                if (!reactor.awaitTermination(5, SECONDS)) {
                    throw new RuntimeException("Reactor " + reactor + " failed to terminate");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (deleteFilesOnExit) {
            for (String path : filePaths) {
                System.out.println("Deleting " + path);
                if (!new File(path).delete()) {
                    System.out.println("Failed to delete " + path);
                }
            }
        }

        long durationMs = System.currentTimeMillis() - startMs;
        System.out.println("Teardown: done [duration=" + durationMs + " ms]");
    }


    private class InitFileTask implements Runnable, BiConsumer<Integer, Throwable> {
        private final long startBlock;
        private final long endBlock;
        public AtomicInteger completed;
        public long startMs;
        private AsyncFile file;
        private CountDownLatch completionLatch;
        private long block;
        private final long bufferAddress;
        private final ByteBuffer buffer;
        private final int blockSize;
        private final long blockCount;

        public InitFileTask(int ioTaskIndex, int iodepth) {
            // Setting up the buffer
            this.buffer = allocateDirect(bs, pageSize());
            this.bufferAddress = BufferUtil.addressOf(buffer);
            for (int c = 0; c < buffer.capacity() / 2; c++) {
                buffer.putChar('c');
            }

            this.blockSize = StorageBenchmark.this.bs;
            long fileBlockCount = StorageBenchmark.this.fileSize / StorageBenchmark.this.bs;

            this.blockCount = fileBlockCount / iodepth;
            this.startBlock = (fileBlockCount * ioTaskIndex) / iodepth;
            this.block = startBlock;
            this.endBlock = startBlock + blockCount;
        }

        @Override
        public void run() {
            long offset = block * blockSize;
            block++;
            Promise<Integer> p = file.pwrite(offset, blockSize, bufferAddress);
            p.then(this).releaseOnComplete();
        }

        @Override
        public void accept(Integer result, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            if (block == endBlock) {
                if (completed.decrementAndGet() == 0) {
                    file.close().then((integer, throwable1) -> {
                        if (throwable1 != null) {
                            throwable1.printStackTrace();
                            System.exit(1);
                        }

                        long durationMs = System.currentTimeMillis() - startMs;
                        System.out.println("Creating file " + file.path() + " completed in " + durationMs + " ms");
                        completionLatch.countDown();
                    });
                } else {
                    completionLatch.countDown();
                }

                return;
            }

            run();
        }
    }

    private static long nextLong(Random random, long bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be positive");
        }

        long l = random.nextLong();
        if (l == Long.MIN_VALUE) {
            return 0;
        } else if (l < 0) {
            l = -l;
        }

        return l % bound;
    }

    private static String randomTmpFile(String dir) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        return dir + separator + uuid;
    }

    private void printResults(long durationMs) {
        DecimalFormat longFormat = new DecimalFormat("#,###");

        long totalOperations = operationCount * numJobs;

        System.out.println("Duration: " + longFormat.format(durationMs) + " ms");
        System.out.println("Reactors: " + numJobs);
        System.out.println("I/O depth: " + iodepth);
        System.out.println("Direct I/O: " + direct);
        System.out.println("Page size: " + pageSize() + " B");
        System.out.println("Operations: " + humanReadableCountSI(operationCount));
        System.out.println("fsync: " + fsync);
        System.out.println("fdatasync: " + fdatasync);
        System.out.println("Speed: " + humanReadableCountSI(totalOperations * 1000f / durationMs) + " IOPS");
        switch (readwrite) {
            case READWRITE_NOP:
                System.out.println("Workload: nop");
                break;
            case READWRITE_WRITE:
                System.out.println("Workload: sequential write");
                break;
            case READWRITE_READ:
                System.out.println("Workload: sequential read");
                break;
            case READWRITE_RANDWRITE:
                System.out.println("Workload: random write");
                break;
            case READWRITE_RANDREAD:
                System.out.println("Workload: random read");
                break;
            default:
                System.out.println("Workload: unknown");
        }
        System.out.println("File size: " + humanReadableByteCountSI(fileSize));
        System.out.println("Block size: " + bs+" B");
        long dataSize = bs * totalOperations;

        if (readwrite != READWRITE_NOP) {
            System.out.println("Read/Written: " + humanReadableByteCountSI(dataSize));
            System.out.println("Bandwidth: " + humanReadableByteCountSI(dataSize * 1000f / durationMs ) + "/s");

            long totalTimeMicros = MILLISECONDS.toMicros(numJobs * iodepth * durationMs);
            System.out.println("Average latency: " + (totalTimeMicros / totalOperations) + " us");
        }
    }

    private class MonitorThread extends Thread {
        private boolean shutdown;
        private final StringBuffer sb = new StringBuffer();

        private void shutdown() {
            shutdown = true;
            interrupt();
        }

        @Override
        public void run() {
            try {
                long lastMs = System.currentTimeMillis();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }

                LastAsyncFileMetrics[] metricsArray = new LastAsyncFileMetrics[asyncFiles.size()];
                for (int k = 0; k < metricsArray.length; k++) {
                    metricsArray[k] = new LastAsyncFileMetrics();
                }
                while (!shutdown) {
                    long nowMs = System.currentTimeMillis();

                    for (int k = 0; k < metricsArray.length; k++) {
                        AsyncFile file = asyncFiles.get(k);
                        AsyncFileMetrics metrics = file.getMetrics();
                        LastAsyncFileMetrics lastMetrics = metricsArray[k];

                        long reads = metrics.reads();
                        if (reads > 0) {
                            double readsThp = ((reads - lastMetrics.reads) * 1000d) / (nowMs - lastMs);
                            lastMetrics.reads = reads;
                            sb.append(" reads=");
                            sb.append(humanReadableCountSI(readsThp));
                            sb.append("/s");
                        }


                        long bytesRead = metrics.bytesRead();
                        if (bytesRead > 0) {
                            double bytesThp = ((bytesRead - lastMetrics.bytesRead) * 1000d) / (nowMs - lastMs);
                            lastMetrics.bytesRead = bytesRead;
                            sb.append(" read-bytes=");
                            sb.append(humanReadableByteCountSI(bytesThp));
                            sb.append("/s");
                        }
                        long writes = metrics.writes();
                        if (writes > 0) {
                            double writeThp = ((writes - lastMetrics.writes) * 1000d) / (nowMs - lastMs);
                            lastMetrics.writes = writes;
                            sb.append(" writes=");
                            sb.append(humanReadableCountSI(writeThp));
                            sb.append("/s");
                        }
                        long bytesWritten = metrics.bytesWritten();
                        if (bytesWritten > 0) {
                            double bytesThp = ((bytesWritten - lastMetrics.bytesWritten) * 1000d) / (nowMs - lastMs);
                            lastMetrics.bytesWritten = bytesWritten;
                            sb.append(" write-bytes=");
                            sb.append(humanReadableByteCountSI(bytesThp));
                            sb.append("/s");
                        }
                        long fsyncs = metrics.fsyncs();
                        if (fsyncs > 0) {
                            double fsyncsThp = ((fsyncs - lastMetrics.fsyncs) * 1000d) / (nowMs - lastMs);
                            lastMetrics.fsyncs = fsyncs;
                            sb.append(" fsyncs=");
                            sb.append(humanReadableCountSI(fsyncsThp));
                            sb.append("/s");
                        }
                        long fdatasyncs = metrics.fdatasyncs();
                        if (fdatasyncs > 0) {
                            double fdataSyncsThp = ((fdatasyncs - lastMetrics.fdatasyncs) * 1000d) / (nowMs - lastMs);
                            lastMetrics.fdatasyncs = fdatasyncs;
                            sb.append(" fdatasyncs=");
                            sb.append(humanReadableCountSI(fdataSyncsThp));
                            sb.append("/s");
                        }


                        long nops = metrics.nops();
                        if (nops > 0) {
                            double nopsThp = ((nops - lastMetrics.nops) * 1000d) / (nowMs - lastMs);
                            lastMetrics.nops = nops;
                            sb.append(" nops=");
                            sb.append(humanReadableCountSI(nopsThp));
                            sb.append("/s");
                        }

                        System.out.println(sb);
                        sb.setLength(0);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                    lastMs = nowMs;
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    private static String humanReadableByteCountSI(double bytes) {
        if (Double.isInfinite(bytes)) {
            return "Infinite";
        }

        if (-1000 < bytes && bytes < 1000) {
            return bytes + "B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.2f %cB", bytes / 1000.0, ci.current());
    }

    private static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + "B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.2f %cB", bytes / 1000.0, ci.current());
    }

    private static String humanReadableCountSI(double count) {
        if (Double.isInfinite(count)) {
            return "Infinite";
        }

        if (-1000 < count && count < 1000) {
            return String.valueOf(count);
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (count <= -999_950 || count >= 999_950) {
            count /= 1000;
            ci.next();
        }
        return String.format("%.2f%c", count / 1000.0, ci.current());
    }

    private static class LastAsyncFileMetrics {
        private long reads;
        private long writes;
        private long fsyncs;
        private long nops;
        private long fdatasyncs;
        private long bytesRead;
        private long bytesWritten;
    }
}
