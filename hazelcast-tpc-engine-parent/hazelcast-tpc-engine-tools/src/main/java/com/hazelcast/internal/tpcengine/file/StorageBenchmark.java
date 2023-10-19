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

package com.hazelcast.internal.tpcengine.file;

import com.hazelcast.internal.tpcengine.FormatUtil;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.TpcTestSupport;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.nio.NioReactor;
import com.hazelcast.internal.tpcengine.util.IntBiConsumer;
import com.hazelcast.internal.tpcengine.util.IntPromise;
import com.hazelcast.internal.util.ThreadAffinity;

import java.io.File;
import java.nio.file.FileSystems;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.tpcengine.FormatUtil.humanReadableByteCountSI;
import static com.hazelcast.internal.tpcengine.Reactor.Builder.newReactorBuilder;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_NOATIME;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDWR;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
import static java.lang.System.currentTimeMillis;
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
    // the number of threads.
    public int numJobs;
    public String affinity;
    public boolean spin;
    // the number of concurrent tasks in 1 thread.
    public int iodepth;
    public long fileSize;
    public int bs;
    public List<String> directories = new ArrayList<>();
    // the type of workload.
    public int readwrite;
    public boolean deleteFilesOnExit;
    public boolean direct;
    // Can only be io_uring for now because nio doesn't support files.
    public ReactorType reactorType;
    public int fsync;
    public int fdatasync;
    public int runtimeSeconds;
    // the executor used to process storage requests when the Nio reactor is used.
    public Executor nioStorageExecutor = Executors.newCachedThreadPool();

    //  private final Map<Reactor, List<AsyncFile>> filesMap = new ConcurrentHashMap<>();
    private final Map<Reactor, List<String>> pathsMap = new ConcurrentHashMap<>();

    private final ArrayList<Reactor> reactors = new ArrayList<>();
    private static volatile boolean stop;

    public static void main(String[] args) {
        StorageBenchmark benchmark = new StorageBenchmark();
        benchmark.runtimeSeconds = 600;
        benchmark.affinity = "1";
        benchmark.numJobs = 1;
        benchmark.iodepth = 100;
        benchmark.fileSize = 4 * 1024 * 1024L;
        benchmark.bs = 4 * 1024;
        benchmark.directories.add("/home/pveentjer");
        //benchmark.directories.add("/mnt/benchdrive1");
        //benchmark.directories.add("/mnt/benchdrive2");
        //benchmark.directories.add("/mnt/benchdrive3");
        benchmark.readwrite = READWRITE_WRITE;
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

        printConfig();
        try {
            setup();

            CountDownLatch startLatch = new CountDownLatch(1);
            for (int jobIndex = 0; jobIndex < numJobs; jobIndex++) {
                Reactor reactor = reactors.get(jobIndex);
                reactor.offer(() -> {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    AtomicInteger opened = new AtomicInteger();
                    List<AsyncFile> files = new ArrayList<>();

                    for (String path : pathsMap.get(reactor)) {
                        AsyncFile file = reactor.eventloop().newAsyncFile(path);
                        files.add(file);
                        int openFlags = O_RDWR | O_NOATIME;
                        if (direct) {
                            openFlags |= O_DIRECT;
                        }

                        IntPromise openPromise = new IntPromise(reactor.eventloop());
                        file.open(openPromise, openFlags, PERMISSIONS_ALL);
                        openPromise.then((integer, throwable) -> {
                            if (throwable != null) {
                                throwable.printStackTrace();
                                System.exit(1);
                            }

                            opened.incrementAndGet();

                            if (opened.get() == files.size()) {
                                // when the last file is opened, we schedule the IOTasks.
                                for (int k = 0; k < iodepth; k++) {
                                    reactor.offer(new IOTask(reactor, this, files, k));
                                }
                            }
                        });
                    }

                });
            }

            MonitorThread monitorThread = new MonitorThread();
            monitorThread.start();

            System.out.println("Benchmark: started");
            startLatch.countDown();
            long startMs = System.currentTimeMillis();

            monitorThread.join();
            long durationMs = System.currentTimeMillis() - startMs;
            System.out.println("Benchmark: completed");

            printResults(durationMs);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            teardown();
        }
    }

    private void printConfig() {
        System.out.println("Reactor:" + reactorType);
        System.out.println("Duration: " + runtimeSeconds + " seconds.");
        System.out.println("numJobs: " + numJobs);
        System.out.println("affinity: " + affinity);
        System.out.println("spin: " + spin);
        System.out.println("iodepth: " + iodepth);
        System.out.println("fileSize: " + fileSize);

        System.out.println("blockSize: " + bs);
        System.out.println("directories: " + directories);
        System.out.println("readwrite: " + readwrite);
        System.out.println("deleteFilesOnExit: " + deleteFilesOnExit);
        System.out.println("fsync: " + fsync);
        System.out.println("fdatasync: " + fdatasync);
    }

    private void setup() throws Exception {
        long startMs = System.currentTimeMillis();
        System.out.println("Setup: starting");
        ThreadAffinity threadAffinity = affinity == null ? null : new ThreadAffinity(affinity);

        for (int k = 0; k < numJobs; k++) {
            Reactor.Builder reactorBuilder = newReactorBuilder(reactorType);
            reactorBuilder.threadAffinity = threadAffinity;
            //reactorBuilder.spin = spin;
            if (reactorBuilder instanceof NioReactor.Builder) {
                NioReactor.Builder nioReactorBuilder = (NioReactor.Builder) reactorBuilder;
                nioReactorBuilder.storageExecutor = nioStorageExecutor;
            }
            Reactor reactor = reactorBuilder.build();
            reactors.add(reactor);
            reactor.start();
        }

        setupFiles();

        long durationMs = System.currentTimeMillis() - startMs;
        System.out.println("Setup: done [duration=" + durationMs + " ms]");
    }

    private static class IOTask implements Runnable, IntBiConsumer<Throwable> {
        private final List<AsyncFile> files;
        private int fileIndex;
        private long block;
        private final IOBuffer writeBuffer;
        private final IOBuffer readBuffer;
        private final Random random = new Random();
        private final int bs;
        private final long blockCount;
        private final int readwrite;
        private int syncCounter;
        private final int syncInterval;
        private boolean fdatasync;

        public IOTask(Reactor reactor, StorageBenchmark benchmark, List<AsyncFile> files, int taskIndex) {
            this.readwrite = benchmark.readwrite;
            this.files = files;
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
            // Setting up the buffer
            this.writeBuffer = reactor.eventloop().storageAllocator().allocate(bs);
            for (int c = 0; c < writeBuffer.capacity() / 2; c++) {
                writeBuffer.writeChar('c');
            }
            writeBuffer.flip();
            this.readBuffer = reactor.eventloop().storageAllocator().allocate(bs);
        }

        @Override
        public void run() {
            AsyncFile file = files.get(fileIndex);
            fileIndex++;
            if (fileIndex == files.size()) {
                fileIndex = 0;
            }

            switch (readwrite) {
                case READWRITE_NOP: {
                    file.nop(this);
                }
                break;
                case READWRITE_WRITE: {
                    if (syncInterval > 0) {
                        if (syncCounter == 0) {
                            syncCounter = syncInterval;
                            if (fdatasync) {
                                file.fdatasync(this);
                            } else {
                                file.fsync(this);
                            }
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
                    writeBuffer.position(0);
                    file.pwrite(this, offset, bs, writeBuffer);
                }
                break;
                case READWRITE_READ: {
                    long offset = block * bs;
                    block++;
                    if (block > blockCount) {
                        block = 0;
                    }

                    readBuffer.position(0);
                    file.pread(this, offset, bs, readBuffer);
                }
                break;
                case READWRITE_RANDWRITE: {
                    if (syncInterval > 0) {
                        if (syncCounter == 0) {
                            syncCounter = syncInterval;
                            if (fdatasync) {
                                file.fdatasync(this);
                            } else {
                                file.fsync(this);
                            }
                            break;
                        } else {
                            syncCounter--;
                        }
                    }

                    long nextBlock = nextLong(random, blockCount);
                    long offset = nextBlock * bs;
                    writeBuffer.position(0);
                    file.pwrite(this, offset, bs, writeBuffer);
                }
                break;
                case READWRITE_RANDREAD: {
                    long nextBlock = nextLong(random, blockCount);
                    long offset = nextBlock * bs;
                    readBuffer.position(0);
                    file.pread(this, offset, bs, readBuffer);
                }
                break;
                default:
                    throw new RuntimeException("Unknown workload");
            }
        }

        @Override
        public void accept(int result, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            if (stop) {
                return;
            }

            run();
        }
    }

    private void setupFiles() throws InterruptedException {
        int iodepth = 128;

        CountDownLatch completionLatch = new CountDownLatch(iodepth * numJobs);

        for (int jobIndex = 0; jobIndex < numJobs; jobIndex++) {
            Reactor reactor = reactors.get(jobIndex);

            reactor.offer(() -> {
                List<String> paths = new ArrayList<>();
                for (String directory : directories) {
                    String path = randomTmpFile(directory);

                    System.out.println("Creating file " + path);
                    AsyncFile file = reactor.eventloop().newAsyncFile(path);
                    paths.add(path);

                    int openFlags = O_RDWR | O_NOATIME | O_DIRECT | O_CREAT;

                    long startMs = System.currentTimeMillis();
                    IntPromise openPromise = new IntPromise(reactor.eventloop());
                    file.open(openPromise, openFlags, PERMISSIONS_ALL);
                    openPromise.then((integer, throwable) -> {
                        if (throwable != null) {
                            throwable.printStackTrace();
                            System.exit(1);
                        }

                        AtomicInteger completed = new AtomicInteger(iodepth);
                        // we write content fo the file so that the file content actually exists.
                        // and we do it concurrently to speed up this process.
                        for (int k = 0; k < iodepth; k++) {
                            InitFileTask initFileTask = new InitFileTask(reactor, k, iodepth);
                            initFileTask.startMs = startMs;
                            initFileTask.completionLatch = completionLatch;
                            initFileTask.completed = completed;
                            initFileTask.file = file;
                            reactor.offer(initFileTask);
                        }
                    });
                }
                pathsMap.put(reactor, paths);
            });
        }

        completionLatch.await();
    }

    private void teardown() {
        long startMs = System.currentTimeMillis();
        System.out.println("Teardown: starting");

        try {
            TpcTestSupport.terminateAll(reactors);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (deleteFilesOnExit) {
            for (Reactor reactor : reactors) {
                for (String path : pathsMap.get(reactor)) {
                    System.out.println("Deleting " + path);
                    if (!new File(path).delete()) {
                        System.out.println("Failed to delete " + path);
                    }
                }
            }
        }

        long durationMs = System.currentTimeMillis() - startMs;
        System.out.println("Teardown: done [duration=" + durationMs + " ms]");
    }


    private class InitFileTask implements Runnable, IntBiConsumer<Throwable> {
        private final long startBlock;
        private final long endBlock;
        private final Reactor reactor;
        public AtomicInteger completed;
        public long startMs;
        private AsyncFile file;
        private CountDownLatch completionLatch;
        private long block;
        private final IOBuffer buffer;
        private final int blockSize;
        private final long blockCount;

        public InitFileTask(Reactor reactor, int ioTaskIndex, int iodepth) {
            // Setting up the buffer
            this.reactor = reactor;
            this.buffer = reactor.eventloop().storageAllocator().allocate(StorageBenchmark.this.bs);
            for (int c = 0; c < buffer.capacity() / 2; c++) {
                buffer.writeChar('c');
            }
            buffer.flip();

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

            file.pwrite(this, offset, blockSize, buffer);
        }

        @Override
        public void accept(int result, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            if (block == endBlock) {
                if (completed.decrementAndGet() == 0) {
                    IntPromise closePromise = new IntPromise(reactor.eventloop());
                    file.close(closePromise);
                    closePromise.then((integer, throwable1) -> {
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

            buffer.position(0);
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

        Metrics metrics = new Metrics();
        collect(metrics);
        System.out.println("Duration: " + longFormat.format(durationMs) + " ms");
        System.out.println("Reactors: " + numJobs);
        System.out.println("I/O depth: " + iodepth);
        System.out.println("Direct I/O: " + direct);
        System.out.println("Page size: " + pageSize() + " B");
        System.out.println("fsync: " + fsync);
        System.out.println("fdatasync: " + fdatasync);
        long totalTimeMicros = MILLISECONDS.toMicros(numJobs * iodepth * durationMs);

        System.out.println("Speed: " + FormatUtil.humanReadableCountSI(metrics.ops() * 1000f / durationMs) + " IOPS");
        System.out.println("File size: " + humanReadableByteCountSI(fileSize));
        System.out.println("Block size: " + bs + " B");
        switch (readwrite) {
            case READWRITE_NOP:
                System.out.println("Workload: nop");
                System.out.println("Nops: " + FormatUtil.humanReadableCountSI(metrics.ops()));
                System.out.println("Average latency: " + (totalTimeMicros / metrics.ops()) + " us");
                break;
            case READWRITE_WRITE:
                System.out.println("Workload: sequential read");
                System.out.println("Writes: " + humanReadableByteCountSI(metrics.bytesWritten));
                System.out.println("Bandwidth: " + humanReadableByteCountSI(metrics.bytesWritten * 1000f / durationMs) + "/s");
                System.out.println("Average latency: " + (totalTimeMicros / metrics.ops()) + " us");
                break;
            case READWRITE_READ:
                System.out.println("Workload: sequential read");
                System.out.println("Read: " + humanReadableByteCountSI(metrics.bytesRead));
                System.out.println("Bandwidth: " + humanReadableByteCountSI(metrics.bytesRead * 1000f / durationMs) + "/s");
                System.out.println("Average latency: " + (totalTimeMicros / metrics.ops()) + " us");
                break;
            case READWRITE_RANDWRITE:
                System.out.println("Workload: random write");
                System.out.println("Write: " + humanReadableByteCountSI(metrics.bytesWritten));
                System.out.println("Bandwidth: " + humanReadableByteCountSI(metrics.bytesWritten * 1000f / durationMs) + "/s");
                System.out.println("Average latency: " + (totalTimeMicros / metrics.ops()) + " us");
                break;
            case READWRITE_RANDREAD:
                System.out.println("Workload: random read");
                System.out.println("Read: " + humanReadableByteCountSI(metrics.bytesRead));
                System.out.println("Bandwidth: " + humanReadableByteCountSI(metrics.bytesRead * 1000f / durationMs) + "/s");
                System.out.println("Average latency: " + (totalTimeMicros / metrics.ops()) + " us");
                break;
            default:
                System.out.println("Workload: unknown");
        }
    }

    private class MonitorThread extends Thread {
        private final StringBuffer sb = new StringBuffer();

        public MonitorThread() {
            super("MonitorThread");
        }

        @Override
        public void run() {
            try {
                run0();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            stop = true;
        }

        private void run0() throws Exception {
            long runtimeMs = SECONDS.toMillis(runtimeSeconds);
            long startMs = currentTimeMillis();
            long endMs = startMs + runtimeMs;
            long lastMs = startMs;

            Metrics lastMetrics = new Metrics();
            Metrics metrics = new Metrics();
            while (System.currentTimeMillis() < endMs) {
                Thread.sleep(1000);

                long nowMs = System.currentTimeMillis();
                long durationMs = nowMs - lastMs;
                collect(metrics);

                printEtd(nowMs, startMs);

                printEta(endMs, nowMs);

                printReads(metrics, lastMetrics, durationMs);

                printBytesRead(metrics, lastMetrics, durationMs);

                printWrites(metrics, lastMetrics, durationMs);

                printBytesWritten(metrics, lastMetrics, durationMs);

                printFsyncs(metrics, lastMetrics, durationMs);

                printFDataSyncs(metrics, lastMetrics, durationMs);

                printNops(metrics, lastMetrics, durationMs);

                printTaskQueueCsCount(metrics, lastMetrics, durationMs);

                printTaskCsCount(metrics, lastMetrics, durationMs);

                printIoSchedulerTicks(metrics, lastMetrics, durationMs);

                printParkCount(metrics, lastMetrics, durationMs);

                printParkTimeNanos(metrics, lastMetrics);

                System.out.println(sb);
                sb.setLength(0);

                Metrics tmp = lastMetrics;
                lastMetrics = metrics;
                metrics = tmp;
                lastMs = nowMs;
            }
        }

        private void printTaskQueueCsCount(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.taskQueueCsCount > 0) {
                double thp = ((metrics.taskQueueCsCount - lastMetrics.taskQueueCsCount) * 1000d) / durationMs;
                sb.append("[cs ");
                sb.append(FormatUtil.humanReadableCountSI(thp));
                sb.append("/s]");
            }
        }

        private void printIoSchedulerTicks(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.ioSchedulerTicks > 0) {
                double thp = ((metrics.ioSchedulerTicks - lastMetrics.ioSchedulerTicks) * 1000d) / durationMs;
                sb.append("[io-ticks ");
                sb.append(FormatUtil.humanReadableCountSI(thp));
                sb.append("/s]");
            }
        }

        private void printNops(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.nops > 0) {
                double nopsThp = ((metrics.nops - lastMetrics.nops) * 1000d) / durationMs;
                sb.append("[nop ");
                sb.append(FormatUtil.humanReadableCountSI(nopsThp));
                sb.append("/s]");
            }
        }

        private void printFsyncs(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.fsyncs > 0) {
                double fsyncsThp = ((metrics.fsyncs - lastMetrics.fsyncs) * 1000d) / durationMs;
                sb.append("[fsyncs ");
                sb.append(FormatUtil.humanReadableCountSI(fsyncsThp));
                sb.append("/s]");
            }
        }

        private void printBytesWritten(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.bytesWritten > 0) {
                double bytesThp = ((metrics.bytesWritten - lastMetrics.bytesWritten) * 1000d) / durationMs;
                sb.append("[write-bytes ");
                sb.append(humanReadableByteCountSI(bytesThp));
                sb.append("/s]");
            }
        }

        private void printReads(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.reads > 0) {
                double readsThp = ((metrics.reads - lastMetrics.reads) * 1000d) / durationMs;
                sb.append("[reads ");
                sb.append(FormatUtil.humanReadableCountSI(readsThp));
                sb.append("/s]");
            }
        }

        private void printEta(long endMs, long nowMs) {
            long eta = MILLISECONDS.toSeconds(endMs - nowMs);
            sb.append("[eta ");
            sb.append(eta / 60);
            sb.append("m:");
            sb.append(eta % 60);
            sb.append("s]");
        }

        private void printEtd(long nowMs, long startMs) {
            long completedSeconds = MILLISECONDS.toSeconds(nowMs - startMs);
            double completed = (100f * completedSeconds) / runtimeSeconds;
            sb.append("[etd ");
            sb.append(completedSeconds / 60);
            sb.append("m:");
            sb.append(completedSeconds % 60);
            sb.append("s ");
            sb.append(String.format("%,.3f", completed));
            sb.append("%]");
        }

        private void printTaskCsCount(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.taskCsCount > 0) {
                double thp = ((metrics.taskCsCount - lastMetrics.taskCsCount) * 1000d) / durationMs;
                sb.append("[task-cs ");
                sb.append(FormatUtil.humanReadableCountSI(thp));
                sb.append("/s]");
            }
        }

        private void printParkTimeNanos(Metrics metrics, Metrics lastMetrics) {
            if (metrics.parkTimeNanos > 0) {
                long parkCount = metrics.parkCount - lastMetrics.parkCount;
                double avg = (metrics.parkTimeNanos - lastMetrics.parkTimeNanos) / parkCount;
                sb.append("[avg-park ");
                sb.append(String.format("%.2f", avg));
                sb.append(" ns]");
            }
        }

        private void printParkCount(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.parkCount > 0) {
                double thp = ((metrics.parkCount - lastMetrics.parkCount) * 1000d) / durationMs;
                sb.append("[parks ");
                sb.append(FormatUtil.humanReadableCountSI(thp));
                sb.append("/s]");
            }
        }

        private void printFDataSyncs(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.fdatasyncs > 0) {
                double fdataSyncsThp = ((metrics.fdatasyncs - lastMetrics.fdatasyncs) * 1000d) / durationMs;
                sb.append("[fdatasyncs ");
                sb.append(FormatUtil.humanReadableCountSI(fdataSyncsThp));
                sb.append("/s]");
            }
        }

        private void printWrites(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.writes > 0) {
                double writeThp = ((metrics.writes - lastMetrics.writes) * 1000d) / durationMs;
                sb.append("[writes ");
                sb.append(FormatUtil.humanReadableCountSI(writeThp));
                sb.append("/s]");
            }
        }

        private void printBytesRead(Metrics metrics, Metrics lastMetrics, long durationMs) {
            if (metrics.bytesRead > 0) {
                double bytesThp = ((metrics.bytesRead - lastMetrics.bytesRead) * 1000d) / durationMs;
                sb.append("[read-bytes ");
                sb.append(humanReadableByteCountSI(bytesThp));
                sb.append("/s]");
            }
        }
    }

    private static class Metrics {
        private long ioSchedulerTicks;
        private long parkCount;
        private long reads;
        private long writes;
        private long fsyncs;
        private long nops;
        private long fdatasyncs;
        private long bytesRead;
        private long bytesWritten;
        private long taskCsCount;
        private long taskQueueCsCount;
        private int parkTimeNanos;

        private long ops() {
            return reads + writes + fsyncs + fdatasyncs + nops;
        }

        private void clear() {
            reads = 0;
            writes = 0;
            fsyncs = 0;
            nops = 0;
            fdatasyncs = 0;
            bytesRead = 0;
            bytesWritten = 0;
            taskCsCount = 0;
            taskQueueCsCount = 0;
            parkCount = 0;
            parkTimeNanos = 0;
            ioSchedulerTicks = 0;
        }
    }

    private void collect(Metrics target) {
        target.clear();

        for (Reactor reactor : reactors) {
            Reactor.Metrics reactorMetrics = reactor.metrics();
            target.taskCsCount += reactorMetrics.taskCsCount();
            target.taskQueueCsCount += reactorMetrics.taskQueueCsCount();
            target.ioSchedulerTicks += reactorMetrics.ioSchedulerTicks();
            target.parkCount += reactorMetrics.parkCount();
            target.parkTimeNanos += reactorMetrics.parkTimeNanos();
            reactor.files().foreach(f -> {
                AsyncFile.Metrics metrics = f.metrics();
                target.reads += metrics.reads();
                target.writes += metrics.writes();
                target.nops += metrics.nops();
                target.bytesWritten += metrics.bytesWritten();
                target.bytesRead += metrics.bytesRead();
                target.fsyncs += metrics.fsyncs();
                target.fdatasyncs += metrics.fdatasyncs();
            });
        }
    }
}
