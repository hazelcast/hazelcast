/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast;


import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.iouring.IOUringReactor;
import com.hazelcast.internal.tpc.iouring.IOUringReactorBuilder;
import com.hazelcast.internal.tpc.iouring.StorageDeviceRegistry;
import com.hazelcast.internal.util.ThreadAffinity;

import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpc.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpc.AsyncFile.O_WRONLY;
import static com.hazelcast.internal.tpc.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.BufferUtil.toPageAlignedAddress;
import static com.hazelcast.internal.tpc.util.OS.pageSize;

/**
 *
 * For a FIO based benchmark so you can compare
 * fio --name=write_throughput --numjobs=64 --size=10G --time_based --runtime=60s --ramp_time=2s --ioengine=io_uring  --direct=1 --verify=0 --bs=4k --iodepth=64 --rw=write --group_reporting=1
 */
public class AsyncFileWriteBenchmark {
    private static String path = "/run/media/pveentjer/b72258e9-b9c9-4f7c-8b76-cef961eeec55";

    public static final long fileSize = 128 * pageSize();
    public static final long operationsPerThread = 24 * 1000 * 1000;
    public static final int concurrencyPerThread = 64;
    public static final int openFlags = O_CREAT | O_DIRECT | O_WRONLY;
    public static final int blockSize = pageSize();
    public static final int reactorCount = 1;

    public static void main(String[] args) throws Exception {
        ThreadAffinity threadAffinity = new ThreadAffinity("1,3");

        long startMs = System.currentTimeMillis();

        List<CountDownLatch> latches = new ArrayList<>();
        for (int k = 0; k < reactorCount; k++) {
            latches.add(run(threadAffinity));
        }

        for (CountDownLatch latch : latches) {
            latch.await();
        }

        long duration = System.currentTimeMillis() - startMs;
        long operations = reactorCount * operationsPerThread;

        System.out.println((operations * 1000f / duration) + " IOPS");
        long dataSize = blockSize * operations;
        System.out.println("Bandwidth: " + (dataSize * 1000 / (duration * 1024 * 1024)) + " MB/s");
        System.exit(0);
    }

    public static CountDownLatch run(ThreadAffinity threadAffinity) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * pageSize());
        long rawAddress = addressOf(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        for (int k = 0; k < buffer.capacity() / 2; k++) {
            buffer.putChar('a');
        }

        StorageDeviceRegistry storageDeviceRegistry = new StorageDeviceRegistry();
        storageDeviceRegistry.register("/run/media/pveentjer/b72258e9-b9c9-4f7c-8b76-cef961eeec55", 512, 512);

        IOUringReactorBuilder configuration = new IOUringReactorBuilder();
        //configuration.setFlags(IORING_SETUP_IOPOLL);
        configuration.setThreadAffinity(threadAffinity);
        // configuration.setSpin(true);
        configuration.setStorageDeviceRegistry(storageDeviceRegistry);

        IOUringReactor reactor = new IOUringReactor(configuration);
        reactor.start();

        List<AsyncFile> files = new ArrayList<>();
        files.add(createFile(reactor, path));
        //files.add(createFile(reactor, "/mnt/testdrive1"));

        System.out.println("Init done");

        System.out.println("Starting benchmark");

        long sequentialOperations = operationsPerThread / concurrencyPerThread;

        CountDownLatch completionLatch = new CountDownLatch(concurrencyPerThread);
        reactor.offer(() -> {
            for (int k = 0; k < concurrencyPerThread; k++) {
                PWriteLoop loop = new PWriteLoop();
                loop.latch = completionLatch;
                loop.bufferAddress = bufferAddress;
                loop.sequentialOperations = sequentialOperations;
                loop.reactor = reactor;
                loop.file = files.get(k % files.size());
                reactor.offer(loop);
            }
        });

        return completionLatch;
    }

    private static AsyncFile createFile(IOUringReactor eventloop, String dir) throws Exception {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.offer(() -> {
            AsyncFile file = eventloop.eventloop().newAsyncFile(randomTmpFile(dir));
            file.open(openFlags, PERMISSIONS_ALL)
                    .then((o, o2) -> file.fallocate(0, 0, fileSize)
                            .then((o1, o21) -> {
                                initFuture.complete(file);
                            }));
        });

        return initFuture.get();
    }

    private static class PWriteLoop implements Runnable, BiConsumer<Integer, Throwable> {
        private long count;
        private AsyncFile file;
        private long bufferAddress;
        private long sequentialOperations;
        private CountDownLatch latch;
        private IOUringReactor reactor;

        @Override
        public void run() {
            if (count < sequentialOperations) {
                count++;

                // todo: we can write outside of the file; so no good
                long offset = ThreadLocalRandom.current().nextInt(10 * 1024) * blockSize;

                file.pwrite(offset, blockSize, bufferAddress)
                        .then(this)
                        .releaseOnComplete();
            } else {
                latch.countDown();
            }
        }

        @Override
        public void accept(Integer result, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            reactor.offer(this);
        }
    }

    private static String randomTmpFile(String dir) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        return dir + separator + uuid;
    }
}
