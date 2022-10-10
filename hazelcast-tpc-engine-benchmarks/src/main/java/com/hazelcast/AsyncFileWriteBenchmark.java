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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.BufferUtil.toPageAlignedAddress;
import static com.hazelcast.internal.tpc.util.OS.pageSize;

public class AsyncFileWriteBenchmark {
    private static String path = System.getProperty("user.home");

    public static long operationsPerThread = 10 * 1000 * 1000;
    public static int concurrencyPerThread = 64;
    public static int openFlags = AsyncFile.O_CREAT | AsyncFile.O_DIRECT | AsyncFile.O_WRONLY;
    public static int blockSize = 4096;
    public static int threadCount = 1;

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
        long rawAddress = addressOf(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        for (int k = 0; k < buffer.capacity() / 2; k++) {
            buffer.putChar('a');
        }

        StorageDeviceRegistry storageDeviceRegistry = new StorageDeviceRegistry();

        IOUringReactorBuilder configuration = new IOUringReactorBuilder();
        //configuration.setFlags(IORING_SETUP_IOPOLL);
        configuration.setThreadAffinity(threadAffinity);
        // configuration.setSpin(true);
        configuration.setStorageDeviceRegistry(storageDeviceRegistry);
        IOUringReactor eventloop = new IOUringReactor(configuration);
        eventloop.start();

        List<AsyncFile> files = new ArrayList<>();
        files.add(createFile(eventloop, path));
        //files.add(createFile(eventloop, "/mnt/testdrive1"));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operationsPerThread / concurrencyPerThread;

        CountDownLatch completionLatch = new CountDownLatch(concurrencyPerThread);
        eventloop.offer(() -> {
            for (int k = 0; k < concurrencyPerThread; k++) {
                PWriteLoop loop = new PWriteLoop();
                loop.latch = completionLatch;
                loop.bufferAddress = bufferAddress;
                loop.sequentialOperations = sequentialOperations;
                loop.eventloop = eventloop;
                loop.file = files.get(k % files.size());
                eventloop.offer(loop);
            }
        });

        return completionLatch;
    }

    private static AsyncFile createFile(IOUringReactor eventloop, String dir) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.offer(() -> {
            AsyncFile file = eventloop.eventloop().newAsyncFile(randomTmpFile(dir));
            file.open(openFlags, AsyncFile.PERMISSIONS_ALL)
                    .then((o, o2) -> file.fallocate(0, 0, 100 * 1024L * pageSize())
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
        private IOUringReactor eventloop;

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
        public void accept(Integer result, Throwable throwable) {
            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            eventloop.offer(this);
        }
    }

    private static String randomTmpFile(String dir) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        return dir + separator + uuid;
    }
}
