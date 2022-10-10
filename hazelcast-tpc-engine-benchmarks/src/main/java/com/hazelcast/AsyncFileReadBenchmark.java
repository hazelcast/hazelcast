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

public class AsyncFileReadBenchmark {
    private static String path = System.getProperty("user.home");

    public static long operations = 100 * 1000 * 1000;
    public static int concurrency = 1;
    public static int openFlags = AsyncFile.O_CREAT | AsyncFile.O_DIRECT | AsyncFile.O_RDONLY;
    public static int blockSize = 4096;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IOUringReactorBuilder configuration = new IOUringReactorBuilder();
        configuration.setThreadAffinity(new ThreadAffinity("1"));
        configuration.setSpin(false);
        IOUringReactor eventloop = new IOUringReactor(configuration);
        eventloop.start();

        List<AsyncFile> files = new ArrayList<>();
        files.add(initFile(eventloop, path));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operations / concurrency;

        CountDownLatch latch = new CountDownLatch(concurrency);
        eventloop.offer(() -> {
            for (int k = 0; k < concurrency; k++) {
                PReadLoop loop = new PReadLoop();
                loop.latch = latch;
                loop.file = files.get(k % files.size());
                loop.sequentialOperations = sequentialOperations;
                loop.eventloop = eventloop;
                eventloop.offer(loop);
            }
        });

        latch.await();
        long duration = System.currentTimeMillis() - startMs;
        System.out.println((operations * 1000f / duration) + " IOPS");
        long dataSize = blockSize * operations;
        System.out.println("Bandwidth: " + (dataSize * 1000 / (duration * 1024 * 1024)) + " MB/s");
        System.exit(0);
    }

    private static AsyncFile initFile(IOUringReactor eventloop, String path) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.offer(() -> {
            AsyncFile file = eventloop.eventloop().newAsyncFile(path);
            file.open(openFlags, AsyncFile.PERMISSIONS_ALL).then(new BiConsumer() {
                @Override
                public void accept(Object o, Object o2) {
                    initFuture.complete(file);
                }
            });
        });

        return initFuture.get();
    }

    private static class PReadLoop implements Runnable, BiConsumer<Integer, Throwable> {
        private long count;
        private AsyncFile file;
        private long sequentialOperations;
        private CountDownLatch latch;
        private IOUringReactor eventloop;

        ByteBuffer buffer = ByteBuffer.allocateDirect(20 * pageSize());
        long rawAddress = addressOf(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        @Override
        public void run() {
            System.out.println("foo");
            if (count < sequentialOperations) {
                count++;
                long offset = ThreadLocalRandom.current().nextInt(10 * 1024) * blockSize;

                //file.nop().then(this).releaseOnComplete();
                file.pread(offset, blockSize, bufferAddress).then(this).releaseOnComplete();

            } else {
                latch.countDown();
            }
        }

        @Override
        public void accept(Integer o, Throwable throwable) {
            System.out.println("Storage response received");

            if (throwable != null) {
                throwable.printStackTrace();
                System.exit(1);
            }

            eventloop.offer(this);
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
