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

import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.util.OS.pageSize;

public class AsyncFileNopBenchmark {
    private static String path = System.getProperty("user.home");

    public static long operations = 10000l * 1000 * 1000;
    public static final int concurrency = 100;
    public static final int openFlags = AsyncFile.O_CREAT | AsyncFile.O_DIRECT | AsyncFile.O_WRONLY;

    public static void main(String[] args) throws Exception {
        StorageDeviceRegistry storageDeviceRegistry = new StorageDeviceRegistry();
        //storageDeviceRegistry.registerStorageDevice(path, 100, 16384);

        IOUringReactorBuilder configuration = new IOUringReactorBuilder();
        configuration.setThreadAffinity(new ThreadAffinity("1"));
        //  configuration.setSpin(true);
        configuration.setStorageDeviceRegistry(storageDeviceRegistry);
        IOUringReactor eventloop = new IOUringReactor(configuration);
        eventloop.start();

        List<AsyncFile> files = new ArrayList<>();
        //files.add(initFile(eventloop, "/home/pveentjer"));
        files.add(initFile(eventloop, path));

        System.out.println("Init done");

        long startMs = System.currentTimeMillis();

        System.out.println("Starting benchmark");

        long sequentialOperations = operations / concurrency;

        CountDownLatch latch = new CountDownLatch(concurrency);
        eventloop.offer(() -> {
            for (int k = 0; k < concurrency; k++) {
                NopLoop loop = new NopLoop();
                loop.latch = latch;
                loop.sequentialOperations = sequentialOperations;
                loop.eventloop = eventloop;
                loop.file = files.get(k % files.size());
                eventloop.offer(loop);
            }
        });

        latch.await();
        long duration = System.currentTimeMillis() - startMs;
        System.out.println((operations * 1000 / duration) + " IOPS");
        System.exit(0);
    }

    private static AsyncFile initFile(IOUringReactor eventloop, String dir) throws ExecutionException, InterruptedException {
        CompletableFuture<AsyncFile> initFuture = new CompletableFuture<>();
        eventloop.offer(() -> {
            AsyncFile file = eventloop.eventloop().newAsyncFile(randomTmpFile(dir));
            file.open(openFlags, AsyncFile.PERMISSIONS_ALL)
                    .then((o, o2) -> file.fallocate(0, 0, 100 * 1024 * pageSize())
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
        private IOUringReactor eventloop;

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

            eventloop.offer(this);
        }
    }

    private static String randomTmpFile(String dir) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        return dir + separator + uuid;
    }
}
