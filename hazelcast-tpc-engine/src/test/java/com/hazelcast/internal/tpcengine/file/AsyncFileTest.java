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

import com.hazelcast.internal.tpcengine.Reactor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertSuccessEventually;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_WRONLY;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.toPageAlignedAddress;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
import static org.junit.Assert.assertFalse;

public abstract class AsyncFileTest {

    private Reactor reactor;

    public abstract Reactor newReactor();

    private List<String> testFiles = new LinkedList<>();

    @Before
    public void before() {
        reactor = newReactor();
        reactor.start();
    }

    @After
    public void after() {
        if (reactor != null) {
            reactor.shutdown();
        }

        for (String testFile : testFiles) {
            File file = new File(testFile);
            file.delete();
        }
    }

    @Test
    public void testOpen() {
        CompletableFuture future = new CompletableFuture();
        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(randomTmpFile());

            assertFalse(new File(file.path()).exists());

            file.open(O_WRONLY | O_CREAT, PERMISSIONS_ALL).then((integer, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                } else {
                    future.complete(null);
                }
            });

        };
        reactor.offer(task);

        assertSuccessEventually(future);
    }

//    @Test
//    public void testFallocate() {
//        String path = randomTmpFile();
//        System.out.println(path);
//
//        CompletableFuture future = new CompletableFuture();
//        TestRunnable task = new TestRunnable() {
//            @Override
//            public void runInternal() {
//                AsyncFile file = reactor.eventloop().newAsyncFile(path);
//
//                file.open(O_WRONLY | O_CREAT, PERMISSIONS_ALL).then(new BiConsumer<Integer, Throwable>() {
//                    @Override
//                    public void accept(Integer integer, Throwable throwable1) {
//                        if(throwable1!=null){
//                            future.completeExceptionally(throwable1);
//                        }else{
//                            file.fallocate()
//                        }
//                    }
//                })
//
//                file.fallocate(0, 0, 10 * pageSize()).then((BiConsumer) (o, o2) -> {
//                    System.out.println("result:" + o);
//                    System.out.println("throwable:" + o2);
//
//                    file.close().then((BiConsumer) (o1, o21) -> completed.countDown());
//                });
//                assertTrue(new File(file.path()).exists());
//
//                System.out.println(file.path());
//            }
//        };
//        reactor.offer(task);
//
//        assertSuccessEventually(future);
//    }

    @Test
    public void testWrite() {
        String path = randomTmpFile();
        System.out.println(path);

        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * pageSize());
        long rawAddress = addressOf(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        for (int k = 0; k < buffer.capacity() / 2; k++) {
            buffer.putChar('a');
        }

        CompletableFuture future = new CompletableFuture();

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(path);

            file.open(O_WRONLY | O_CREAT, PERMISSIONS_ALL).then((integer, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {
                    file.pwrite(0, 20, bufferAddress).then((result, throwable2) -> {
                        if (throwable2 != null) {
                            future.completeExceptionally(throwable2);
                        } else {
                            file.close().then((integer1, throwable3) -> {
                                if (throwable3 != null) {
                                    future.completeExceptionally(throwable3);
                                } else {
                                    future.complete(null);
                                }
                            });
                        }
                    });
                }
            });
        };
        reactor.offer(task);

        assertSuccessEventually(future);
    }

    @Test
    public void testRead() {
        String path = randomTmpFile();
        write(path, "1234");


        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * pageSize());
        long rawAddress = addressOf(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);


        CompletableFuture future = new CompletableFuture();
        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(path);

            file.open(AsyncFile.O_RDONLY, PERMISSIONS_ALL).then((result1, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {
                    file.pread(0, 4096, bufferAddress).then((result2, throwable2) -> {
                        if (throwable2 != null) {
                            future.completeExceptionally(throwable2);
                        } else {
                            future.complete(null);
                        }
                    });
                }
            });
        };
        reactor.offer(task);

        assertSuccessEventually(future);
    }

    private String randomTmpFile() {
        String tmpdir = System.getProperty("java.io.tmpdir");
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String separator = FileSystems.getDefault().getSeparator();
        String path = tmpdir + separator + uuid;
        testFiles.add(path);
        return path;
    }

    private void write(String path, String content) {
        try {
            FileWriter myWriter = new FileWriter(path);
            myWriter.write(content);
            myWriter.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
