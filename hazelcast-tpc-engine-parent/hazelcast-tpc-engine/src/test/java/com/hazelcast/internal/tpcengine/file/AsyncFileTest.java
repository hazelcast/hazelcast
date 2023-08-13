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

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertSuccessEventually;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDONLY;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_WRONLY;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpcengine.file.FileTestSupport.randomTmpFile;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public abstract class AsyncFileTest {

    private Reactor reactor;

    public abstract Reactor newReactor();

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
    }

    @Test
    public void test_1B() {
        run(1);
    }

    @Test
    public void test_2B() {
        run(2);
    }

    @Test
    public void test_1KB() {
        run(1024);
    }

    @Test
    public void test_2KB() {
        run(2048);
    }

    @Test
    public void test_4KB() {
        run(4096);
    }

    @Test
    public void test_8KB() {
        run(8192);
    }

    @Test
    public void test_64KB() {
        run(64 * 1024);
    }

    @Test
    public void test_128KB() {
        run(128 * 1024);
    }

    @Test
    public void test_256KB() {
        run(256 * 1024);
    }

    @Test
    public void test_512KB() {
        run(512 * 1024);
    }

    @Test
    public void test_1MB() {
        run(1024 * 1024);
    }

    public void run(int size) {
        CompletableFuture<Long> future = new CompletableFuture<Long>();
        File tmpFile = randomTmpFile(size);

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());

            file.open(O_RDONLY, PERMISSIONS_ALL).then((integer, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                }

                future.complete(file.size());
            });

        };
        reactor.offer(task);

        assertSuccessEventually(future);
        assertEquals(new Long(size), future.join());
    }

//    @Test
//    public void test_fileSize_whenNotOpened() {
//        CompletableFuture<UncheckedIOException> future = new CompletableFuture<>();
//        String path = randomTmpFile();
//
//        Runnable task = () -> {
//            AsyncFile file = reactor.eventloop().newAsyncFile(path);
//
//            try{
//                System.out.println(file.size());
//                future.completeExceptionally(new Exception("An IOException should have been thrown"));
//            }catch (UncheckedIOException e){
//                future.complete(e);
//            }catch (Throwable e){
//                future.completeExceptionally(e);
//            }
//        };
//        reactor.offer(task);
//
//        assertSuccessEventually(future);
//        assertNotNull(future.join());
//    }

    @Test
    public void testOpen() {
        File tmpFile = randomTmpFile();

        CompletableFuture future = new CompletableFuture();
        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());

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
        File tmpFile = randomTmpFile();

        CompletableFuture future = new CompletableFuture();

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());
            IOBuffer buffer = reactor.eventloop().storageAllocator().allocate(pageSize());
            for (int k = 0; k < buffer.capacity() / 2; k++) {
                buffer.writeChar('a');
            }
            buffer.flip();

            file.open(O_WRONLY | O_CREAT, PERMISSIONS_ALL).then((integer, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {
                    file.pwrite(0, 20, buffer).then((result, throwable2) -> {
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
        File tmpFile = randomTmpFile();
        FileTestSupport.write(tmpFile, "1234");

        CompletableFuture future = new CompletableFuture();
        Runnable task = () -> {
            Eventloop eventloop = reactor.eventloop();
            IOBuffer buffer = eventloop.storageAllocator().allocate(pageSize());
            AsyncFile file = eventloop.newAsyncFile(tmpFile.getAbsolutePath());

            file.open(O_RDONLY, PERMISSIONS_ALL).then((result1, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {

                    file.pread(0, 4096, buffer).then((result2, throwable2) -> {
                        if (throwable2 != null) {
                            future.completeExceptionally(throwable2);
                        } else {
                            buffer.clear();
                            future.complete(null);
                        }
                    });
                }
            });
        };
        reactor.offer(task);

        assertSuccessEventually(future);
    }
}
