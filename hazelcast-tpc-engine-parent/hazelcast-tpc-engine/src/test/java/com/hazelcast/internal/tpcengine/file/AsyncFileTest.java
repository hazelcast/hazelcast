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
import com.hazelcast.internal.tpcengine.util.IntPromise;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
    public void test_size() {
        CompletableFuture<Long> future = new CompletableFuture<>();
        int size = 1039;
        File tmpFile = randomTmpFile(size);

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());

            IntPromise openPromise = new IntPromise(reactor.eventloop());
            file.open(openPromise, O_RDONLY, PERMISSIONS_ALL);
            openPromise.then((integer, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                }

                future.complete(file.size());
            });

        };
        reactor.offer(task);

        assertSuccessEventually(future);
        assertEquals(Long.valueOf(size), future.join());
    }

    @Test
    public void test_delete() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        int size = 1039;
        File tmpFile = randomTmpFile(size);

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());

            IntPromise openPromise = new IntPromise(reactor.eventloop());
            file.open(openPromise, O_RDONLY, PERMISSIONS_ALL);
            openPromise.then((integer, throwable) -> {
                try {
                    file.delete();
                    future.complete(0);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });

        };
        reactor.offer(task);

        assertSuccessEventually(future);
        assertFalse(tmpFile.exists());
        assertEquals(Integer.valueOf(0), future.join());
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

            IntPromise openPromise = new IntPromise(reactor.eventloop());
            file.open(openPromise, O_WRONLY | O_CREAT, PERMISSIONS_ALL);
            openPromise.then((integer, throwable) -> {
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
    public void testNop() throws ExecutionException, InterruptedException {
        File tmpFile = randomTmpFile();

        CompletableFuture future = new CompletableFuture();
        CompletableFuture<AsyncFile> fileFuture = new CompletableFuture<>();

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());
            fileFuture.complete(file);

            IntPromise openPromise = new IntPromise(reactor.eventloop());
            file.open(openPromise, O_WRONLY | O_CREAT, PERMISSIONS_ALL);
            openPromise.then((integer, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {
                    IntPromise nopPromise = new IntPromise(reactor.eventloop());
                    file.nop(nopPromise);
                    nopPromise.then((result, throwable2) -> {
                        if (throwable2 != null) {
                            future.completeExceptionally(throwable2);
                        } else {
                            IntPromise closePromise = new IntPromise(reactor.eventloop());
                            file.close(closePromise);
                            closePromise.then((integer1, throwable3) -> {
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
        AsyncFile file = fileFuture.get();

        AsyncFile.Metrics metrics = file.metrics();
        assertEquals(1, metrics.nops());
        assertEquals(0, metrics.fsyncs());
        assertEquals(0, metrics.fdatasyncs());
        assertEquals(0, metrics.reads());
        assertEquals(0, metrics.bytesRead());
        assertEquals(0, metrics.writes());
        assertEquals(0, metrics.bytesWritten());
    }


    @Test
    public void testWrite() throws ExecutionException, InterruptedException {
        File tmpFile = randomTmpFile();

        CompletableFuture future = new CompletableFuture();
        CompletableFuture<AsyncFile> fileFuture = new CompletableFuture<>();

        IOBuffer buffer = reactor.eventloop().storageAllocator().allocate(pageSize());
        Random random = new Random();
        for (int k = 0; k < buffer.capacity(); k++) {
            buffer.writeByte((byte) random.nextInt());
        }
        buffer.flip();

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());
            fileFuture.complete(file);

            IntPromise openPromise = new IntPromise(reactor.eventloop());
            file.open(openPromise, O_WRONLY | O_CREAT, PERMISSIONS_ALL);
            openPromise.then((integer, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {
                    IntPromise writePromise = new IntPromise(reactor.eventloop());
                    file.pwrite(writePromise, 0, buffer.remaining(), buffer);
                    writePromise.then((result, throwable2) -> {
                        if (throwable2 != null) {
                            future.completeExceptionally(throwable2);
                        } else {
                            IntPromise closePromise = new IntPromise(reactor.eventloop());
                            file.close(closePromise);
                            closePromise.then((integer1, throwable3) -> {
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
        AsyncFile file = fileFuture.get();

        AsyncFile.Metrics metrics = file.metrics();
        assertEquals(0, metrics.nops());
        assertEquals(0, metrics.fsyncs());
        assertEquals(0, metrics.fdatasyncs());
        assertEquals(0, metrics.reads());
        assertEquals(0, metrics.bytesRead());
        assertEquals(1, metrics.writes());
        assertEquals(buffer.capacity(), metrics.bytesWritten());
    }

    @Test
    public void testRead() throws ExecutionException, InterruptedException {
        int pageSize = pageSize();

        File tmpFile = randomTmpFile();
        byte[] bytes = new byte[4 * pageSize];
        new Random().nextBytes(bytes);
        FileTestSupport.write(tmpFile, new String(bytes));

        CompletableFuture future = new CompletableFuture();
        CompletableFuture<AsyncFile> fileFuture = new CompletableFuture<>();
        Runnable task = () -> {
            Eventloop eventloop = reactor.eventloop();
            IOBuffer buffer = eventloop.storageAllocator().allocate(pageSize);
            AsyncFile file = eventloop.newAsyncFile(tmpFile.getAbsolutePath());
            fileFuture.complete(file);

            IntPromise openPromise = new IntPromise(reactor.eventloop());
            file.open(openPromise, O_RDONLY, PERMISSIONS_ALL);
            openPromise.then((result1, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {
                    IntPromise readPromise = new IntPromise(reactor.eventloop());
                    file.pread(readPromise, 0, buffer.remaining(), buffer);
                    readPromise.then((result2, throwable2) -> {
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
        AsyncFile file = fileFuture.get();
        AsyncFile.Metrics metrics = file.metrics();
        assertEquals(0, metrics.nops());
        assertEquals(0, metrics.fsyncs());
        assertEquals(0, metrics.fdatasyncs());
        assertEquals(1, metrics.reads());
        assertEquals(pageSize, metrics.bytesRead());
        assertEquals(0, metrics.writes());
        assertEquals(0, metrics.bytesWritten());
    }

    @Test
    public void testFSync() throws Exception {
        testFSync(true);
    }

    @Test
    public void testFDataSync() throws Exception {
        testFSync(false);
    }

    // The effects of an fsync can't easily be tested so we just check if the metrics
    // are updated and no exceptions are thrown.
    public void testFSync(boolean fsync) throws Exception {
        File tmpFile = randomTmpFile();

        CompletableFuture future = new CompletableFuture();
        CompletableFuture<AsyncFile> fileFuture = new CompletableFuture<>();

        Runnable task = () -> {
            AsyncFile file = reactor.eventloop().newAsyncFile(tmpFile.getAbsolutePath());
            fileFuture.complete(file);

            IntPromise openPromise = new IntPromise(reactor.eventloop());
            file.open(openPromise, O_WRONLY | O_CREAT, PERMISSIONS_ALL);
            openPromise.then((integer, throwable1) -> {
                if (throwable1 != null) {
                    future.completeExceptionally(throwable1);
                } else {
                    IntPromise fsyncPromise = new IntPromise(reactor.eventloop());
                    if (fsync) {
                        file.fsync(fsyncPromise);
                    } else {
                        file.fdatasync(fsyncPromise);
                    }
                    fsyncPromise.then((result, throwable2) -> {
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
        AsyncFile file = fileFuture.get();

        AsyncFile.Metrics metrics = file.metrics();
        assertEquals(0, metrics.nops());
        if (fsync) {
            assertEquals(1, metrics.fsyncs());
            assertEquals(0, metrics.fdatasyncs());
        } else {
            assertEquals(0, metrics.fsyncs());
            assertEquals(1, metrics.fdatasyncs());
        }
        assertEquals(0, metrics.reads());
        assertEquals(0, metrics.bytesRead());
        assertEquals(0, metrics.writes());
        assertEquals(0, metrics.bytesWritten());
    }

}
