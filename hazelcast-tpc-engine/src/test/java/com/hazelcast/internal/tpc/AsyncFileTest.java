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

package com.hazelcast.internal.tpc;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpc.AsyncFile.O_WRONLY;
import static com.hazelcast.internal.tpc.AsyncFile.PERMISSIONS_ALL;
import static com.hazelcast.internal.tpc.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.BufferUtil.toPageAlignedAddress;
import static com.hazelcast.internal.tpc.util.OS.pageSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    public void test() {
        TestRunnable task = new TestRunnable() {
            @Override
            public void runInternal() {
                AsyncFile file = reactor.eventloop.newAsyncFile(randomTmpFile());

                assertFalse(new File(file.path()).exists());

                file.open(O_WRONLY | O_CREAT, PERMISSIONS_ALL);

                assertTrue(new File(file.path()).exists());

                System.out.println(file.path());
            }
        };
        reactor.offer(task);

        assertCompletesEventually(task);
        assertNoProblems(task);
    }

    @Test
    public void testFallocate() {
        String path = randomTmpFile();
        System.out.println(path);

        CountDownLatch completed = new CountDownLatch(1);
        TestRunnable task = new TestRunnable() {
            @Override
            public void runInternal() {
                AsyncFile file = reactor.eventloop().newAsyncFile(path);

                file.open(O_WRONLY | O_CREAT, PERMISSIONS_ALL);

                file.fallocate(0, 0, 10 * pageSize()).then((BiConsumer) (o, o2) -> {
                    System.out.println("result:" + o);
                    System.out.println("throwable:" + o2);

                    file.close().then((BiConsumer) (o1, o21) -> completed.countDown());
                });
                assertTrue(new File(file.path()).exists());

                System.out.println(file.path());
            }
        };
        reactor.offer(task);

        assertOpenEventually(completed);
    }

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

        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        AtomicReference resultRef = new AtomicReference();

        TestRunnable task = new TestRunnable() {
            @Override
            public void runInternal() {
                AsyncFile file = reactor.eventloop().newAsyncFile(path);

                file.open(O_WRONLY | O_CREAT, PERMISSIONS_ALL);

                file.pwrite(0, 20, bufferAddress).then((BiConsumer<Integer, Throwable>) (result, throwable) -> {
                    throwableRef.set(throwable);
                    resultRef.set(result);
                    file.close().then((o, o2) -> completed.countDown());
                });
            }
        };
        reactor.offer(task);

        assertCompletesEventually(task);
        assertNoProblems(task);
        assertOpenEventually(completed);
        assertNull(throwableRef.get());
        assertEquals(Boolean.TRUE, resultRef.get());
    }

    @Test
    public void testRead() {
        String path = randomTmpFile();
        write(path, "1234");


        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * pageSize());
        long rawAddress = addressOf(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        AtomicReference resultRef = new AtomicReference();

        TestRunnable task = new TestRunnable() {
            @Override
            public void runInternal() {
                AsyncFile file = reactor.eventloop().newAsyncFile(path);

                file.open(AsyncFile.O_RDONLY, PERMISSIONS_ALL);

                file.pread(0, 4096, bufferAddress).then((result, throwable) -> {
                    throwableRef.set(throwable);
                    resultRef.set(result);
                    completed.countDown();
                });
            }
        };
        reactor.offer(task);

        assertCompletesEventually(task);
        assertNoProblems(task);
        assertOpenEventually(completed);
        assertNull(throwableRef.get());
        assertEquals(Boolean.TRUE, resultRef.get());
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

    private void delete(String path) {
        File file = new File(path);
        file.delete();
    }

    private void assertNoProblems(TestRunnable runnable) {
        assertNull(runnable.throwableRef.get());
    }

    private void assertCompletesEventually(TestRunnable runnable) {
        assertOpenEventually(runnable.completed);
    }

    private abstract class TestRunnable implements Runnable {
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicReference<Throwable> throwableRef = new AtomicReference<>();

        public abstract void runInternal() throws Exception;

        @Override
        public final void run() {
            try {
                runInternal();
            } catch (Throwable e) {
                throwableRef.set(e);
            } finally {
                completed.countDown();
            }
        }
    }
}
