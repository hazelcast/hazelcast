package com.hazelcast.tpc.engine;

import io.netty.channel.unix.Buffer;
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

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.tpc.engine.AsyncFile.pageSize;
import static com.hazelcast.tpc.util.Util.toPageAlignedAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AsyncFileTest {

    private Eventloop eventloop;

    public abstract Eventloop newEventloop();

    private List<String> testFiles = new LinkedList<>();

    @Before
    public void before() {
        eventloop = newEventloop();
        eventloop.start();
    }

    @After
    public void after() {
        if (eventloop != null) {
            eventloop.shutdown();
        }

        for(String testFile: testFiles){
            File file = new File(testFile);
            file.delete();
        }
    }

    @Test
    public void test() {
        TestRunnable task = new TestRunnable() {
            @Override
            public void runInternal() {
                AsyncFile file = eventloop.unsafe().newAsyncFile(randomTmpFile());

                assertFalse(new File(file.path()).exists());

                file.open(AsyncFile.O_WRONLY | AsyncFile.O_CREAT);

                assertTrue(new File(file.path()).exists());

                System.out.println(file.path());
            }
        };
        eventloop.execute(task);

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
                AsyncFile file = eventloop.unsafe().newAsyncFile(path);

                file.open(AsyncFile.O_WRONLY | AsyncFile.O_CREAT);

                file.fallocate(0, 0, 10 * AsyncFile.pageSize()).then(new BiConsumer() {
                    @Override
                    public void accept(Object o, Object o2) {
                        System.out.println("result:"+o);
                        System.out.println("throwable:"+o2);

                        file.close().then(new BiConsumer() {
                            @Override
                            public void accept(Object o, Object o2) {
                                completed.countDown();
                            }
                        });
                    }
                });
                assertTrue(new File(file.path()).exists());

                System.out.println(file.path());
            }
        };
        eventloop.execute(task);

        assertOpenEventually(completed);
    }

    @Test
    public void testWrite() {
        String path = randomTmpFile();
        System.out.println(path);

        ByteBuffer buffer = ByteBuffer.allocateDirect(2 * pageSize());
        long rawAddress = Buffer.memoryAddress(buffer);
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
                AsyncFile file = eventloop.unsafe().newAsyncFile(path);

                file.open(AsyncFile.O_WRONLY | AsyncFile.O_CREAT);

                file.pwrite(0, 20, bufferAddress).then((BiConsumer<Object, Throwable>) (result, throwable) -> {
                    throwableRef.set(throwable);
                    resultRef.set(result);
                    file.close().then((o, o2) -> completed.countDown());
                });
            }
        };
        eventloop.execute(task);

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
        long rawAddress = Buffer.memoryAddress(buffer);
        long bufferAddress = toPageAlignedAddress(rawAddress);

        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        AtomicReference resultRef = new AtomicReference();

        TestRunnable task = new TestRunnable() {
            @Override
            public void runInternal() {
                AsyncFile file = eventloop.unsafe().newAsyncFile(path);

                file.open(AsyncFile.O_RDONLY);


                file.pread(0, 4096, bufferAddress).then((BiConsumer<Object, Throwable>) (result, throwable) -> {
                    throwableRef.set(throwable);
                    resultRef.set(result);
                    completed.countDown();
                });
            }
        };
        eventloop.execute(task);

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
