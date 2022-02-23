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

package com.hazelcast.internal.util.executor;

import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoggingScheduledExecutorTest extends HazelcastTestSupport {

    private TestLogger logger = new TestLogger();
    private TestThreadFactory factory = new TestThreadFactory();

    private LoggingScheduledExecutor executor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(5, SECONDS);
        }
    }

    @Test
    public void test_setRemoveOnCancelPolicy_isCalledOnJava7() throws Exception {
        assumeThatNoJDK6();

        executor = new LoggingScheduledExecutor(logger, 1, factory);

        Method method = ScheduledThreadPoolExecutor.class.getMethod("getRemoveOnCancelPolicy");
        // will only return true when the setRemoveOnCancelPolicy was called with true.
        assertEquals(Boolean.TRUE, method.invoke(executor));
    }

    @Test
    @Category(SlowTest.class)
    public void no_remaining_task_after_cancel() throws Exception {
        executor = new LoggingScheduledExecutor(logger, 1, factory);

        for (int i = 0; i < 1000; i++) {
            Future<Integer> future = executor.schedule(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    HOURS.sleep(1);
                    return null;
                }
            }, 10, SECONDS);

            future.cancel(true);
        }

        final BlockingQueue<Runnable> workQueue = ((LoggingScheduledExecutor) executor).getQueue();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(0, workQueue.size());
            }
        });
    }

    @Test
    public void no_remaining_task_after_cancel_long_delayed_tasks() throws Exception {
        executor = new LoggingScheduledExecutor(logger, 1, factory);

        for (int i = 0; i < 1000; i++) {
            Future<Integer> future = executor.schedule(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    HOURS.sleep(1);
                    return null;
                }
            }, 10, HOURS);

            future.cancel(true);
        }

        final BlockingQueue<Runnable> workQueue = ((LoggingScheduledExecutor) executor).getQueue();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(0, workQueue.size());
            }
        });
    }

    @Test
    public void testConstructor_withRejectedExecutionHandler() {
        RejectedExecutionHandler handler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable runnable, ThreadPoolExecutor executor) {
            }
        };

        executor = new LoggingScheduledExecutor(logger, 1, factory, handler);
    }

    @Test
    public void logsExecutionException_withRunnable() {
        executor = new LoggingScheduledExecutor(logger, 1, factory);
        executor.submit(new FailedRunnable());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(RuntimeException.class, logger.getThrowable());

                Level level = logger.getLevel();
                assertEquals(SEVERE, level);
            }
        });
    }

    @Test
    public void throwsExecutionException_withCallable() throws Exception {
        executor = new LoggingScheduledExecutor(logger, 1, factory);
        Future<Integer> future = executor.submit(new FailedCallable());

        expectedException.expect(new RootCauseMatcher(RuntimeException.class));
        future.get();

        assertNull(logger.getThrowable());
    }

    @Test
    public void throwsExecutionException_withCallable_withFutureGetTimeout() throws Exception {
        executor = new LoggingScheduledExecutor(logger, 1, factory);
        Future<Integer> future = executor.submit(new FailedCallable());

        expectedException.expect(new RootCauseMatcher(RuntimeException.class));
        future.get(10, SECONDS);

        assertNull(logger.getThrowable());
    }

    @Test
    public void testFuture_withCancellation() throws Exception {
        final CountDownLatch blocker = new CountDownLatch(1);

        executor = new LoggingScheduledExecutor(logger, 1, factory);
        Future<Integer> future = executor.submit(new BlockingCallable(blocker));

        assertFalse(future.isCancelled());
        assertTrue(future.cancel(true));

        expectedException.expect(CancellationException.class);
        future.get();
    }

    private static class FailedRunnable implements Runnable {

        @Override
        public void run() {
            throw new RuntimeException();
        }

        @Override
        public String toString() {
            return "FailedRunnable{}";
        }
    }

    private static class FailedCallable implements Callable<Integer> {

        @Override
        public Integer call() throws Exception {
            throw new RuntimeException();
        }

        @Override
        public String toString() {
            return "FailedCallable{}";
        }
    }

    private static class BlockingCallable implements Callable<Integer> {

        private final CountDownLatch blocker;

        BlockingCallable(CountDownLatch blocker) {
            this.blocker = blocker;
        }

        @Override
        public Integer call() throws Exception {
            blocker.await();
            return 42;
        }
    }

    private static class TestThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    }

    private static class TestLogger extends AbstractLogger {

        private final AtomicReference<Throwable> throwableHolder = new AtomicReference<Throwable>();
        private final AtomicReference<String> messageHolder = new AtomicReference<String>();
        private final AtomicReference<Level> logLevelHolder = new AtomicReference<Level>();

        @Override
        public void log(Level level, String message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            throwableHolder.set(thrown);
            messageHolder.set(message);
            logLevelHolder.set(level);
        }

        @Override
        public void log(LogEvent logEvent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Level getLevel() {
            return logLevelHolder.get();
        }

        @Override
        public boolean isLoggable(Level level) {
            return false;
        }

        public Throwable getThrowable() {
            return throwableHolder.get();
        }

        public String getMessage() {
            return messageHolder.get();
        }
    }
}
