/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.executor;

import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import com.hazelcast.util.executor.LoggingScheduledExecutor.LoggingDelegatingFuture;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.SEVERE;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LoggingScheduledExecutorTest extends HazelcastTestSupport {

    private TestLogger logger = new TestLogger();
    private TestThreadFactory factory = new TestThreadFactory();

    private ScheduledExecutorService executor;

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

                String message = logger.getMessage();
                assertTrue("Found message: '" + message + "'", message.contains("FailedRunnable"));

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
        future.get(1, SECONDS);

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

    @Test
    public void testLoggingDelegatingFuture() {
        executor = new LoggingScheduledExecutor(logger, 1, factory);
        Runnable task = new FailedRunnable();

        ScheduledFuture<?> scheduledFuture1 = executor.schedule(task, 0, SECONDS);
        ScheduledFuture<?> scheduledFuture2 = executor.scheduleAtFixedRate(task, 0, 1, SECONDS);

        assertInstanceOf(LoggingDelegatingFuture.class, scheduledFuture1);
        assertInstanceOf(LoggingDelegatingFuture.class, scheduledFuture2);

        LoggingDelegatingFuture future1 = (LoggingDelegatingFuture) scheduledFuture1;
        LoggingDelegatingFuture future2 = (LoggingDelegatingFuture) scheduledFuture2;

        assertFalse(future1.isPeriodic());
        assertTrue(future2.isPeriodic());

        assertEquals(future1, future1);
        assertNotEquals(future1, future2);
        assertNotEquals(future1, null);

        assertEquals(future1.hashCode(), future1.hashCode());
        assertNotEquals(future1.hashCode(), future2.hashCode());
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
