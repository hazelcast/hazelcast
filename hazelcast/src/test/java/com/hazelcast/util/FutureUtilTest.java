/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class FutureUtilTest extends HazelcastTestSupport {

    @Test
    public void test_first_wait_second_finished() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new SimpleTask(waitLock)));
        }

        FutureUtil.waitWithDeadline(futures, 10, FutureUtil.logAllExceptions(Level.WARNING));
    }

    @Test
    public void test_first_finished_second_wait() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new SimpleTask(waitLock)));
        }

        FutureUtil.waitWithDeadline(futures, 10, FutureUtil.logAllExceptions(Level.WARNING));
    }

    @Test
    public void test_first_wait_second_finished_with_value() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new SimpleCallable(waitLock, i + 1)));
        }

        Collection<Integer> result = FutureUtil.returnWithDeadline(futures, 10, FutureUtil.logAllExceptions(Level.WARNING));
        assertEquals(2, result.size());

        Integer[] array = result.toArray(new Integer[result.size()]);
        assertEquals(1, (int) array[0]);
        assertEquals(2, (int) array[1]);
    }

    @Test
    public void test_first_finished_second_wait_with_value() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new SimpleCallable(waitLock, i + 1)));
        }

        Collection<Integer> result = FutureUtil.returnWithDeadline(futures, 10, FutureUtil.logAllExceptions(Level.WARNING));
        assertEquals(2, result.size());

        Integer[] array = result.toArray(new Integer[result.size()]);
        assertEquals(1, (int) array[0]);
        assertEquals(2, (int) array[1]);
    }

    @Test
    public void test_first_finished_second_interrupted_wait() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new BlockingTask(waitLock, interrupted)));
        }

        try {
            FutureUtil.waitWithDeadline(futures, 5, FutureUtil.logAllExceptions(Level.WARNING));
        } catch (TimeoutException e) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(true, interrupted.get());
                }
            });
            return;
        }

        fail();
    }

    @Test
    public void test_first_finished_second_interrupted_return() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new BlockingCallable(waitLock, interrupted)));
        }

        try {
            FutureUtil.returnWithDeadline(futures, 5, FutureUtil.logAllExceptions(Level.WARNING));
        } catch (TimeoutException e) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(true, interrupted.get());
                }
            });
            return;
        }

        fail();
    }

    @Test(expected = TimeoutException.class)
    public void test_timeout_exception() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new TimeoutingTask(waitLock)));
        }

        FutureUtil.returnWithDeadline(futures, 1, FutureUtil.logAllExceptions(Level.WARNING));
    }

    @Test
    public void test_timeout_timeunit_wait() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new SimpleTask(waitLock)));
        }

        FutureUtil.waitWithDeadline(futures, 5, TimeUnit.SECONDS, FutureUtil.logAllExceptions(Level.WARNING));
    }

    @Test
    public void test_timeout_timeunit_return() throws Exception {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new SimpleCallable(waitLock, i + 1)));
        }

        Collection<Integer> result = FutureUtil
                .returnWithDeadline(futures, 5, TimeUnit.SECONDS, FutureUtil.logAllExceptions(Level.WARNING));

        assertEquals(2, result.size());

        Integer[] array = result.toArray(new Integer[result.size()]);
        assertEquals(1, (int) array[0]);
        assertEquals(2, (int) array[1]);
    }

    @Test
    public void test_failing_second_wait() throws Throwable {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new FailingCallable(waitLock)));
        }

        ExceptionCollector exceptionHandler = new ExceptionCollector();
        FutureUtil.waitWithDeadline(futures, 5, TimeUnit.SECONDS, exceptionHandler);

        assertEquals(1, exceptionHandler.throwables.size());
        Throwable throwable = exceptionHandler.throwables.iterator().next();
        assertTrue(throwable instanceof ExecutionException);
        assertTrue(throwable.getCause() instanceof SpecialRuntimeException);
    }

    @Test
    public void test_failing_second_return() throws Throwable {
        AtomicBoolean waitLock = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        List<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < 2; i++) {
            futures.add(executorService.submit(new FailingCallable(waitLock)));
        }

        ExceptionCollector exceptionHandler = new ExceptionCollector();
        FutureUtil.returnWithDeadline(futures, 5, TimeUnit.SECONDS, exceptionHandler);

        assertEquals(1, exceptionHandler.throwables.size());
        Throwable throwable = exceptionHandler.throwables.iterator().next();
        assertTrue(throwable instanceof ExecutionException);
        assertTrue(throwable.getCause() instanceof SpecialRuntimeException);
    }

    private static final class ExceptionCollector implements FutureUtil.ExceptionHandler {

        private final List<Throwable> throwables = new ArrayList<Throwable>();

        @Override
        public void handleException(Throwable throwable) {
            throwables.add(throwable);
        }
    }

    private static final class TimeoutingTask
            implements Runnable {

        private final AtomicBoolean waitLock;

        private TimeoutingTask(AtomicBoolean waitLock) {
            this.waitLock = waitLock;
        }

        @Override
        public void run() {
            if (waitLock.compareAndSet(true, false)) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
        }
    }

    private static final class SimpleTask
            implements Runnable {

        private final AtomicBoolean waitLock;

        private SimpleTask(AtomicBoolean waitLock) {
            this.waitLock = waitLock;
        }

        @Override
        public void run() {
            if (waitLock.compareAndSet(true, false)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
        }
    }

    private static final class SimpleCallable
            implements Callable<Integer> {

        private final AtomicBoolean waitLock;
        private final int index;

        private SimpleCallable(AtomicBoolean waitLock, int index) {
            this.waitLock = waitLock;
            this.index = index;
        }

        @Override
        public Integer call()
                throws Exception {
            if (waitLock.compareAndSet(true, false)) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
            return index;
        }
    }

    private static final class BlockingTask
            implements Runnable {

        private final AtomicBoolean waitLock;
        private final AtomicBoolean interrupted;

        private BlockingTask(AtomicBoolean waitLock, AtomicBoolean interrupted) {
            this.waitLock = waitLock;
            this.interrupted = interrupted;
        }

        @Override
        public void run() {
            if (waitLock.compareAndSet(true, false)) {
                try {
                    for (; ; ) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException ignored) {
                    interrupted.set(true);
                }
            }
        }
    }

    private static final class BlockingCallable
            implements Callable<Integer> {

        private final AtomicBoolean waitLock;
        private final AtomicBoolean interrupted;

        private BlockingCallable(AtomicBoolean waitLock, AtomicBoolean interrupted) {
            this.waitLock = waitLock;
            this.interrupted = interrupted;
        }

        @Override
        public Integer call()
                throws Exception {
            if (waitLock.compareAndSet(true, false)) {
                try {
                    for (; ; ) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException ignored) {
                    interrupted.set(true);
                }
            }
            return 1;
        }
    }

    private static final class FailingCallable
            implements Callable<Integer> {

        private final AtomicBoolean waitLock;

        private FailingCallable(AtomicBoolean waitLock) {
            this.waitLock = waitLock;
        }

        @Override
        public Integer call()
                throws Exception {
            if (waitLock.compareAndSet(true, false)) {
                try {
                    Thread.sleep(1000);
                    return 1;
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
            throw new SpecialRuntimeException("foo");
        }
    }

    private static class SpecialRuntimeException
            extends RuntimeException {
        private SpecialRuntimeException(String message) {
            super(message);
        }
    }

}
