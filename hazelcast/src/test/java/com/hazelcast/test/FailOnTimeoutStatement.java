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

package com.hazelcast.test;

import org.junit.runners.model.Statement;
import org.junit.runners.model.TestTimedOutException;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FailOnTimeoutStatement extends Statement {

    private final Statement originalStatement;
    private final TimeUnit timeUnit;
    private String name;
    private final long timeout;

    /**
     * Creates an instance wrapping the given statement with the given timeout in milliseconds.
     *
     * @param name          name of the thread to be used for evaluating the statement
     * @param statement     the statement to wrap
     * @param timeoutMillis the timeout in milliseconds
     */
    public FailOnTimeoutStatement(String name, Statement statement, long timeoutMillis) {
        this.name = name;
        this.timeout = timeoutMillis;
        this.timeUnit = TimeUnit.MILLISECONDS;
        this.originalStatement = statement;
    }

    @Override
    public void evaluate() throws Throwable {
        CallableStatement callable = new CallableStatement();
        FutureTask<Throwable> task = new FutureTask<>(callable);
        Thread thread = newThread(task, name);
        thread.setDaemon(true);
        thread.start();
        callable.awaitStarted();
        Throwable throwable = getResult(task, thread);
        if (throwable != null) {
            throw throwable;
        }
    }

    Thread newThread(FutureTask<Throwable> task, String name) {
        if (Thread.currentThread() instanceof MultithreadedTestRunnerThread) {
            return new MultithreadedTestRunnerThread(task, name);
        } else {
            return new Thread(task, name);
        }
    }

    /**
     * Wait for the test task, returning the exception thrown by the test if the
     * test failed, an exception indicating a timeout if the test timed out, or
     * {@code null} if the test passed.
     */
    private Throwable getResult(FutureTask<Throwable> task, Thread thread) {
        try {
            if (timeout > 0) {
                return task.get(timeout, timeUnit);
            } else {
                return task.get();
            }
        } catch (InterruptedException e) {
            // propagate interrupt
            thread.interrupt();
            return e;
        } catch (ExecutionException e) {
            // test failed; have caller re-throw the exception thrown by the test
            return e.getCause();
        } catch (TimeoutException e) {
            return createTimeoutException(thread);
        }
    }

    private Exception createTimeoutException(Thread thread) {
        StackTraceElement[] stackTrace = thread.getStackTrace();
        Exception currThreadException = new TestTimedOutException(timeout, timeUnit);
        if (stackTrace != null) {
            currThreadException.setStackTrace(stackTrace);
            thread.interrupt();
        }

        return currThreadException;
    }

    private class CallableStatement implements Callable<Throwable> {

        private final CountDownLatch startLatch = new CountDownLatch(1);

        @Override
        public Throwable call() throws Exception {
            try {
                startLatch.countDown();
                originalStatement.evaluate();
            } catch (Exception e) {
                throw e;
            } catch (Throwable e) {
                return e;
            }
            return null;
        }

        void awaitStarted() throws InterruptedException {
            startLatch.await();
        }
    }
}
