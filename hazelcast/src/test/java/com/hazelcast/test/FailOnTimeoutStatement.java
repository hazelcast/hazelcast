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
        FutureTask<Throwable> task = new FutureTask<Throwable>(callable);
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        thread.start();
        callable.awaitStarted();
        Throwable throwable = getResult(task, thread);
        if (throwable != null) {
            throw throwable;
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
            return e; // caller will re-throw; no need to call Thread.interrupt()
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

        public void awaitStarted() throws InterruptedException {
            startLatch.await();
        }
    }
}
