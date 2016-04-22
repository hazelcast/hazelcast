package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractInvocationFuture_AbstractTest extends HazelcastTestSupport {

    protected ILogger logger;
    protected Executor executor;
    protected TestFuture future;
    protected Object value = "somevalue";

    @Before
    public void setup() {
        logger = Logger.getLogger(getClass());
        executor = Executors.newSingleThreadExecutor();
        future = new TestFuture();
    }


    class TestFuture extends AbstractInvocationFuture {
        volatile boolean interruptDetected;

        public TestFuture() {
            super(AbstractInvocationFuture_AbstractTest.this.executor, AbstractInvocationFuture_AbstractTest.this.logger);
        }

        public TestFuture(Executor executor, ILogger logger) {
            super(executor, logger);
        }

        @Override
        protected void onInterruptDetected() {
            interruptDetected = true;
            complete(new InterruptedException());
        }

        @Override
        protected String invocationToString() {
            return "someinvocation";
        }

        @Override
        protected Object resolveAndThrow(Object state) throws ExecutionException, InterruptedException {
            if (state instanceof Throwable) {
                if (state instanceof Error) {
                    throw (Error) state;
                } else if (state instanceof RuntimeException) {
                    throw (RuntimeException) state;
                } else if (state instanceof InterruptedException) {
                    throw (InterruptedException) state;
                } else {
                    throw new ExecutionException((Throwable) state);
                }
            }
            return state;
        }

        @Override
        protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
            return new TimeoutException();
        }
    }
}
