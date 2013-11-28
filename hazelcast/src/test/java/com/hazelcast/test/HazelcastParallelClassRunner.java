package com.hazelcast.test;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastParallelClassRunner extends AbstractHazelcastClassRunner {

    private AtomicInteger numThreads;
    public static int maxThreads = 8;

    public HazelcastParallelClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
        numThreads = new AtomicInteger(0);
    }

    @Override
    protected void runChild(final FrameworkMethod method, final RunNotifier notifier) {
        while (numThreads.get() > maxThreads) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                System.err.println ("Interrupted: " + method.getName());
                e.printStackTrace();
                return; // The user may have interrupted us; this won't happen normally
            }
        }
        numThreads.incrementAndGet();
        new Thread (new TestRunner(method, notifier)).start();
    }

    @Override
    protected Statement childrenInvoker(final RunNotifier notifier) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                HazelcastParallelClassRunner.super.childrenInvoker(notifier).evaluate();
                // wait for all child threads (tests) to complete
                while (numThreads.get() > 0) {
                    Thread.sleep(25);
                }
            }
        };
    }

    private class TestRunner implements Runnable {
        private final FrameworkMethod method;
        private final RunNotifier notifier;

        public TestRunner(final FrameworkMethod method, final RunNotifier notifier) {
            this.method = method;
            this.notifier = notifier;
        }

        @Override
        public void run () {
            long start = System.currentTimeMillis();
            String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
            System.out.println("Started Running Test: " + testName);
            HazelcastParallelClassRunner.super.runChild(method, notifier);
            numThreads.decrementAndGet();
            float took = (float) (System.currentTimeMillis() - start) / 1000;
            System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
        }
    }

}
