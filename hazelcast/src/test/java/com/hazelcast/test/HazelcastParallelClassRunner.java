package com.hazelcast.test;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastParallelClassRunner extends BlockJUnit4ClassRunner {

    static {
        final String logging = "hazelcast.logging.type";
        if (System.getProperty(logging) == null) {
            System.setProperty(logging, "log4j");
        }
        if (System.getProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK) == null) {
            System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "false");
        }
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");

        // randomize multicast group...
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);
    }

    private AtomicInteger numThreads;
    public static int maxThreads = 8;

    public HazelcastParallelClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
        numThreads = new AtomicInteger(0);

    }

    //protected List<FrameworkMethod> computeTestMethods() {
    //    List<FrameworkMethod> methods = super.computeTestMethods();
    //    Collections.shuffle(methods);
    //    return methods;
    //}

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
        new Thread (new Test(method, notifier)).start();
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

    class Test implements Runnable {
        private final FrameworkMethod method;
        private final RunNotifier notifier;

        public Test (final FrameworkMethod method, final RunNotifier notifier) {
            this.method = method;
            this.notifier = notifier;
        }

        @Override
        public void run () {
            long start = System.currentTimeMillis();
            String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
            System.out.println("Started Running Test: " + testName);

            // System.err.println (method.getName());
            HazelcastParallelClassRunner.super.runChild(method, notifier);
            numThreads.decrementAndGet();
            float took = (float) (System.currentTimeMillis() - start) / 1000;
            System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
        }
    }
}