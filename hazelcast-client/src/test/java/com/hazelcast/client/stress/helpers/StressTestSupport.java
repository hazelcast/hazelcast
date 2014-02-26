package com.hazelcast.client.stress.helpers;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.modularhelpers.SimpleClusterUtil;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static org.junit.Assert.assertNull;

public abstract class StressTestSupport extends HazelcastTestSupport {

    //todo: should be system property
    public static int RUNNING_TIME_SECONDS = 20;
    //todo: should be system property
    public static int CLUSTER_SIZE = 6;
    //todo: should be system property
    public static final int KILL_DELAY_SECONDS = 10;

    protected SimpleClusterUtil cluster = new SimpleClusterUtil(CLUSTER_SIZE);

    private CountDownLatch startLatch;
    private KillMemberThread killMemberThread;
    private volatile boolean stopOnError = true;
    private volatile boolean stopTest = false;
    private boolean clusterChangeEnabled = true;

    @Before
    public void setUp() {
        startLatch = new CountDownLatch(1);
        cluster.initCluster();
    }

    public void setClusterChangeEnabled(boolean membershutdownEnabled) {
        this.clusterChangeEnabled = membershutdownEnabled;
    }

    public void setClusterConfig(Config config) {
        cluster.setConfig(config);
    }

    @After
    public void tearDown() {
        cluster.shutDown();
    }

    private final boolean startAndWaitForTestCompletion() {
        System.out.println("Cluster change enabled:" + clusterChangeEnabled);
        if (clusterChangeEnabled) {
            killMemberThread = new KillMemberThread();
            killMemberThread.start();
        }

        System.out.println("==================================================================");
        System.out.println("Test started.");
        System.out.println("==================================================================");

        startLatch.countDown();

        for (int k = 1; k <= RUNNING_TIME_SECONDS; k++) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            float percent = (k * 100.0f) / RUNNING_TIME_SECONDS;
            System.out.printf("%.1f Running for %s of %s seconds\n", percent, k, RUNNING_TIME_SECONDS);

            if (stopTest) {
                System.err.println("==================================================================");
                System.err.println("Test ended premature!");
                System.err.println("==================================================================");
                return false;
            }
        }

        stopTest();

        System.out.println("==================================================================");
        System.out.println("Test completed.");
        System.out.println("==================================================================");

        return true;
    }


    /*run all test threads for set amount of time
    * and wait for them to finish with a join,
    * then calls assertResult(), to do you post test asserting
    * */
    public void runTest(boolean clusterChangeEnabled, TestThread[] threads) {
        setClusterChangeEnabled(clusterChangeEnabled);
        startAndWaitForTestCompletion();
        joinAll(threads);
        assertResult();
    }

    /* Called after the test has run and we have joined all thread
    *  Do you post test asserting hear
    */
    abstract public void assertResult();


    protected final void setStopOnError(boolean stopOnError) {
        this.stopOnError = stopOnError;
    }

    protected final void stopTest() {
        stopTest = true;
    }

    protected final boolean isStopped() {
        return stopTest;
    }

    public final void assertNoErrors(TestThread... threads) {
        for (TestThread thread : threads) {
            thread.assertNoError();
        }
    }

    public final void joinAll(TestThread... threads) {
        for (TestThread t : threads) {
            try {
                t.join(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while joining thread:" + t);
            }

            if (t.isAlive()) {
                System.err.println("Could not join Thread:" + t.getName() + ", it is still alive");
                for (StackTraceElement e : t.getStackTrace()) {
                    System.err.println("\tat " + e);
                }
                throw new RuntimeException("Could not join thread:" + t + ", thread is still alive");
            }
        }

        assertNoErrors(threads);
    }

    public final static AtomicLong ID_GENERATOR = new AtomicLong(1);

    public abstract class TestThread extends Thread {
        private volatile Throwable error;
        protected final Random random = new Random();

        public TestThread() {
            setName(getClass().getName() + "(" + ID_GENERATOR.getAndIncrement() +")");
        }

        @Override
        public final void run() {
            try {
                startLatch.await();
                doRun();
            } catch (Throwable t) {
                if (stopOnError) {
                    stopTest();
                }
                t.printStackTrace();
                this.error = t;
            }
        }

        public final void assertNoError() {
            assertNull(getName() + " encountered an error", error);
        }

        public abstract void doRun() throws Exception;
    }


    public class KillMemberThread extends TestThread {

        @Override
        public void doRun() throws Exception {
            while (!stopTest) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(KILL_DELAY_SECONDS));
                } catch (InterruptedException e) {
                }

                cluster.shutDownRandomNode();

                cluster.addNode();
            }
        }
    }
}

