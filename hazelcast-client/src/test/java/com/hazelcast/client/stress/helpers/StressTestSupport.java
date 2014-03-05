package com.hazelcast.client.stress.helpers;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.modularhelpers.ClusterSupport;
import org.junit.After;
import org.junit.Before;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static org.junit.Assert.assertNull;

public abstract class StressTestSupport extends HazelcastTestSupport {

    public final static AtomicLong ID_GENERATOR = new AtomicLong(1);
    //todo: should be system property
    public static int RUNNING_TIME_SECONDS = 10;
    //todo: should be system property
    public static int CLUSTER_SIZE = 3;
    //todo: should be system property
    public static int KILL_DELAY_SECONDS = RUNNING_TIME_SECONDS / 4;

    protected ClusterSupport cluster = new ClusterSupport(CLUSTER_SIZE);

    private CountDownLatch startLatch;
    private Thread killThread = null;
    private volatile boolean stopOnError = true;
    private volatile boolean stopTest = false;
    private boolean clusterChangeEnabled = true;

    @Before
    public void setUp() {
        startLatch = new CountDownLatch(1);
        cluster.initCluster();
    }

    private void setClusterChangeEnabled(boolean member_shutdown_Enabled) {
        clusterChangeEnabled = member_shutdown_Enabled;

        if( clusterChangeEnabled == true && killThread == null){
            killThread = new KillMemberThread();
        }
    }

    public void setClusterConfig(Config config) {
        cluster.setConfig(config);
    }

    @After
    public void tearDown() {
        cluster.shutDown();
    }

    public void setKillThread(Thread t){
        killThread = t;
    }

    private final boolean runTestReportLoop() {
        System.out.println("Cluster change enabled:" + clusterChangeEnabled);
        if (clusterChangeEnabled) {
            killThread.start();
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

    /**
    * run all test threads for set amount of time
    * and wait for them to finish with a join,
    * then calls assertResult(), which you can override to do your post test asserting
    */
    public void runTest(boolean clusterChangeEnabled, TestThread[] threads) {
        setClusterChangeEnabled(clusterChangeEnabled);
        runTestReportLoop();
        joinAll(threads);
        assertResult();
    }

    /**
    * Called after the test has run and we have joined all thread
    * Do you post test asserting hear
    */
    public void assertResult(){}

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

    public abstract class TestThread extends Thread {
        private volatile Throwable error;
        protected final Random random = new Random();

        public TestThread() {
            setName(getClass().getName() + ID_GENERATOR.getAndIncrement());
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

    public class KillMemberOwningKeyThread extends TestThread {

        private Object key = null;

        public KillMemberOwningKeyThread(Object key){
            this.key = key;
        }

        @Override
        public void doRun() throws Exception {
            while (!stopTest) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(KILL_DELAY_SECONDS));
                } catch (InterruptedException e) {
                }

                cluster.shutDownNodeOwning(key);
                cluster.addNode();
            }
        }
    }
}

