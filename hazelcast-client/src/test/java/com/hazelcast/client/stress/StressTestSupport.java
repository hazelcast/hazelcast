package com.hazelcast.client.stress;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
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
    public static final int RUNNING_TIME_SECONDS = 60;
    //todo: should be system property
    public static final int CLUSTER_SIZE = 6;
    //todo: should be system property
    public static final int KILL_DELAY_SECONDS = 10;

    private final List<HazelcastInstance> instances = new CopyOnWriteArrayList();
    private CountDownLatch startLatch;
    private KillMemberThread killMemberThread;
    private volatile boolean stopOnError = true;
    private volatile boolean stopTest = false;

    @Before
    public void setUp() {
        startLatch = new CountDownLatch(1);
        for (int k = 0; k < CLUSTER_SIZE; k++) {
            HazelcastInstance hz = newHazelcastInstance(createClusterConfig());
            instances.add(hz);
        }

        killMemberThread = new KillMemberThread();
        killMemberThread.start();
    }

    public Config createClusterConfig(){
        return new Config();
    }

    @After
    public void tearDown() {
        for (HazelcastInstance hz : instances) {
            try {
                hz.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void startTest() {
        startLatch.countDown();
    }

    public boolean waitForTestCompletion(){
        for(int k=1;k<=RUNNING_TIME_SECONDS;k++){
              try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            float percent =(k*100.0f)/RUNNING_TIME_SECONDS;
            System.out.printf("%.1f Running for %s of %s seconds\n",percent,k,RUNNING_TIME_SECONDS);

            if(stopTest){
                System.err.println("Test has stopped premature!!!!");
                return false;
            }
        }

        return true;
    }

    protected void setStopOnError(boolean stopOnError) {
        this.stopOnError = stopOnError;
    }

    protected void stopTest() {
        stopTest = true;
    }

    protected boolean isStopped() {
        return stopTest;
    }

    public void assertNoErrors(TestThread... threads) {
        for (TestThread thread : threads) {
            thread.assertNoError();
        }
    }

    public void joinAll(TestThread... threads) {
        for (TestThread t : threads) {
            try {
                t.join(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while joining thread:" + t);
            }

            System.err.println("IsStopped:" + isStopped());
            if (t.isAlive()) {
                System.err.println("Could not join Thread:" + t.getName() + ", it is still alive");
                for (StackTraceElement e : t.getStackTrace()) {
                    System.err.println("\tat " + e);
                }
                throw new RuntimeException("Could not join thread:" + t + ", thread is still alive");
            }
        }
    }

    public final static AtomicLong ID_GENERATOR = new AtomicLong(1);

    public abstract class TestThread extends Thread {
        private volatile Throwable error;
        protected final Random random = new Random();

        public TestThread() {
            setName(getClass().getName() + "" + ID_GENERATOR.getAndIncrement());
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

        public void assertNoError() {
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

                int index = random.nextInt(CLUSTER_SIZE);
                HazelcastInstance instance = instances.remove(index);
                instance.shutdown();

                HazelcastInstance newInstance = newHazelcastInstance(createClusterConfig());
                instances.add(newInstance);
            }
        }
    }
}

