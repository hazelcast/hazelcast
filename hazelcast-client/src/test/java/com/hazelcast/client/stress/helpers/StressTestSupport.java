package com.hazelcast.client.stress.helpers;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.modularhelpers.ClusterSupport;
import org.junit.After;
import org.junit.Before;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static org.junit.Assert.assertNull;

public abstract class StressTestSupport<T extends TestThread> extends HazelcastTestSupport {

    //todo: should be system property
    public static int RUNNING_TIME_SECONDS = 180;
    //todo: should be system property
    public static int CLUSTER_SIZE = 3;
    //todo: should be system property
    public static int KILL_DELAY_SECONDS = RUNNING_TIME_SECONDS / 4;

    public final int TOTAL_HZ_CLIENT_INSTANCES = 3;
    public final int THREADS_PER_INSTANCE = 5;

    protected ClusterSupport cluster = new ClusterSupport(CLUSTER_SIZE);

    private CountDownLatch startLatch = new CountDownLatch(1);

    private Thread killThread = null;

    private volatile AtomicInteger stopTest = new AtomicInteger(1);

    private boolean clusterChangeEnabled = true;

    protected ClientConfig clientConfig = new ClientConfig();

    protected List<T> stressThreads = new ArrayList<T>();


    public void setUp() {
        cluster.initCluster();
    }

    public void setClientConfig(ClientConfig clientConfig){
        this.clientConfig = clientConfig;
    }

    public void setUp(Object yourThis) {
        cluster.initCluster();

        for ( int i = 0; i < TOTAL_HZ_CLIENT_INSTANCES; i++ ) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);

            for ( int j = 0; j < THREADS_PER_INSTANCE; j++ ) {
                T t = getInstanceOfT(yourThis, instance);
                t.setStartLatch(startLatch);
                t.setStoped(stopTest);
                t.start();
                stressThreads.add(t);
            }
        }
    }

    private T getInstanceOfT(Object yours, HazelcastInstance instance){

        ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
        Class<T> type = (Class<T>) superClass.getActualTypeArguments()[0];

        try{
            Constructor<T> con = type.getConstructor(yours.getClass(), HazelcastInstance.class);
            return con.newInstance(yours, instance);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() {
        cluster.shutDown();
    }

    public void setKillThread(Thread t){
        killThread = t;
    }

    public void setClusterConfig(Config config) {
        cluster.setConfig(config);
    }

    /**
     * run all test threads for set amount of time
     * and wait for them to finish with a join,
     * then calls assertResult(), which you can override to do your post test asserting
     */
    public void runTest(boolean clusterChangeEnabled) {
        setClusterChangeEnabled(clusterChangeEnabled);
        runTestReportLoop();
        joinAll();
        assertNoErrors();
        assertResult();
    }

    private void setClusterChangeEnabled(boolean member_shutdown_Enabled) {
        clusterChangeEnabled = member_shutdown_Enabled;

        if( clusterChangeEnabled == true && killThread == null){
            killThread = new KillMemberThread();
        }
    }

    /**
     * Called after the test has run and we have joined all thread
     * Do you post test asserting hear
     */
    public void assertResult(){}


    private void runTestReportLoop() {
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

            for (TestThread t : stressThreads) {
                if(t.error!=null){
                    System.err.println("==================================================================");
                    System.err.println("Test ended premature!");
                    System.err.println("==================================================================");
                    stopTest.decrementAndGet();
                    return;
                }
            }
        }
        System.out.println("==================================================================");
        System.out.println("Test completed.");
        System.out.println("==================================================================");
        stopTest.decrementAndGet();
        return;
    }



    private final void joinAll() {
        for (TestThread t : stressThreads) {
            try {
                t.join(1000 * 5);
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
    }

    private final void assertNoErrors() {
        for (T thread : stressThreads) {
            thread.assertNoError();
        }
    }

    public class KillMemberThread extends Thread {
        @Override
        public void run(){

            while ( stopTest.get() !=0 ) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(KILL_DELAY_SECONDS));
                } catch (InterruptedException e) {
                }

                cluster.shutDownRandomNode();
                cluster.addNode();
            }
        }
    }

    public class KillMemberOwningKeyThread extends Thread {
        private Object key = null;

        public KillMemberOwningKeyThread(Object key){
            this.key = key;
        }

        @Override
        public void run() {
            while ( stopTest.get() != 0 ) {
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

