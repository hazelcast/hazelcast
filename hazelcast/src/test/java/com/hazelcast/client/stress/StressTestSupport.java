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

package com.hazelcast.client.stress;

import com.hazelcast.config.Config;
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
    public static final int RUNNING_TIME_SECONDS = 180;
    //todo: should be system property
    public static final int CLUSTER_SIZE = 6;
    //todo: should be system property
    public static final int KILL_DELAY_SECONDS = 10;

    private final List<HazelcastInstance> instances = new CopyOnWriteArrayList<HazelcastInstance>();
    private CountDownLatch startLatch;
    private KillMemberThread killMemberThread;
    private volatile boolean stopOnError = true;
    private volatile boolean stopTest = false;
    private boolean clusterChangeEnabled = true;

    @Before
    public void setUp() {
        startLatch = new CountDownLatch(1);
        for (int k = 0; k < CLUSTER_SIZE; k++) {
            HazelcastInstance hz = newHazelcastInstance(createClusterConfig());
            instances.add(hz);
        }
    }

    public void setClusterChangeEnabled(boolean membershutdownEnabled) {
        this.clusterChangeEnabled = membershutdownEnabled;
    }

    public Config createClusterConfig() {
        return new Config();
    }

    @After
    public void tearDown() {
        stopTest = true;
        if (killMemberThread != null) {
            try {
                killMemberThread.join(TimeUnit.SECONDS.toMillis(KILL_DELAY_SECONDS * 4));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (HazelcastInstance hz : instances) {
            try {
                hz.getLifecycleService().terminate();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public final boolean startAndWaitForTestCompletion() {
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

        System.out.println("==================================================================");
        System.out.println("Test completed.");
        System.out.println("==================================================================");

        stopTest();
        return true;
    }

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

    public static final AtomicLong ID_GENERATOR = new AtomicLong(1);

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

                int index = random.nextInt(CLUSTER_SIZE);
                HazelcastInstance instance = instances.remove(index);
                instance.shutdown();

                HazelcastInstance newInstance = newHazelcastInstance(createClusterConfig());
                instances.add(newInstance);
            }
        }
    }
}
