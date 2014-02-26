package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static org.junit.Assert.fail;

/**
 * This tests verifies that we can Obtain a lock in a multi threaded test
 * my simply having a number to threads compeating for a lock.
 * TODO we are not checking a result, just relying on exceptions.  however there are some log mesages, that could serve
 * as an error condition.  e.g , thread is not owner of lock,
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ObtainLockStressTest extends StressTestSupport {

    public static int TOTAL_HZ_INSTANCES = 5;
    public static int THREADS_PER_INSTANCE = 2;

    private StressThread[] stressThreads = new StressThread[TOTAL_HZ_INSTANCES * THREADS_PER_INSTANCE];

    @Before
    public void setUp() {
        super.RUNNING_TIME_SECONDS = 20;
        super.setUp();

        int index=0;
        for (int i = 0; i < TOTAL_HZ_INSTANCES; i++) {

            HazelcastInstance instance = HazelcastClient.newHazelcastClient(new ClientConfig());

            for (int j = 0; j < THREADS_PER_INSTANCE; j++) {

                StressThread t = new StressThread(instance);
                t.start();
                stressThreads[index++] = t;
            }
        }
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testChangingCluster() {
        runTest(true, stressThreads);
    }

    @Test
    public void testFixedCluster() {
        runTest(false, stressThreads);
    }

    //no asserts at end
    public void assertResult(){}


    public class StressThread extends TestThread {

        private HazelcastInstance instance;
        private ILock lock;

        public StressThread( HazelcastInstance node ){
            super();

            this.instance = node;
            lock = instance.getLock("L");
        }

        @Override
        public void doRun() throws Exception {

            while ( !isStopped() ) {

                try{
                    if(lock.tryLock()){
                        lock.unlock();
                    }
                }catch(TargetDisconnectedException e){

                    System.out.println("===>>> "+this + " " + getName() + " "+ e);
                    break;
                }
            }
        }
    }

}
