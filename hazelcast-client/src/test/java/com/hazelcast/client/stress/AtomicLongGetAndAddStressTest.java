package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TestThread;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that atomicLong updates via getandInc are not lost.
 * a number of threads are all going for the same AtomicLong to do a get and add.
 * we verify that the actual updates to the atomic long, are the same as the total number of updates from all threads.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AtomicLongGetAndAddStressTest extends StressTestSupport<AtomicLongGetAndAddStressTest.StressThread> {

    private String atomicKey = "atomicL";
    private IAtomicLong acutal;

    @Before
    public void setUp() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);

        super.setClientConfig(clientConfig);
        super.setUp(this);

        acutal = cluster.getRandomNode().getAtomicLong(atomicKey);
    }

    @Test
    public void testChangingCluster() {
        setKillThread( new KillMemberOwningKeyThread(atomicKey) );
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    @Override
    public void assertResult() {

        long expetedTotal=0;
        for ( StressThread s : stressThreads ) {
            expetedTotal += s.count.get();
        }
        assertEquals(acutal + " has value " + acutal.get(), expetedTotal, acutal.get());
    }

    public class StressThread extends TestThread {
        private IAtomicLong atomicLong;
        public AtomicInteger count = new AtomicInteger(0);

        public StressThread(HazelcastInstance node){
            super(node);
            //we are using only 1 instance of atomicLong to create high contention between threads
            atomicLong = instance.getAtomicLong(atomicKey);
        }

        public void doRun() throws Exception {
            int inc = 1;
            atomicLong.getAndAdd(inc);
            count.addAndGet(inc);
        }
    }

}
