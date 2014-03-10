package com.hazelcast.client.stress;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.support.StressTestSupport;
import com.hazelcast.client.stress.support.TestThread;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
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

    @Before
    public void setUp() {
        cluster.initCluster();

        TOTAL_HZ_INSTANCES = 1;
        THREADS_PER_INSTANCE = 15;


        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);

        setClientConfig(clientConfig);
        initStressThreadsWithClient(this);
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
        HazelcastInstance hz = cluster.getRandomNode();
        IAtomicLong atomicLong = hz.getAtomicLong(atomicKey);

        assertEquals(atomicLong + " has value " + atomicLong.get(), expetedTotal, atomicLong.get());
    }

    public class StressThread extends TestThread {
        private IAtomicLong atomicLong;
        public AtomicInteger count = new AtomicInteger(0);

        public StressThread(HazelcastInstance node){
            super(node);
            //we are using only 1 instance of atomicLong to create high contention between threads
            atomicLong = instance.getAtomicLong(atomicKey);
        }

        public void testLoop() throws Exception {
            int inc = 1;
            atomicLong.getAndAdd(inc);
            count.addAndGet(inc);
        }
    }
}
