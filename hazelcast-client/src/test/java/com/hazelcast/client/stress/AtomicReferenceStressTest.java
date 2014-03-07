package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TestThread;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;

/**
 * This tests verifies that AtomicReference updates via compair and set  are not lost.
 * we verify that the actual updates of the atomic ref, are the same as the expected update count.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AtomicReferenceStressTest extends StressTestSupport<AtomicReferenceStressTest.StressThread> {

    @Before
    public void setUp() {
        super.setUp(this);
    }

    //@Test
    public void testChangingCluster() {
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    public void assertResult() {

        long total=0;
        for ( StressThread s : stressThreads ) {
            total += s.count;
        }

        HazelcastInstance hz = cluster.getRandomNode();
        IAtomicReference<Long> expeted = hz.getAtomicReference("ref");

        assertEquals(expeted+" has failed writes ", total, (long) expeted.get());
    }

    public class StressThread extends TestThread {

        private HazelcastInstance instance;

        IAtomicReference<Long> ref;

        public long count=0;

        public StressThread(HazelcastInstance node){
            super(node);
            ref = instance.getAtomicReference("ref");
            ref.set(0l);
        }

        @Override
        public void doRun() throws Exception {
            long i = ref.get();
            if ( ref.compareAndSet(i, i + 1) ){
                count++;
            }
        }
    }

}
