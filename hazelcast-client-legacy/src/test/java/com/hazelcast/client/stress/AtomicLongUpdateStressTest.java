package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * This test fails sporadically. It seems to indicate a problem within the core because there is not much logic
 * in the atomicwrapper that can fail (just increment).
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class AtomicLongUpdateStressTest extends StressTestSupport {

    public static final int CLIENT_THREAD_COUNT = 5;
    public static final int REFERENCE_COUNT = 10 * 1000;

    private HazelcastInstance client;
    private IAtomicLong[] references;
    private StressThread[] stressThreads;

    @Before
    public void setUp() {
        super.setUp();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);
        references = new IAtomicLong[REFERENCE_COUNT];
        for (int k = 0; k < references.length; k++) {
            references[k] = client.getAtomicLong("atomicreference:" + k);
        }

        stressThreads = new StressThread[CLIENT_THREAD_COUNT];
        for (int k = 0; k < stressThreads.length; k++) {
            stressThreads[k] = new StressThread();
            stressThreads[k].start();
        }
    }

    @After
    public void tearDown() {
        super.tearDown();

        if (client != null) {
            client.shutdown();
        }
    }

    //@Test
    public void testChangingCluster() {
        test(true);
    }

    @Test
    public void testFixedCluster() {
        test(false);
    }

    public void test(boolean clusterChangeEnabled) {
        setClusterChangeEnabled(clusterChangeEnabled);

        startAndWaitForTestCompletion();
        joinAll(stressThreads);
        assertNoUpdateFailures();
    }

    private void assertNoUpdateFailures() {
        int[] increments = new int[REFERENCE_COUNT];
        for (StressThread t : stressThreads) {
            t.addIncrements(increments);
        }

        Set<Integer> failedKeys = new HashSet<Integer>();
        for (int k = 0; k < REFERENCE_COUNT; k++) {
            long expectedValue = increments[k];
            long foundValue = references[k].get();
            if (expectedValue != foundValue) {
                failedKeys.add(k);
            }
        }

        if (failedKeys.isEmpty()) {
            return;
        }

        int index = 1;
        for (Integer key : failedKeys) {
            System.err.println("Failed write: " + index + " found:" + references[key].get() + " expected:" + increments[key]);
            index++;
        }

        fail("There are failed writes, number of failures:" + failedKeys.size());
    }

    public class StressThread extends TestThread {
        private final int[] increments = new int[REFERENCE_COUNT];

        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                int index = random.nextInt(REFERENCE_COUNT);
                int increment = random.nextInt(100);
                increments[index] += increment;
                IAtomicLong reference = references[index];
                reference.addAndGet(increment);
            }
        }

        public void addIncrements(int[] increments) {
            for (int k = 0; k < increments.length; k++) {
                increments[k] += this.increments[k];
            }
        }
    }
}
