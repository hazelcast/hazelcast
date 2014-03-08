package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.stress.helpers.StressTestSupport;
import com.hazelcast.client.stress.helpers.TestThread;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AtomicLongStableReadStressTest extends StressTestSupport<AtomicLongStableReadStressTest.StressThread> {
    public static final int REFERENCE_COUNT = 10 * 1000;
    public static final String atomicName = "atomicreference:";

    @Before
    public void setUp() {
        cluster.initCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);

        setClientConfig(clientConfig);

        TOTAL_HZ_CLIENT_INSTANCES = 1;
        THREADS_PER_INSTANCE = 15;
        initStressThreadsWithClient(this);

        HazelcastInstance node = cluster.getRandomNode();
        for (int key = 0; key < REFERENCE_COUNT; key++) {
           IAtomicLong atomic = node.getAtomicLong(atomicName+key);
           atomic.set(key);
        }
    }

    @Test
    public void testChangingCluster() {
        runTest(true);
    }

    @Test
    public void testFixedCluster() {
        runTest(false);
    }

    public class StressThread extends TestThread {

        public StressThread(HazelcastInstance node){
            super(node);
        }

        @Override
        public void doRun() throws Exception {
            int key = random.nextInt(REFERENCE_COUNT);
            IAtomicLong reference = instance.getAtomicLong(atomicName+key);
            long value = reference.get();
            assertEquals(format("The value for atomic reference: %s was not consistent", reference), key, value);
        }
    }
}
