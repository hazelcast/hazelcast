package com.hazelcast.client.quorum.countdownlatch;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.quorum.countdownlatch.CountDownLatchQuorumWriteTest;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class ClientCountDownLatchQuorumWriteTest extends CountDownLatchQuorumWriteTest {

    private static PartitionedClusterClients clients;

    @BeforeClass
    public static void setUp() {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        initTestEnvironment(new Config(), factory);
        clients = new PartitionedClusterClients(cluster, factory);
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
        clients.terminateAll();
    }

    @Override
    protected ICountDownLatch latch(int index) {
        return clients.client(index).getCountDownLatch(LATCH_NAME + quorumType.name());
    }

}
