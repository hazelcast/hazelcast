package com.hazelcast.client.quorum.semaphore;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.concurrent.semaphore.SemaphoreQuorumReadTest;
import com.hazelcast.config.Config;
import com.hazelcast.core.ISemaphore;
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
public class ClientSemaphoreQuorumReadTest extends SemaphoreQuorumReadTest {

    private static PartitionedClusterClients CLIENTS;

    @BeforeClass
    public static void setUp() {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        initTestEnvironment(new Config(), factory);
        CLIENTS = new PartitionedClusterClients(CLUSTER, factory);
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
        CLIENTS.terminateAll();
    }

    protected ISemaphore semaphore(int index) {
        return CLIENTS.client(index).getSemaphore(SEMAPHORE + quorumType.name());
    }

}
