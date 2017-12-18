package com.hazelcast.client.quorum.durableexecutor;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.quorum.durableexecutor.DurableExecutorQuorumWriteTest;
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
public class ClientDurableExecutorQuorumWriteTest extends DurableExecutorQuorumWriteTest {

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

    protected DurableExecutorService exec(int index, QuorumType quorumType) {
        return clients.client(index).getDurableExecutorService(EXEC_NAME + quorumType.name());
    }

}
