package com.hazelcast.client.quorum.pncounter;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.quorum.pncounter.PNCounterQuorumWriteTest;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPNCounterQuorumWriteTest extends PNCounterQuorumWriteTest {

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
    protected PNCounter pnCounter(int index) {
        return clients.client(index).getPNCounter(PN_COUNTER_NAME + quorumType.name());
    }

}
