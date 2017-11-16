package com.hazelcast.client.quorum.set;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.ISet;
import com.hazelcast.quorum.set.SetReadQuorumTest;
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
public class ClientSetReadQuorumTest extends SetReadQuorumTest {

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

    protected ISet set(int index) {
        return CLIENTS.client(index).getSet(SET_NAME + quorumType.name());
    }

}
