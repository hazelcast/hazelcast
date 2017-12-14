package com.hazelcast.client.quorum.multimap;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.quorum.multimap.TransactionalMultiMapQuorumReadTest;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class ClientTransactionalMultiMapQuorumReadTest extends TransactionalMultiMapQuorumReadTest {

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
    public TransactionContext newTransactionContext(int index) {
        return clients.client(index).newTransactionContext(options);
    }

}