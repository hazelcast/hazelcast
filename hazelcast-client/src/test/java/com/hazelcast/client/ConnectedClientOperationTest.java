package com.hazelcast.client;

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.operations.GetConnectedClientsOperation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConnectedClientOperationTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testNumberOfConnectedClients() throws Exception {
        HazelcastInstance h1 = factory.newHazelcastInstance();
        HazelcastInstance h2 = factory.newHazelcastInstance();
        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);

        int numberOfClients = 6;
        for (int i = 0; i < numberOfClients; i++) {
            factory.newHazelcastClient();
        }

        Node node = TestUtil.getNode(h1);
        Map<ClientType, Integer> clientStats = node.clientEngine.getConnectedClientStats();

        assertEquals(numberOfClients, clientStats.get(ClientType.JAVA).intValue());
        assertEquals(0, clientStats.get(ClientType.CPP).intValue());
        assertEquals(0, clientStats.get(ClientType.CSHARP).intValue());
        assertEquals(0, clientStats.get(ClientType.NODEJS).intValue());
        assertEquals(0, clientStats.get(ClientType.PYTHON).intValue());
        assertEquals(0, clientStats.get(ClientType.OTHER).intValue());
    }

    @Test
    public void testGetConnectedClientsOperation_WhenZeroClientConnects() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        Node node = TestUtil.getNode(instance);

        Operation operation = new GetConnectedClientsOperation();
        OperationService operationService = node.nodeEngine.getOperationService();
        Future<Map<String, ClientType>> future =
                operationService.invokeOnTarget(ClientEngineImpl.SERVICE_NAME, operation, node.address);
        Map<String, ClientType> clients = future.get();
        assertEquals(0, clients.size());
    }

    @Test
    public void testGetConnectedClientsOperation_WhenMoreThanZeroClientConnects() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();

        factory.newHazelcastClient();
        factory.newHazelcastClient();

        Node node = TestUtil.getNode(instance);
        Operation operation = new GetConnectedClientsOperation();
        OperationService operationService = node.nodeEngine.getOperationService();
        Future<Map<String, ClientType>> future =
                operationService.invokeOnTarget(ClientEngineImpl.SERVICE_NAME, operation, node.address);
        Map<String, ClientType> clients = future.get();
        assertEquals(2, clients.size());
    }
}
