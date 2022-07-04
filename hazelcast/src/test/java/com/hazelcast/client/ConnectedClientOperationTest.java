/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client;

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.operations.GetConnectedClientsOperation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        assertClusterSize(2, h1, h2);

        int numberOfClients = 6;
        for (int i = 0; i < numberOfClients; i++) {
            factory.newHazelcastClient();
        }

        Node node = getNode(h1);
        Map<String, Long> clientStats = node.clientEngine.getActiveClientsInCluster();

        assertEquals(numberOfClients, clientStats.get(ConnectionType.JAVA_CLIENT).intValue());
        assertNull(clientStats.get(ConnectionType.CPP_CLIENT));
        assertNull(clientStats.get(ConnectionType.CSHARP_CLIENT));
        assertNull(clientStats.get(ConnectionType.NODEJS_CLIENT));
        assertNull(clientStats.get(ConnectionType.PYTHON_CLIENT));
        assertNull(clientStats.get(ConnectionType.GO_CLIENT));
    }

    @Test
    public void testGetConnectedClientsOperation_WhenZeroClientConnects() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        Node node = getNode(instance);

        Operation operation = new GetConnectedClientsOperation();
        OperationService operationService = node.nodeEngine.getOperationService();
        Future<Map<String, String>> future =
                operationService.invokeOnTarget(ClientEngineImpl.SERVICE_NAME, operation, node.address);
        Map<String, String> clients = future.get();
        assertEquals(0, clients.size());
    }

    @Test
    public void testGetConnectedClientsOperation_WhenMoreThanZeroClientConnects() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();

        factory.newHazelcastClient();
        factory.newHazelcastClient();

        Node node = getNode(instance);
        Operation operation = new GetConnectedClientsOperation();
        OperationService operationService = node.nodeEngine.getOperationService();
        Future<Map<String, String>> future =
                operationService.invokeOnTarget(ClientEngineImpl.SERVICE_NAME, operation, node.address);
        Map<String, String> clients = future.get();
        assertEquals(2, clients.size());
    }
}
