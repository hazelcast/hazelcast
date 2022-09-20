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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.client.Client;
import com.hazelcast.client.ClientListener;
import com.hazelcast.client.ClientService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.phonehome.TestUtil.CLIENT_VERSIONS_SEPARATOR;
import static com.hazelcast.internal.util.phonehome.TestUtil.CLIENT_VERSIONS_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.CONNECTIONS_CLOSED_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.CONNECTIONS_OPENED_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.TOTAL_CONNECTION_DURATION_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.getNode;
import static com.hazelcast.internal.util.phonehome.TestUtil.getParameters;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class PhoneHomeClientsTest extends HazelcastTestSupport {

    private static final Map<TestUtil.ClientPrefix, String> PREFIX_TO_CLIENT_TYPE = new HashMap<>(6);

    static {
        PREFIX_TO_CLIENT_TYPE.put(TestUtil.ClientPrefix.CLC, ConnectionType.CL_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(TestUtil.ClientPrefix.CPP, ConnectionType.CPP_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(TestUtil.ClientPrefix.CSHARP, ConnectionType.CSHARP_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(TestUtil.ClientPrefix.JAVA, ConnectionType.JAVA_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(TestUtil.ClientPrefix.NODEJS, ConnectionType.NODEJS_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(TestUtil.ClientPrefix.PYTHON, ConnectionType.PYTHON_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(TestUtil.ClientPrefix.GO, ConnectionType.GO_CLIENT);
    }

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private final TestUtil.DummyClientFactory clientFactory = new TestUtil.DummyClientFactory();
    private final AtomicInteger disconnectedClientCount = new AtomicInteger();
    private Node node;

    @Parameterized.Parameter
    public TestUtil.ClientPrefix clientPrefix;

    @Parameterized.Parameters(name = "clientType:{0}")
    public static Collection<Object[]> parameters() {
        return asList(
                new Object[][]{
                        {TestUtil.ClientPrefix.CLC},
                        {TestUtil.ClientPrefix.CPP},
                        {TestUtil.ClientPrefix.CSHARP},
                        {TestUtil.ClientPrefix.GO},
                        {TestUtil.ClientPrefix.JAVA},
                        {TestUtil.ClientPrefix.NODEJS},
                        {TestUtil.ClientPrefix.PYTHON},
                }
        );
    }

    @Before
    public void setup() {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        node = getNode(instance);
    }

    @After
    public void cleanup() {
        clientFactory.terminateAll();
        factory.terminateAll();
    }

    @Test
    @Category(QuickTest.class) //marked quick to form a small subset that also runs in the PR builder
    public void testSingleClient_withSingleMember_whenTheClientIsActive() throws IOException {
        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.0");
        client.connectTo(node);

        sleepAtLeastMillis(100);
        assertParameters(node, 1, 1, 0, 100, "4.0");
    }

    @Test
    @Category(SlowTest.class)
    public void testSingleClient_withMultipleMembers_whenTheClientIsActive() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node1 = getNode(instance);

        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.0.1");
        client.connectTo(node);
        client.connectTo(node1);

        sleepAtLeastMillis(100);
        assertParameters(node, 1, 1, 0, 100, "4.0.1");
        assertParameters(node1, 1, 1, 0, 100, "4.0.1");
    }

    @Test
    @Category(QuickTest.class) //marked quick to form a small subset that also runs in the PR builder
    public void testSingleClient_withSingleMember_whenTheClientIsShutdown() throws IOException {
        addClientListener(node);
        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.2-BETA");
        TestUtil.DummyConnection connection = client.connectTo(node);

        sleepAtLeastMillis(100);
        connection.close();
        assertEqualsEventually(1, disconnectedClientCount);
        assertParameters(node, 0, 1, 1, 100, "4.2-BETA");
    }

    @Test
    @Category(SlowTest.class)
    public void testSingleClient_withMultipleMembers_whenTheClientIsShutdown() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node1 = getNode(instance);
        addClientListener(node);
        addClientListener(node1);

        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "v4.0");
        TestUtil.DummyConnection connection = client.connectTo(node);
        TestUtil.DummyConnection connection1 = client.connectTo(node1);

        sleepAtLeastMillis(100);
        connection.close();
        connection1.close();
        assertEqualsEventually(2, disconnectedClientCount);
        assertParameters(node, 0, 1, 1, 100, "v4.0");
        assertParameters(node1, 0, 1, 1, 100, "v4.0");
    }

    @Test
    @Category(SlowTest.class)
    public void testMultipleClients_withSingleMember_whenTheClientsAreActive() throws IOException {
        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyClient client1 = clientFactory.newClient(getClientType(), "4.1");
        client.connectTo(node);
        client1.connectTo(node);

        sleepAtLeastMillis(90);
        assertParameters(node, 2, 2, 0, 2 * 90, "4.1");
    }

    @Test
    @Category(SlowTest.class)
    public void testMultipleClients_withSingleMember_whenTheClientsAreShutdown() throws IOException {
        addClientListener(node);
        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.0.1");
        TestUtil.DummyClient client1 = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyConnection connection = client.connectTo(node);
        TestUtil.DummyConnection connection1 = client1.connectTo(node);

        sleepAtLeastMillis(110);
        connection.close();
        connection1.close();
        assertEqualsEventually(2, disconnectedClientCount);
        assertParameters(node, 0, 2, 2, 2 * 110, "4.0.1", "4.1");
    }

    @Test
    @Category(SlowTest.class)
    public void testMultipleClients_withSingleMember_whenSomeClientsAreShutdown() throws IOException {
        addClientListener(node);
        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.0");
        TestUtil.DummyConnection connection = client.connectTo(node);

        connection.close();
        assertEqualsEventually(1, disconnectedClientCount);

        TestUtil.DummyClient client1 = clientFactory.newClient(getClientType(), "4.0");
        client1.connectTo(node);
        TestUtil.DummyClient client2 = clientFactory.newClient(getClientType(), "4.1");
        client2.connectTo(node);

        sleepAtLeastMillis(80);
        assertParameters(node, 2, 3, 1, 2 * 80, "4.0", "4.1");
    }

    @Test
    @Category(SlowTest.class)
    public void testMultipleClients_withMultipleMembers_whenTheClientsAreActive() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node1 = getNode(instance);

        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyClient client1 = clientFactory.newClient(getClientType(), "4.1");
        client.connectTo(node);
        client.connectTo(node1);
        client1.connectTo(node);
        client1.connectTo(node1);

        sleepAtLeastMillis(90);
        assertParameters(node, 2, 2, 0, 2 * 90, "4.1");
        assertParameters(node1, 2, 2, 0, 2 * 90, "4.1");
    }

    @Test
    @Category(SlowTest.class)
    public void testMultipleClients_withMultipleMembers_whenTheClientsAreShutdown() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node1 = getNode(instance);
        addClientListener(node);
        addClientListener(node1);

        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.0.1");
        TestUtil.DummyClient client1 = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyConnection connection = client.connectTo(node);
        TestUtil.DummyConnection connection1 = client.connectTo(node1);
        TestUtil.DummyConnection connection2 = client1.connectTo(node);
        TestUtil.DummyConnection connection3 = client1.connectTo(node1);

        sleepAtLeastMillis(110);
        connection.close();
        connection1.close();
        connection2.close();
        connection3.close();
        assertEqualsEventually(4, disconnectedClientCount);
        assertParameters(node, 0, 2, 2, 2 * 110, "4.0.1", "4.1");
        assertParameters(node1, 0, 2, 2, 2 * 110, "4.0.1", "4.1");
    }

    @Test
    @Category(SlowTest.class)
    public void testMultipleClients_withMultipleMembers_whenSomeClientsAreShutdown() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node1 = getNode(instance);
        addClientListener(node);
        addClientListener(node1);

        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.0");
        TestUtil.DummyConnection connection = client.connectTo(node);
        TestUtil.DummyConnection connection1 = client.connectTo(node1);

        connection.close();
        connection1.close();
        assertEqualsEventually(2, disconnectedClientCount);

        TestUtil.DummyClient client1 = clientFactory.newClient(getClientType(), "4.0");
        TestUtil.DummyClient client2 = clientFactory.newClient(getClientType(), "4.1");
        client1.connectTo(node);
        client1.connectTo(node1);
        client2.connectTo(node);
        client2.connectTo(node1);

        sleepAtLeastMillis(80);
        assertParameters(node, 2, 3, 1, 2 * 80, "4.0", "4.1");
        assertParameters(node1, 2, 3, 1, 2 * 80, "4.0", "4.1");
    }

    @Test
    @Category(SlowTest.class)
    public void testConsecutivePhoneHomes() throws IOException {
        addClientListener(node);
        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.3");
        TestUtil.DummyConnection connection = client.connectTo(node);

        sleepAtLeastMillis(100);
        assertParameters(node, 1, 1, 0, 100, "4.3");

        connection.close();
        assertEqualsEventually(1, disconnectedClientCount);
        assertParameters(node, 0, 0, 1, 0, "4.3");
        assertParameters(node, 0, 0, 0, 0, "");
    }

    @Test
    @Category(SlowTest.class)
    public void testUniSocketClients() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node1 = getNode(instance);
        addClientListener(node);
        addClientListener(node1);

        TestUtil.DummyClient client = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyConnection connection = client.connectTo(node);
        connection.close();

        TestUtil.DummyClient client1 = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyConnection connection1 = client1.connectTo(node1);
        connection1.close();

        assertEqualsEventually(2, disconnectedClientCount);

        TestUtil.DummyClient client2 = clientFactory.newClient(getClientType(), "4.2");
        TestUtil.DummyConnection connection2 = client2.connectTo(node);

        TestUtil.DummyClient client3 = clientFactory.newClient(getClientType(), "4.3");
        TestUtil.DummyConnection connection3 = client3.connectTo(node);

        TestUtil.DummyClient client4 = clientFactory.newClient(getClientType(), "4.0.1");
        TestUtil.DummyConnection connection4 = client4.connectTo(node1);

        TestUtil.DummyClient client5 = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyConnection connection5 = client5.connectTo(node1);

        TestUtil.DummyClient client6 = clientFactory.newClient(getClientType(), "4.1");
        TestUtil.DummyConnection connection6 = client6.connectTo(node1);

        sleepAtLeastMillis(100);
        assertParameters(node, 5, 3, 1, 2 * 100, "4.1", "4.2", "4.3");
        assertParameters(node1, 5, 4, 1, 3 * 100, "4.0.1", "4.1");

        connection2.close();
        connection3.close();
        connection4.close();
        connection5.close();
        connection6.close();

        assertEqualsEventually(7, disconnectedClientCount);

        assertParameters(node, 0, 0, 2, 0, "4.2", "4.3");
        assertParameters(node1, 0, 0, 3, 0, "4.0.1", "4.1");
    }

    private void assertParameters(Node node, long activeConnectionCount, long openedConnectionCount,
                                  long closedConnectionCount, long duration, String... versions) {
        Map<String, String> parameters = getParameters(node);
        assertEquals(activeConnectionCount, Long.parseLong(getActiveConnectionCount(parameters)));
        assertEquals(openedConnectionCount, Long.parseLong(getOpenedConnectionCount(parameters)));
        assertEquals(closedConnectionCount, Long.parseLong(getClosedConnectionCount(parameters)));
        assertTrue(Long.parseLong(getTotalConnectionDuration(parameters)) >= duration);
        List<String> clientVersions = getClientVersions(parameters);
        assertEquals(versions.length, clientVersions.size());
        for (String version : versions) {
            assertTrue(clientVersions.contains(version));
        }
    }

    private String getClientType() {
        return PREFIX_TO_CLIENT_TYPE.get(clientPrefix);
    }

    private String getActiveConnectionCount(Map<String, String> parameters) {
        return parameters.get(clientPrefix.getPrefix());
    }

    private String getOpenedConnectionCount(Map<String, String> parameters) {
        return parameters.get(clientPrefix.getPrefix() + CONNECTIONS_OPENED_SUFFIX);
    }

    private String getClosedConnectionCount(Map<String, String> parameters) {
        return parameters.get(clientPrefix.getPrefix() + CONNECTIONS_CLOSED_SUFFIX);
    }

    private String getTotalConnectionDuration(Map<String, String> parameters) {
        return parameters.get(clientPrefix.getPrefix() + TOTAL_CONNECTION_DURATION_SUFFIX);
    }

    private List<String> getClientVersions(Map<String, String> parameters) {
        return asList(parameters.get(clientPrefix.getPrefix() + CLIENT_VERSIONS_SUFFIX).split(CLIENT_VERSIONS_SEPARATOR));
    }

    private void addClientListener(Node node) {
        ClientService clientService = node.getNodeEngine().getHazelcastInstance().getClientService();
        clientService.addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
            }

            @Override
            public void clientDisconnected(Client client) {
                disconnectedClientCount.incrementAndGet();
            }
        });
    }
}
