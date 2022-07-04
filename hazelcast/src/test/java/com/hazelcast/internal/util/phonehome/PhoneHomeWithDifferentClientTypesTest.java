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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.phonehome.TestUtil.CLIENT_VERSIONS_SEPARATOR;
import static com.hazelcast.internal.util.phonehome.TestUtil.CLIENT_VERSIONS_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.CONNECTIONS_CLOSED_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.CONNECTIONS_OPENED_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.TOTAL_CONNECTION_DURATION_SUFFIX;
import static com.hazelcast.internal.util.phonehome.TestUtil.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class PhoneHomeWithDifferentClientTypesTest extends HazelcastTestSupport {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private final TestUtil.DummyClientFactory clientFactory = new TestUtil.DummyClientFactory();
    private Node node;

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
    public void testSingleMember_withMultipleClients() throws IOException {
        TestUtil.DummyClient cppClient = clientFactory.newClient(ConnectionType.CPP_CLIENT, "4.1");
        TestUtil.DummyConnection cppConnection = cppClient.connectTo(node);
        cppConnection.close();

        TestUtil.DummyClient cppClient1 = clientFactory.newClient(ConnectionType.CPP_CLIENT, "4.2");
        TestUtil.DummyConnection cppConnection1 = cppClient1.connectTo(node);
        cppConnection1.close();

        TestUtil.DummyClient goClient = clientFactory.newClient(ConnectionType.GO_CLIENT, "4.1");
        TestUtil.DummyConnection goConnection = goClient.connectTo(node);
        goConnection.close();

        TestUtil.DummyClient clClient = clientFactory.newClient(ConnectionType.CL_CLIENT, "1.0.0");
        TestUtil.DummyConnection clConnection = clClient.connectTo(node);
        clConnection.close();

        TestUtil.DummyClient pythonClient = clientFactory.newClient(ConnectionType.PYTHON_CLIENT, "4.3");
        TestUtil.DummyConnection pythonConnection = pythonClient.connectTo(node);
        pythonConnection.close();

        waitUntilExpectedEndpointCountIsReached(node, 0);

        TestUtil.DummyClient javaClient = clientFactory.newClient(ConnectionType.JAVA_CLIENT, "4.0");
        TestUtil.DummyConnection javaConnection = javaClient.connectTo(node);

        TestUtil.DummyClient cSharpClient = clientFactory.newClient(ConnectionType.CSHARP_CLIENT, "4.0.1");
        TestUtil.DummyConnection cSharpConnection = cSharpClient.connectTo(node);

        TestUtil.DummyClient nodeJSClient = clientFactory.newClient(ConnectionType.NODEJS_CLIENT, "4.0.1");
        TestUtil.DummyConnection nodeJSConnection = nodeJSClient.connectTo(node);

        TestUtil.DummyClient nodeJSClient1 = clientFactory.newClient(ConnectionType.NODEJS_CLIENT, "4.0.1");
        TestUtil.DummyConnection nodeJSConnection1 = nodeJSClient1.connectTo(node);

        sleepAtLeastMillis(100);

        Map<String, String> parameters = getParameters(node);
        assertParameters(parameters, TestUtil.ClientPrefix.CPP, 0, 2, 2, 0, "4.1", "4.2");
        assertParameters(parameters, TestUtil.ClientPrefix.GO, 0, 1, 1, 0, "4.1");
        assertParameters(parameters, TestUtil.ClientPrefix.CLC, 0, 1, 1, 0, "1.0.0");
        assertParameters(parameters, TestUtil.ClientPrefix.PYTHON, 0, 1, 1, 0, "4.3");
        assertParameters(parameters, TestUtil.ClientPrefix.JAVA, 1, 1, 0, 100, "4.0");
        assertParameters(parameters, TestUtil.ClientPrefix.CSHARP, 1, 1, 0, 100, "4.0.1");
        assertParameters(parameters, TestUtil.ClientPrefix.NODEJS, 2, 2, 0, 2 * 100, "4.0.1");

        javaConnection.close();
        cSharpConnection.close();
        nodeJSConnection.close();
        nodeJSConnection1.close();

        waitUntilExpectedEndpointCountIsReached(node, 0);

        parameters = getParameters(node);
        assertParameters(parameters, TestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CLC, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.PYTHON, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.JAVA, 0, 0, 1, 0, "4.0");
        assertParameters(parameters, TestUtil.ClientPrefix.CSHARP, 0, 0, 1, 0, "4.0.1");
        assertParameters(parameters, TestUtil.ClientPrefix.NODEJS, 0, 0, 2, 0, "4.0.1");
    }

    @Test
    public void testMultipleMembers_withMultipleClients() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node1 = getNode(instance);

        TestUtil.DummyClient pythonClient = clientFactory.newClient(ConnectionType.PYTHON_CLIENT, "4.0");
        TestUtil.DummyConnection pythonConnection = pythonClient.connectTo(node);
        TestUtil.DummyConnection pythonConnection1 = pythonClient.connectTo(node1);
        pythonConnection.close();
        pythonConnection1.close();

        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node1, 0);

        TestUtil.DummyClient nodeJSClient = clientFactory.newClient(ConnectionType.NODEJS_CLIENT, "4.0");
        TestUtil.DummyConnection nodeJSConnection = nodeJSClient.connectTo(node);
        TestUtil.DummyConnection nodeJSConnection1 = nodeJSClient.connectTo(node1);

        TestUtil.DummyClient cppClient = clientFactory.newClient(ConnectionType.CPP_CLIENT, "4.1");
        TestUtil.DummyConnection cppConnection = cppClient.connectTo(node);
        cppConnection.close();

        TestUtil.DummyClient javaClient = clientFactory.newClient(ConnectionType.JAVA_CLIENT, "4.2");
        TestUtil.DummyConnection javaConnection = javaClient.connectTo(node1);
        javaConnection.close();

        sleepAtLeastMillis(100);

        waitUntilExpectedEndpointCountIsReached(node, 1);
        waitUntilExpectedEndpointCountIsReached(node1, 1);

        Map<String, String> parameters = getParameters(node);
        assertParameters(parameters, TestUtil.ClientPrefix.CPP, 0, 1, 1, 0, "4.1");
        assertParameters(parameters, TestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CLC, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.PYTHON, 0, 1, 1, 0, "4.0");
        assertParameters(parameters, TestUtil.ClientPrefix.JAVA, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.NODEJS, 1, 1, 0, 100, "4.0");

        parameters = getParameters(node1);
        assertParameters(parameters, TestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CLC, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.PYTHON, 0, 1, 1, 0, "4.0");
        assertParameters(parameters, TestUtil.ClientPrefix.JAVA, 0, 1, 1, 0, "4.2");
        assertParameters(parameters, TestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.NODEJS, 1, 1, 0, 100, "4.0");

        nodeJSConnection.close();
        nodeJSConnection1.close();

        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node1, 0);

        parameters = getParameters(node);
        assertParameters(parameters, TestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CLC, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.PYTHON, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.JAVA, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.NODEJS, 0, 0, 1, 0, "4.0");

        parameters = getParameters(node1);
        assertParameters(parameters, TestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CLC, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.PYTHON, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.JAVA, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
        assertParameters(parameters, TestUtil.ClientPrefix.NODEJS, 0, 0, 1, 0, "4.0");

    }

    private void waitUntilExpectedEndpointCountIsReached(Node node, int expectedEndpointCount) {
        assertTrueEventually(() -> assertEquals(expectedEndpointCount, node.clientEngine.getClientEndpointCount()));
    }

    private void assertParameters(Map<String, String> parameters, TestUtil.ClientPrefix prefix, long activeConnectionCount,
                                  long openedConnectionCount, long closedConnectionCount, long duration,
                                  String... versions) {
        assertEquals(activeConnectionCount, Long.parseLong(getActiveConnectionCount(parameters, prefix)));
        assertEquals(openedConnectionCount, Long.parseLong(getOpenedConnectionCount(parameters, prefix)));
        assertEquals(closedConnectionCount, Long.parseLong(getClosedConnectionCount(parameters, prefix)));
        assertTrue(Long.parseLong(getTotalConnectionDuration(parameters, prefix)) >= duration);
        List<String> clientVersions = getClientVersions(parameters, prefix);
        assertEquals(versions.length, clientVersions.size());
        for (String version : versions) {
            assertTrue(clientVersions.contains(version));
        }
    }

    private Map<String, String> getParameters(Node node) {
        return new PhoneHome(node).phoneHome(true);
    }

    private String getActiveConnectionCount(Map<String, String> parameters, TestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix());
    }

    private String getOpenedConnectionCount(Map<String, String> parameters, TestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix() + CONNECTIONS_OPENED_SUFFIX);
    }

    private String getClosedConnectionCount(Map<String, String> parameters, TestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix() + CONNECTIONS_CLOSED_SUFFIX);
    }

    private String getTotalConnectionDuration(Map<String, String> parameters, TestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix() + TOTAL_CONNECTION_DURATION_SUFFIX);
    }

    private List<String> getClientVersions(Map<String, String> parameters, TestUtil.ClientPrefix prefix) {
        return asList(parameters.get(prefix.getPrefix() + CLIENT_VERSIONS_SUFFIX).split(CLIENT_VERSIONS_SEPARATOR));
    }
}
