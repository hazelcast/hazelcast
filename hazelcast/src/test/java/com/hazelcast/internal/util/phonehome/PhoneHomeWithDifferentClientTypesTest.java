/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import java.util.UUID;

import static com.hazelcast.internal.util.phonehome.PhoneHomeTestUtil.ClientAuthenticator.authenticate;
import static com.hazelcast.internal.util.phonehome.PhoneHomeTestUtil.NO_OP;
import static com.hazelcast.internal.util.phonehome.PhoneHomeTestUtil.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class PhoneHomeWithDifferentClientTypesTest extends HazelcastTestSupport {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private Node node;

    @Before
    public void setup() {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        node = getNode(instance);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testSingleMember_withMultipleClients() throws IOException {
        authenticate(node, UUID.randomUUID(), "4.1", ConnectionType.CPP_CLIENT, NO_OP);
        authenticate(node, UUID.randomUUID(), "4.2", ConnectionType.CPP_CLIENT, NO_OP);
        authenticate(node, UUID.randomUUID(), "4.1", ConnectionType.GO_CLIENT, NO_OP);
        authenticate(node, UUID.randomUUID(), "4.3", ConnectionType.PYTHON_CLIENT, NO_OP);

        waitUntilExpectedEndpointCountIsReached(node, 0);

        authenticate(node, UUID.randomUUID(), "4.0", ConnectionType.JAVA_CLIENT, () -> {
            authenticate(node, UUID.randomUUID(), "4.0.1", ConnectionType.CSHARP_CLIENT, () -> {
                authenticate(node, UUID.randomUUID(), "4.0.1", ConnectionType.NODEJS_CLIENT, () -> {
                    authenticate(node, UUID.randomUUID(), "4.0.1", ConnectionType.NODEJS_CLIENT, () -> {
                        sleepAtLeastMillis(100);
                        Map<String, String> parameters = getParameters(node);
                        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CPP, 0, 2, 2, 0, "4.1", "4.2");
                        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.GO, 0, 1, 1, 0, "4.1");
                        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.PYTHON, 0, 1, 1, 0, "4.3");
                        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.JAVA, 1, 1, 0, 100, "4.0");
                        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CSHARP, 1, 1, 0, 100, "4.0.1");
                        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.NODEJS, 2, 2, 0, 2 * 100, "4.0.1");
                    });
                });
            });
        });

        waitUntilExpectedEndpointCountIsReached(node, 0);

        Map<String, String> parameters = getParameters(node);
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.PYTHON, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.JAVA, 0, 0, 1, 0, "4.0");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CSHARP, 0, 0, 1, 0, "4.0.1");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.NODEJS, 0, 0, 2, 0, "4.0.1");
    }

    @Test
    public void testMultipleMembers_withMultipleClients() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node2 = getNode(instance);

        UUID uuid = UUID.randomUUID();
        authenticate(node, uuid, "4.0", ConnectionType.PYTHON_CLIENT, NO_OP);
        authenticate(node2, uuid, "4.0", ConnectionType.PYTHON_CLIENT, NO_OP);

        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node2, 0);

        UUID uuid2 = UUID.randomUUID();
        authenticate(node, uuid2, "4.0", ConnectionType.NODEJS_CLIENT, () -> {
            authenticate(node2, uuid2, "4.0", ConnectionType.NODEJS_CLIENT, () -> {
                authenticate(node, UUID.randomUUID(), "4.1", ConnectionType.CPP_CLIENT, NO_OP);
                authenticate(node2, UUID.randomUUID(), "4.2", ConnectionType.JAVA_CLIENT, NO_OP);
                sleepAtLeastMillis(100);

                waitUntilExpectedEndpointCountIsReached(node, 1);
                waitUntilExpectedEndpointCountIsReached(node2, 1);

                Map<String, String> parameters = getParameters(node);
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CPP, 0, 1, 1, 0, "4.1");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.PYTHON, 0, 1, 1, 0, "4.0");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.JAVA, 0, 0, 0, 0, "");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.NODEJS, 1, 1, 0, 100, "4.0");

                parameters = getParameters(node2);
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.PYTHON, 0, 1, 1, 0, "4.0");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.JAVA, 0, 1, 1, 0, "4.2");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
                assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.NODEJS, 1, 1, 0, 100, "4.0");

            });
        });

        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node2, 0);

        Map<String, String> parameters = getParameters(node);
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.PYTHON, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.JAVA, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.NODEJS, 0, 0, 1, 0, "4.0");

        parameters = getParameters(node2);
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CPP, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.GO, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.PYTHON, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.JAVA, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.CSHARP, 0, 0, 0, 0, "");
        assertParameters(parameters, PhoneHomeTestUtil.ClientPrefix.NODEJS, 0, 0, 1, 0, "4.0");
    }

    private void waitUntilExpectedEndpointCountIsReached(Node node, int expectedEndpointCount) {
        assertTrueEventually(() -> {
            assertEquals(expectedEndpointCount, node.clientEngine.getClientEndpointCount());
        });
    }

    private void assertParameters(Map<String, String> parameters, PhoneHomeTestUtil.ClientPrefix prefix, long activeConnectionCount,
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

    private String getActiveConnectionCount(Map<String, String> parameters, PhoneHomeTestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix());
    }

    private String getOpenedConnectionCount(Map<String, String> parameters, PhoneHomeTestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix() + "co");
    }

    private String getClosedConnectionCount(Map<String, String> parameters, PhoneHomeTestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix() + "cc");
    }

    private String getTotalConnectionDuration(Map<String, String> parameters, PhoneHomeTestUtil.ClientPrefix prefix) {
        return parameters.get(prefix.getPrefix() + "tcd");
    }

    private List<String> getClientVersions(Map<String, String> parameters, PhoneHomeTestUtil.ClientPrefix prefix) {
        return asList(parameters.get(prefix.getPrefix() + "cv").split(","));
    }
}
