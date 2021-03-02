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

import com.hazelcast.client.impl.ClientEndpointStatisticsManagerImpl;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
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
import java.util.UUID;

import static com.hazelcast.internal.util.phonehome.PhoneHomeTestUtil.ClientAuthenticator.authenticate;
import static com.hazelcast.internal.util.phonehome.PhoneHomeTestUtil.NO_OP;
import static com.hazelcast.internal.util.phonehome.PhoneHomeTestUtil.getNode;
import static com.hazelcast.internal.util.phonehome.PhoneHomeTestUtil.getParameters;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class})
public class PhoneHomeClientsTest extends HazelcastTestSupport {

    private static final Map<PhoneHomeTestUtil.ClientPrefix, String> PREFIX_TO_CLIENT_TYPE = new HashMap<>(6);

    static {
        PREFIX_TO_CLIENT_TYPE.put(PhoneHomeTestUtil.ClientPrefix.CPP, ConnectionType.CPP_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(PhoneHomeTestUtil.ClientPrefix.CSHARP, ConnectionType.CSHARP_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(PhoneHomeTestUtil.ClientPrefix.JAVA, ConnectionType.JAVA_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(PhoneHomeTestUtil.ClientPrefix.NODEJS, ConnectionType.NODEJS_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(PhoneHomeTestUtil.ClientPrefix.PYTHON, ConnectionType.PYTHON_CLIENT);
        PREFIX_TO_CLIENT_TYPE.put(PhoneHomeTestUtil.ClientPrefix.GO, ConnectionType.GO_CLIENT);
    }

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private Node node;

    @Parameterized.Parameter
    public PhoneHomeTestUtil.ClientPrefix clientPrefix;

    @Parameterized.Parameters(name = "clientType:{0}")
    public static Collection<Object[]> parameters() {
        return asList(
                new Object[][]{
                        {PhoneHomeTestUtil.ClientPrefix.CPP},
                        {PhoneHomeTestUtil.ClientPrefix.CSHARP},
                        {PhoneHomeTestUtil.ClientPrefix.GO},
                        {PhoneHomeTestUtil.ClientPrefix.JAVA},
                        {PhoneHomeTestUtil.ClientPrefix.NODEJS},
                        {PhoneHomeTestUtil.ClientPrefix.PYTHON},
                }
        );
    }

    @Before
    public void setup() {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        node = getNode(instance);
        ((ClientEngineImpl) node.getClientEngine()).setEndpointStatisticsManager(new ClientEndpointStatisticsManagerImpl());
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testSingleClient_withSingleMember_whenTheClientIsActive() throws IOException {
        authenticate(node, UUID.randomUUID(), "4.0", getClientType(), () -> {
            sleepAtLeastMillis(100);
            assertParameters(node, 1, 1, 0, 100, "4.0");
        });
    }

    @Test
    public void testSingleClient_withMultipleMembers_whenTheClientIsActive() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node2 = getNode(instance);
        UUID uuid = UUID.randomUUID();
        authenticate(node, uuid, "4.0.1", getClientType(), () -> {
            authenticate(node2, uuid, "4.0.1", getClientType(), () -> {
                sleepAtLeastMillis(100);
                assertParameters(node, 1, 1, 0, 100, "4.0.1");
                assertParameters(node2, 1, 1, 0, 100, "4.0.1");
            });
        });
    }

    @Test
    public void testSingleClient_withSingleMember_whenTheClientIsShutdown() throws IOException {
        authenticate(node, UUID.randomUUID(), "4.2-BETA", getClientType(), () -> {
            sleepAtLeastMillis(100);
        });
        waitUntilExpectedEndpointCountIsReached(node, 0);
        assertParameters(node, 0, 1, 1, 100, "4.2-BETA");
    }

    @Test
    public void testSingleClient_withMultipleMembers_whenTheClientIsShutdown() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node2 = getNode(instance);
        UUID uuid = UUID.randomUUID();
        authenticate(node, uuid, "v4.0", getClientType(), () -> {
            authenticate(node2, uuid, "v4.0", getClientType(), () -> {
                sleepAtLeastMillis(100);
            });
        });
        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node2, 0);
        assertParameters(node, 0, 1, 1, 100, "v4.0");
        assertParameters(node2, 0, 1, 1, 100, "v4.0");
    }

    @Test
    public void testMultipleClients_withSingleMember_whenTheClientsAreActive() throws IOException {
        authenticate(node, UUID.randomUUID(), "4.1", getClientType(), () -> {
            authenticate(node, UUID.randomUUID(), "4.1", getClientType(), () -> {
                sleepAtLeastMillis(90);
                assertParameters(node, 2, 2, 0, 2 * 90, "4.1");
            });
        });
    }

    @Test
    public void testMultipleClients_withSingleMember_whenTheClientsAreShutdown() throws IOException {
        authenticate(node, UUID.randomUUID(), "4.0.1", getClientType(), () -> {
            authenticate(node, UUID.randomUUID(), "4.1", getClientType(), () -> {
                sleepAtLeastMillis(110);
            });
        });
        waitUntilExpectedEndpointCountIsReached(node, 0);
        assertParameters(node, 0, 2, 2, 2 * 110, "4.0.1", "4.1");
    }

    @Test
    public void testMultipleClients_withSingleMember_whenSomeClientsAreShutdown() throws IOException {
        authenticate(node, UUID.randomUUID(), "4.0", getClientType(), NO_OP);
        waitUntilExpectedEndpointCountIsReached(node, 0);
        authenticate(node, UUID.randomUUID(), "4.0", getClientType(), () -> {
            authenticate(node, UUID.randomUUID(), "4.1", getClientType(), () -> {
                sleepAtLeastMillis(80);
                assertParameters(node, 2, 3, 1, 2 * 80, "4.0", "4.1");
            });
        });
    }

    @Test
    public void testMultipleClients_withMultipleMembers_whenTheClientsAreActive() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node2 = getNode(instance);
        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        authenticate(node, uuid, "4.1", getClientType(), () -> {
            authenticate(node2, uuid, "4.1", getClientType(), () -> {
                authenticate(node, uuid2, "4.1", getClientType(), () -> {
                    authenticate(node2, uuid2, "4.1", getClientType(), () -> {
                        sleepAtLeastMillis(90);
                        assertParameters(node, 2, 2, 0, 2 * 90, "4.1");
                        assertParameters(node2, 2, 2, 0, 2 * 90, "4.1");
                    });
                });
            });
        });
    }

    @Test
    public void testMultipleClients_withMultipleMembers_whenTheClientsAreShutdown() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node2 = getNode(instance);
        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        authenticate(node, uuid, "4.0.1", getClientType(), () -> {
            authenticate(node2, uuid, "4.0.1", getClientType(), () -> {
                authenticate(node, uuid2, "4.1", getClientType(), () -> {
                    authenticate(node2, uuid2, "4.1", getClientType(), () -> {
                        sleepAtLeastMillis(110);
                    });
                });
            });
        });
        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node2, 0);
        assertParameters(node, 0, 2, 2, 2 * 110, "4.0.1", "4.1");
        assertParameters(node2, 0, 2, 2, 2 * 110, "4.0.1", "4.1");
    }

    @Test
    public void testMultipleClients_withMultipleMembers_whenSomeClientsAreShutdown() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node2 = getNode(instance);
        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        authenticate(node, uuid, "4.0", getClientType(), NO_OP);
        authenticate(node2, uuid2, "4.0", getClientType(), NO_OP);
        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node2, 0);
        authenticate(node, uuid2, "4.0", getClientType(), () -> {
            authenticate(node2, uuid2, "4.0", getClientType(), () -> {
                authenticate(node, uuid3, "4.1", getClientType(), () -> {
                    authenticate(node2, uuid3, "4.1", getClientType(), () -> {
                        sleepAtLeastMillis(80);
                        assertParameters(node, 2, 3, 1, 2 * 80, "4.0", "4.1");
                        assertParameters(node2, 2, 3, 1, 2 * 80, "4.0", "4.1");
                    });
                });
            });
        });
    }

    @Test
    public void testConsecutivePhoneHomes() throws IOException {
        authenticate(node, UUID.randomUUID(), "4.3", getClientType(), () -> {
            sleepAtLeastMillis(100);
            assertParameters(node, 1, 1, 0, 100, "4.3");
        });
        waitUntilExpectedEndpointCountIsReached(node, 0);
        assertParameters(node, 0, 0, 1, 0, "4.3");
        assertParameters(node, 0, 0, 0, 0, "");
    }

    @Test
    public void testUniSocketClients() throws IOException {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfig());
        Node node2 = getNode(instance);

        authenticate(node, UUID.randomUUID(), "4.1", getClientType(), NO_OP);
        authenticate(node2, UUID.randomUUID(), "4.1", getClientType(), NO_OP);

        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node2, 0);

        authenticate(node, UUID.randomUUID(), "4.2", getClientType(), () -> {
            authenticate(node, UUID.randomUUID(), "4.3", getClientType(), () -> {
                authenticate(node2, UUID.randomUUID(), "4.0.1", getClientType(), () -> {
                    authenticate(node2, UUID.randomUUID(), "4.1", getClientType(), () -> {
                        authenticate(node2, UUID.randomUUID(), "4.1", getClientType(), () -> {
                            sleepAtLeastMillis(100);
                            assertParameters(node, 5, 3, 1, 2 * 100, "4.1", "4.2", "4.3");
                            assertParameters(node2, 5, 4, 1, 3 * 100, "4.0.1", "4.1");
                        });
                    });
                });
            });
        });

        waitUntilExpectedEndpointCountIsReached(node, 0);
        waitUntilExpectedEndpointCountIsReached(node2, 0);

        assertParameters(node, 0, 0, 2, 0, "4.2", "4.3");
        assertParameters(node2, 0, 0, 3, 0, "4.0.1", "4.1");
    }

    private void waitUntilExpectedEndpointCountIsReached(Node node, int expectedEndpointCount) {
        assertTrueEventually(() -> {
            assertEquals(expectedEndpointCount, node.clientEngine.getClientEndpointCount());
        });
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
        return parameters.get(clientPrefix.getPrefix() + "co");
    }

    private String getClosedConnectionCount(Map<String, String> parameters) {
        return parameters.get(clientPrefix.getPrefix() + "cc");
    }

    private String getTotalConnectionDuration(Map<String, String> parameters) {
        return parameters.get(clientPrefix.getPrefix() + "tcd");
    }

    private List<String> getClientVersions(Map<String, String> parameters) {
        return asList(parameters.get(clientPrefix.getPrefix() + "cv").split(","));
    }
}
