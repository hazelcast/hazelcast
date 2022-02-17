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

package com.hazelcast.internal.ascii;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.ascii.HTTPCommunicator.ConnectionResponse;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.http.NoHttpResponseException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestClusterTest {

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config createConfig() {
        return smallInstanceConfig();
    }

    protected Config createConfigWithRestEnabled() {
        Config config = createConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        return config;
    }

    protected String getPassword() {
        // Community version doesn't check the password.
        return "";
    }

    @Test
    public void testDisabledRest() throws Exception {
        // REST should be disabled by default
        HazelcastInstance instance = factory.newHazelcastInstance(createConfig());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getClusterInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Test
    public void testClusterInfo_whenAdvancedNetworkWithoutClientEndpoint() throws Exception {
        // when advanced network config is enabled and no client endpoint is defined
        // then client connections are reported as 0
        Config config = createConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
              .setRestEndpointConfig(new RestServerEndpointConfig()
                      .setPort(9999)
                      .enableAllGroups());
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        String response = communicator.getClusterInfo();
        JsonObject json = Json.parse(response).asObject();
        assertEquals(0, json.getInt("connectionCount", -1));
    }

    @Test
    public void testClusterShutdown() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance2);


        ConnectionResponse response = communicator.shutdownCluster(config.getClusterName(), getPassword());
        assertSuccessJson(response);
        assertTrueEventually(() -> {
            assertFalse(instance1.getLifecycleService().isRunning());
            assertFalse(instance2.getLifecycleService().isRunning());
        });
    }

    @Test
    public void testGetClusterState() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        String clusterName = config.getClusterName();
        HTTPCommunicator communicator1 = new HTTPCommunicator(instance1);
        HTTPCommunicator communicator2 = new HTTPCommunicator(instance2);

        instance1.getCluster().changeClusterState(ClusterState.FROZEN);
        ConnectionResponse resp1 = communicator1.getClusterState(clusterName, getPassword());
        assertSuccessJson(resp1, "state", "frozen");

        instance1.getCluster().changeClusterState(ClusterState.PASSIVE);
        ConnectionResponse resp2 = communicator2.getClusterState(clusterName, getPassword());
        assertSuccessJson(resp2, "state", "passive");
    }

    @Test
    public void testChangeClusterState() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);
        String clusterName = config.getClusterName();

        ConnectionResponse resp = communicator.changeClusterState(clusterName, getPassword(), "frozen");
        assertSuccessJson(resp, "state", "frozen");

        assertClusterStateEventually(ClusterState.FROZEN, instance1);
        assertClusterStateEventually(ClusterState.FROZEN, instance2);
    }

    @Test
    public void testGetClusterVersion() throws IOException {
        final HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertJsonContains(communicator.getClusterVersion(),
                "status", "success",
                "version", instance.getCluster().getClusterVersion().toString());
    }

    @Test
    public void testChangeClusterVersion() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp = communicator.changeClusterVersion(clusterName, getPassword(),
                instance.getCluster().getClusterVersion().toString());
        assertSuccessJson(resp, "version", instance.getCluster().getClusterVersion().toString());
    }

    @Test
    public void testHotBackup() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp = communicator.hotBackup(clusterName, getPassword());
        assertSuccessJson(resp);

        ConnectionResponse resp1 = communicator.hotBackupInterrupt(clusterName, getPassword());
        assertSuccessJson(resp1);
    }

    @Test
    public void testForceAndPartialStart() throws IOException {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp1 = communicator.forceStart(clusterName, getPassword());
        assertEquals(HttpURLConnection.HTTP_OK, resp1.responseCode);
        assertJsonContains(resp1.response, "status", "fail");

        ConnectionResponse resp2 = communicator.partialStart(clusterName, getPassword());
        assertEquals(HttpURLConnection.HTTP_OK, resp2.responseCode);
        assertJsonContains(resp2.response, "status", "fail");
    }

    @Test
    public void testListNodes() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HazelcastTestSupport.waitInstanceForSafeState(instance);
        String clusterName = config.getClusterName();
        ConnectionResponse resp = communicator.listClusterNodes(clusterName, getPassword());
        assertSuccessJson(resp,
                "response", String.format("[%s]\n%s\n%s", instance.getCluster().getLocalMember().toString(),
                        BuildInfoProvider.getBuildInfo().getVersion(),
                        System.getProperty("java.version")));
    }

    @Test
    public void testShutdownNode() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        instance.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                    shutdownLatch.countDown();
                }
            }
        });
        String clusterName = config.getClusterName();
        try {
            assertJsonContains(communicator.shutdownMember(clusterName, getPassword()).response, "status", "success");
        } catch (SocketException ignored) {
            // if the node shuts down before response is received, a `SocketException` (or instance of its subclass) is expected
        } catch (NoHttpResponseException ignored) {
            // `NoHttpResponseException` is also a possible outcome when a node shut down before it has a chance
            // to send a response back to a client.
        }


        assertOpenEventually(shutdownLatch);
        assertFalse(instance.getLifecycleService().isRunning());
    }

    @Test
    public void simpleHealthCheck() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String result = communicator.getClusterHealth();

        JsonObject jsonResult = assertJsonContains(result,
                "nodeState", "ACTIVE",
                "clusterState", "ACTIVE");
        assertTrue(jsonResult.getBoolean("clusterSafe", false));
        assertEquals(0, jsonResult.getInt("migrationQueueSize", -1));
        assertEquals(1, jsonResult.getInt("clusterSize", -1));
    }

    @Test
    public void healthCheckWithPathParameters() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals("\"ACTIVE\"", communicator.getClusterHealth("/node-state"));
        assertEquals("\"ACTIVE\"", communicator.getClusterHealth("/cluster-state"));
        assertEquals(HttpURLConnection.HTTP_OK, communicator.getClusterHealthResponseCode("/cluster-safe"));
        assertEquals("0", communicator.getClusterHealth("/migration-queue-size"));
        assertEquals("1", communicator.getClusterHealth("/cluster-size"));
    }

    @Test
    public void healthCheckWithUnknownPathParameter() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.getClusterHealthResponseCode("/unknown-parameter"));
    }

    @Test(expected = IOException.class)
    public void fail_with_deactivatedHealthCheck() throws Exception {
        // Healthcheck REST URL is deactivated by default - no passed config on purpose
        HazelcastInstance instance = factory.newHazelcastInstance(null);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        communicator.getClusterHealth();
    }

    @Test
    public void fail_on_healthcheck_url_with_garbage() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.getFailingClusterHealthWithTrailingGarbage());
    }

    @Test
    public void testHeadRequest_ClusterVersion() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterVersionURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterInfo() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterInfoURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterHealth() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response = communicator.headRequestToClusterHealthURI();
        assertEquals(HttpURLConnection.HTTP_OK, response.responseCode);
        assertEquals(response.responseHeaders.get("Hazelcast-NodeState").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-NodeState"), "ACTIVE");
        assertEquals(response.responseHeaders.get("Hazelcast-ClusterState").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-ClusterState"), "ACTIVE");
        assertEquals(response.responseHeaders.get("Hazelcast-ClusterSize").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-ClusterSize"), "2");
        assertEquals(response.responseHeaders.get("Hazelcast-MigrationQueueSize").size(), 1);
        assertContains(response.responseHeaders.get("Hazelcast-MigrationQueueSize"), "0");
    }

    @Test
    public void testHeadRequest_GarbageClusterHealth() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HTTP_NOT_FOUND, communicator.headRequestToGarbageClusterHealthURI().responseCode);
    }

    @Test
    public void http_get_returns_response_code_200_when_member_is_ready_to_use() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance(createConfigWithRestEnabled());
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        int healthReadyResponseCode = communicator.getHealthReadyResponseCode();
        assertEquals(HttpURLConnection.HTTP_OK, healthReadyResponseCode);
    }

    @Test
    public void testSetLicenseKey() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response =
                communicator.setLicense(config.getClusterName(), getPassword(), "whatever");
        assertSuccessJson(response);
    }

    @Test
    public void testConfigReload() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response = communicator.configReload(config.getClusterName(), getPassword());

        // Reload is enterprise feature. Should fail here.
        assertJsonContains(response.response, "status", "fail",
                "message", "Configuration Reload requires Hazelcast Enterprise Edition");
    }

    @Test
    public void testConfigUpdate() throws Exception {
        Config config = createConfigWithRestEnabled();
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        ConnectionResponse response = communicator.configUpdate(
                config.getClusterName(), getPassword(), "hazelcast:\n"
        );

        // Reload is enterprise feature. Should fail here.
        assertJsonContains(response.response, "status", "fail",
                "message", "Configuration Update requires Hazelcast Enterprise Edition");
    }

    private JsonObject assertJsonContains(String json, String... attributesAndValues) {
        JsonObject object = Json.parse(json).asObject();
        for (int i = 0; i < attributesAndValues.length; ) {
            String key = attributesAndValues[i++];
            String expectedValue = attributesAndValues[i++];
            assertEquals(expectedValue, object.getString(key, null));
        }
        return object;
    }

    private void assertSuccessJson(ConnectionResponse resp, String... attributesAndValues) {
        assertEquals(HttpURLConnection.HTTP_OK, resp.responseCode);
        assertJsonContains(resp.response, "status", "success");
        if (attributesAndValues.length > 0) {
            assertJsonContains(resp.response, attributesAndValues);
        }
    }
}
