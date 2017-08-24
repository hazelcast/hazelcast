/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.http.NoHttpResponseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class RestClusterTest extends HazelcastTestSupport {

    private static final String STATUS_FORBIDDEN = "{\"status\":\"forbidden\"}";

    private Config config = new Config();

    @Before
    public void setup() {
        config.setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        config.setProperty(GroupProperty.HTTP_HEALTHCHECK_ENABLED.getName(), "true");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");
    }

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testDisabledRest() throws Exception {
        // REST should be disabled by default
        Config config = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        try {
            communicator.getClusterInfo();
            fail("Rest is disabled. Not expected to reach here!");
        } catch (NoHttpResponseException ignored) {
        }
    }

    @Test
    public void testClusterShutdown() throws Exception {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance2);

        assertEquals(HttpURLConnection.HTTP_OK, communicator.shutdownCluster("dev", "dev-pass"));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(instance1.getLifecycleService().isRunning());
                assertFalse(instance2.getLifecycleService().isRunning());
            }
        });
    }

    @Test
    public void testGetClusterState() throws Exception {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);

        HTTPCommunicator communicator1 = new HTTPCommunicator(instance1);
        HTTPCommunicator communicator2 = new HTTPCommunicator(instance2);

        instance1.getCluster().changeClusterState(ClusterState.FROZEN);
        assertEquals("{\"status\":\"success\",\"state\":\"frozen\"}",
                communicator1.getClusterState("dev", "dev-pass"));

        instance1.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertEquals("{\"status\":\"success\",\"state\":\"passive\"}",
                communicator2.getClusterState("dev", "dev-pass"));
    }

    @Test
    public void testChangeClusterState() throws Exception {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        assertEquals(STATUS_FORBIDDEN, communicator.changeClusterState("dev1", "dev-pass", "frozen").response);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.changeClusterState("dev", "dev-pass", "frozen").responseCode);

        assertClusterStateEventually(ClusterState.FROZEN, instance1);
        assertClusterStateEventually(ClusterState.FROZEN, instance2);
    }

    @Test
    public void testGetClusterVersion() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String expected = "{\"status\":\"success\","
                + "\"version\":\"" + instance.getCluster().getClusterVersion().toString() + "\"}";
        assertEquals(expected, communicator.getClusterVersion());
    }

    @Test
    public void testChangeClusterVersion() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.changeClusterVersion("dev", "dev-pass",
                instance.getCluster().getClusterVersion().toString()).responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.changeClusterVersion("dev1", "dev-pass", "1.2.3").response);
    }

    @Test
    public void testHotBackup() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.hotBackup("dev", "dev-pass").responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.hotBackup("dev1", "dev-pass").response);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.hotBackupInterrupt("dev", "dev-pass").responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.hotBackupInterrupt("dev1", "dev-pass").response);
    }

    @Test
    public void testForceAndPartialStart() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals(HttpURLConnection.HTTP_OK, communicator.forceStart("dev", "dev-pass").responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.forceStart("dev1", "dev-pass").response);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.partialStart("dev", "dev-pass").responseCode);
        assertEquals(STATUS_FORBIDDEN, communicator.partialStart("dev1", "dev-pass").response);
    }

    @Test
    public void testManagementCenterUrlChange() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT,
                communicator.changeManagementCenterUrl("dev", "dev-pass", "http://bla").responseCode);
    }

    @Test
    public void testListNodes() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HazelcastTestSupport.waitInstanceForSafeState(instance);
        String result = String.format("{\"status\":\"success\",\"response\":\"[%s]\n%s\n%s\"}",
                instance.getCluster().getLocalMember().toString(),
                BuildInfoProvider.getBuildInfo().getVersion(),
                System.getProperty("java.version"));
        assertEquals(result, communicator.listClusterNodes("dev", "dev-pass"));
    }

    @Test
    public void testListNodesWithWrongCredentials() throws Exception {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);
        HazelcastTestSupport.waitInstanceForSafeState(instance1);
        assertEquals(STATUS_FORBIDDEN, communicator.listClusterNodes("dev1", "dev-pass"));
    }

    @Test
    public void testShutdownNode() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
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

        try {
            assertEquals("{\"status\":\"success\"}", communicator.shutdownMember("dev", "dev-pass"));
        } catch (ConnectException ignored) {
            // if node shuts down before response is received, `java.net.ConnectException: Connection refused` is expected
        }

        assertOpenEventually(shutdownLatch);
        assertFalse(instance.getLifecycleService().isRunning());
    }

    @Test
    public void testShutdownNodeWithWrongCredentials() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);

        assertEquals(STATUS_FORBIDDEN, communicator.shutdownMember("dev1", "dev-pass"));
    }

    @Test
    public void simpleHealthCheck() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String result = communicator.getClusterHealth();

        assertEquals("Hazelcast::NodeState=ACTIVE\n" +
                "Hazelcast::ClusterState=ACTIVE\n" +
                "Hazelcast::ClusterSafe=TRUE\n" +
                "Hazelcast::MigrationQueueSize=0\n" +
                "Hazelcast::ClusterSize=1\n", result);
    }

    @Test(expected = NoHttpResponseException.class)
    public void fail_with_deactivatedHealthCheck() throws Exception {
        // Healthcheck REST URL is deactivated by default - no passed config on purpose
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        communicator.getClusterHealth();
    }

    @Test
    public void fail_on_healthcheck_url_with_garbage() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.getFailingClusterHealthWithTrailingGarbage());
    }

    @Test
    public void testHeadRequest_ClusterVersion() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterVersionURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterInfo() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_OK, communicator.headRequestToClusterInfoURI().responseCode);
    }

    @Test
    public void testHeadRequest_ClusterHealth() throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator.headRequestToClusterHealthURI();
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
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, communicator.headRequestToGarbageClusterHealthURI().responseCode);
    }
}
