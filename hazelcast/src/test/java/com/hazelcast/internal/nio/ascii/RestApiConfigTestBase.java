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

package com.hazelcast.internal.nio.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.Versions;

import java.io.IOException;
import java.net.UnknownHostException;

import static com.hazelcast.config.RestEndpointGroup.CLUSTER_READ;
import static com.hazelcast.config.RestEndpointGroup.CLUSTER_WRITE;
import static com.hazelcast.config.RestEndpointGroup.DATA;
import static com.hazelcast.config.RestEndpointGroup.HEALTH_CHECK;
import static com.hazelcast.config.RestEndpointGroup.HOT_RESTART;
import static com.hazelcast.config.RestEndpointGroup.PERSISTENCE;
import static com.hazelcast.config.RestEndpointGroup.WAN;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.Accessors.getAddress;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Shared code for HTTP REST API and Memcache protocol testing.
 */
public abstract class RestApiConfigTestBase extends AbstractTextProtocolsTestBase {

    public static final String POST = "POST";
    public static final String GET = "GET";
    public static final String DELETE = "DELETE";
    public static final String CRLF = "\r\n";

    protected static final TestUrl[] TEST_URLS = new TestUrl[]{
            new TestUrl(CLUSTER_READ, GET, "/hazelcast/rest/cluster", "\"members\":[{\"address\":\""),
            new TestUrl(CLUSTER_READ, POST, "/hazelcast/rest/management/cluster/state", "HTTP/1.1 400 Bad Request"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/changeState", "HTTP/1.1 400 Bad Request"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/version", "HTTP/1.1 400 Bad Request"),
            new TestUrl(CLUSTER_READ, GET, "/hazelcast/rest/management/cluster/version", Versions.CURRENT_CLUSTER_VERSION.toString()),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/clusterShutdown", "HTTP/1.1 400 Bad Request"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/memberShutdown", "HTTP/1.1 400 Bad Request"),
            new TestUrl(CLUSTER_READ, POST, "/hazelcast/rest/management/cluster/nodes", "HTTP/1.1 400 Bad Request"),
            new TestUrl(CLUSTER_READ, GET, "/hazelcast/rest/license", getLicenseInfoExpectedResponse()),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/forceStart", "HTTP/1.1 400 Bad Request"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/partialStart", "HTTP/1.1 400 Bad Request"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/hotBackup", "HTTP/1.1 400 Bad Request"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/hotBackupInterrupt", "HTTP/1.1 400 Bad Request"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/backup", "HTTP/1.1 400 Bad Request"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/backupInterrupt", "HTTP/1.1 400 Bad Request"),
            new TestUrl(PERSISTENCE, POST, "/hazelcast/rest/management/cluster/forceStart", "HTTP/1.1 400 Bad Request"),
            new TestUrl(PERSISTENCE, POST, "/hazelcast/rest/management/cluster/partialStart", "HTTP/1.1 400 Bad Request"),
            new TestUrl(PERSISTENCE, POST, "/hazelcast/rest/management/cluster/hotBackup", "HTTP/1.1 400 Bad Request"),
            new TestUrl(PERSISTENCE, POST, "/hazelcast/rest/management/cluster/hotBackupInterrupt", "HTTP/1.1 400 Bad Request"),
            new TestUrl(PERSISTENCE, POST, "/hazelcast/rest/management/cluster/backup", "HTTP/1.1 400 Bad Request"),
            new TestUrl(PERSISTENCE, POST, "/hazelcast/rest/management/cluster/backupInterrupt", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/sync/map", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/sync/allmaps", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/clearWanQueues", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/addWanConfig", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/pausePublisher", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/stopPublisher", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/resumePublisher", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/consistencyCheck/map", "HTTP/1.1 400 Bad Request"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/addWanConfig", "HTTP/1.1 400 Bad Request"),
            new TestUrl(HEALTH_CHECK, GET, "/hazelcast/health/node-state", "ACTIVE"),
            new TestUrl(HEALTH_CHECK, GET, "/hazelcast/health/cluster-state", "ACTIVE"),
            new TestUrl(HEALTH_CHECK, GET, "/hazelcast/health/cluster-safe", "HTTP/1.1 200"),
            new TestUrl(HEALTH_CHECK, GET, "/hazelcast/health/migration-queue-size", "HTTP/1.1 200"),
            new TestUrl(HEALTH_CHECK, GET, "/hazelcast/health/cluster-size", "HTTP/1.1 200"),
            new TestUrl(DATA, POST, "/hazelcast/rest/maps/", "HTTP/1.1 400"),
            new TestUrl(DATA, GET, "/hazelcast/rest/maps/", "HTTP/1.1 400"),
            new TestUrl(DATA, DELETE, "/hazelcast/rest/maps/", "HTTP/1.1 200"),
            new TestUrl(DATA, POST, "/hazelcast/rest/queues/", "HTTP/1.1 400"),
            new TestUrl(DATA, GET, "/hazelcast/rest/queues/", "HTTP/1.1 400"),
            new TestUrl(DATA, DELETE, "/hazelcast/rest/queues/", "HTTP/1.1 400"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/1", ""),
            new TestUrl(CLUSTER_WRITE, GET, "/hazelcast/1", ""),
            new TestUrl(CLUSTER_WRITE, DELETE, "/hazelcast/1", ""),
    };

    /**
     * Creates Hazelcast {@link Config} with enabled all but provided {@link RestEndpointGroup RestEndpointGroups}.
     */
    protected Config createConfigWithDisabledGroups(RestEndpointGroup... group) {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableAllGroups().disableGroups(group);
        Config c = new Config();
        c.getNetworkConfig().setRestApiConfig(restApiConfig);
        return c;
    }

    /**
     * Creates Hazelcast {@link Config} with disabled all but provided {@link RestEndpointGroup RestEndpointGroups}.
     */
    protected Config createConfigWithEnabledGroups(RestEndpointGroup... group) {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.disableAllGroups().enableGroups(group);
        Config c = new Config();
        c.getNetworkConfig().setRestApiConfig(restApiConfig);
        return c;
    }

    protected static void assertEmptyString(String stringToCheck) {
        if (stringToCheck != null && !stringToCheck.isEmpty()) {
            fail("Empty string was expected, but got '" + stringToCheck + "'");
        }
    }

    /**
     * Asserts that a text protocol client call to given {@link TestUrl} returns an expected response.
     */
    protected void assertTextProtocolResponse(HazelcastInstance hz, TestUrl testUrl) throws UnknownHostException, IOException {
        try (TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress())) {
            client.connect();
            client.sendData(testUrl.method + " " + testUrl.requestUri + " HTTP/1.0" + CRLF + CRLF);
            assertTrueEventually(
                createResponseAssertTask(testUrl.toString(), client, testUrl.expectedSubstring), 10);
        }
    }

    /**
     * Asserts that a wrong text protocol method call using given {@link TestUrl} closes the connection without returning any
     * response (e.g. a call to a disabled REST API).
     */
    protected void assertNoTextProtocolResponse(HazelcastInstance hz, TestUrl testUrl)
            throws UnknownHostException, InterruptedException, IOException {
        TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData(testUrl.method + " " + testUrl.requestUri + " HTTP/1.0" + CRLF);
            client.waitUntilClosed();
            assertTrue("Connection close was expected (from server side). " + testUrl, client.isConnectionClosed());
            String receivedResponse = client.getReceivedString();
            if (receivedResponse != null && !receivedResponse.isEmpty()) {
                fail("Empty response was expected, but got '" + receivedResponse + "'. " + testUrl);
            }
        } finally {
            client.close();
        }
    }

    static class TestUrl {
        final RestEndpointGroup restEndpointGroup;
        final String method;
        final String requestUri;
        final String expectedSubstring;

        TestUrl(RestEndpointGroup restEndpointGroup, String httpMethod, String requestUri, String expectedSubstring) {
            this.restEndpointGroup = restEndpointGroup;
            this.method = httpMethod;
            this.requestUri = requestUri;
            this.expectedSubstring = expectedSubstring;
        }

        @Override
        public String toString() {
            return "TestUrl [restEndpointGroup=" + restEndpointGroup + ", method=" + method + ", requestUri=" + requestUri
                    + ", expectedSubstring=" + expectedSubstring + "]";
        }
    }

    private static String getLicenseInfoExpectedResponse() {
        return BuildInfoProvider.getBuildInfo().isEnterprise() ? "HTTP/1.1 200" : "HTTP/1.1 404";
    }

}
