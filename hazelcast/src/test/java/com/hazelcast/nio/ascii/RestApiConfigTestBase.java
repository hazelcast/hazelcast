/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.ascii;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;

import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.Versions;

import static com.hazelcast.config.RestEndpointGroup.CLUSTER_READ;
import static com.hazelcast.config.RestEndpointGroup.CLUSTER_WRITE;
import static com.hazelcast.config.RestEndpointGroup.DATA;
import static com.hazelcast.config.RestEndpointGroup.HEALTH_CHECK;
import static com.hazelcast.config.RestEndpointGroup.HOT_RESTART;
import static com.hazelcast.config.RestEndpointGroup.WAN;

/**
 * Shared code for HTTP REST API and Memcache protocol testing.
 */
public abstract class RestApiConfigTestBase extends AbstractTextProtocolsTestBase {

    public static final String POST = "POST";
    public static final String GET = "GET";
    public static final String DELETE = "DELETE";
    public static final String CRLF = "\r\n";

    protected static final TestUrl[] TEST_URLS = new TestUrl[]{
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/mancenter/changeurl ", "HTTP/1.1 500"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/mancenter/security/permissions", "forbidden"),
            new TestUrl(CLUSTER_READ, GET, "/hazelcast/rest/cluster", "Members {size:1, ver:1} ["),
            new TestUrl(CLUSTER_READ, POST, "/hazelcast/rest/management/cluster/state", "forbidden"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/changeState", "HTTP/1.1 500"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/version", "HTTP/1.1 500"),
            new TestUrl(CLUSTER_READ, GET, "/hazelcast/rest/management/cluster/version", Versions.CURRENT_CLUSTER_VERSION.toString()),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/clusterShutdown", "forbidden"),
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/rest/management/cluster/memberShutdown", "forbidden"),
            new TestUrl(CLUSTER_READ, POST, "/hazelcast/rest/management/cluster/nodes", "forbidden"),
            new TestUrl(CLUSTER_READ, GET, "/hazelcast/rest/license", getLicenseInfoExpectedResponse()),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/forceStart", "forbidden"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/partialStart", "forbidden"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/hotBackup", "forbidden"),
            new TestUrl(HOT_RESTART, POST, "/hazelcast/rest/management/cluster/hotBackupInterrupt", "forbidden"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/sync/map", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/sync/allmaps", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/clearWanQueues", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/addWanConfig", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/pausePublisher", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/stopPublisher", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/resumePublisher", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/wan/consistencyCheck/map", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/sync/map", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/sync/allmaps", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/mancenter/clearWanQueues", "HTTP/1.1 500"),
            new TestUrl(WAN, POST, "/hazelcast/rest/wan/addWanConfig", "HTTP/1.1 500"),
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
            new TestUrl(CLUSTER_WRITE, POST, "/hazelcast/1", "HTTP/1.1 404"),
            new TestUrl(CLUSTER_WRITE, GET, "/hazelcast/1", "HTTP/1.1 404"),
            new TestUrl(CLUSTER_WRITE, DELETE, "/hazelcast/1", "HTTP/1.1 404"),
    };

    /**
     * Creates Hazelcast {@link Config} with enabled all but provided {@link RestEndpointGroup RestEndpointGroups}.
     */
    protected Config createConfigWithDisabledGroups(RestEndpointGroup... group) {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableAllGroups().disableGroups(group);
        return new Config().setRestApiConfig(restApiConfig);
    }

    /**
     * Creates Hazelcast {@link Config} with disabled all but provided {@link RestEndpointGroup RestEndpointGroups}.
     */
    protected Config createConfigWithEnabledGroups(RestEndpointGroup... group) {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.disableAllGroups().enableGroups(group);
        return new Config().setRestApiConfig(restApiConfig);
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
        final TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData(testUrl.method + " " + testUrl.requestUri + " HTTP/1.0" + CRLF + CRLF);
            assertTrueEventually(createResponseAssertTask(testUrl.toString(), client, testUrl.expectedSubstring), 10);
        } finally {
            client.close();
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

        public TestUrl(RestEndpointGroup restEndpointGroup, String httpMethod, String requestUri, String expectedSubstring) {
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
