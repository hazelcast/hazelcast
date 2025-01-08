/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.HttpURLConnection;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_MAPS;
import static com.hazelcast.internal.ascii.HTTPCommunicator.URI_QUEUES;
import static com.hazelcast.internal.util.phonehome.PhoneHomeIntegrationTest.containingParam;
import static com.hazelcast.internal.util.phonehome.TestUtil.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RestApiPhoneHomeIntegrationTest extends HazelcastTestSupport {
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    // HTTPCommunicator fails when the mock network is used (hazelcast.test.use.network=false).
    // That's why HazelcastTestSupport#createHazelcastInstance cannot be used, which calls
    // TestHazelcastInstanceFactory#newHazelcastInstance under the hood.
    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    private HTTPCommunicator http;
    private Config config;
    private PhoneHome phoneHome;

    @Before
    public void setUp() {
        config = smallInstanceConfig();
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);

        HazelcastInstance instance = factory.newHazelcastInstance(config);
        http = new HTTPCommunicator(instance);
        phoneHome = new PhoneHome(getNode(instance), "http://localhost:" + wireMockRule.port() + "/ping");
        stubFor(post(urlPathEqualTo("/ping")).willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK)));
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void mapOperations() throws IOException {
        assertEquals(200, http.mapPut("my-map", "key", "value"));
        assertEquals(200, http.mapPut("my-map", "key2", "value2"));
        assertEquals(400, http.doPost(http.getUrl(URI_MAPS), "value").responseCode);
        assertEquals(204, http.mapGet("my-other-map", "key").responseCode);
        assertEquals(400, http.doGet(http.getUrl(URI_MAPS)).responseCode);
        assertEquals(400, http.doGet(http.getUrl(URI_MAPS)).responseCode);
        assertEquals(200, http.mapDelete("my-map", "key"));

        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("restenabled" /*REST_ENABLED*/, 1))
                .withRequestBody(containingParam("restmaprequestct" /*REST_MAP_TOTAL_REQUEST_COUNT*/, 7))
                .withRequestBody(containingParam("restqueuerequestct" /*REST_QUEUE_TOTAL_REQUEST_COUNT*/, 0))
                .withRequestBody(containingParam("restrequestct" /*REST_REQUEST_COUNT*/, 7))
                .withRequestBody(containingParam("restuniqrequestct" /*REST_UNIQUE_REQUEST_COUNT*/, 6))
                .withRequestBody(containingParam("restmappostsucc" /*REST_MAP_POST_SUCCESS*/, 2))
                .withRequestBody(containingParam("restmappostfail" /*REST_MAP_POST_FAILURE*/, 1))
                .withRequestBody(containingParam("restmapgetsucc" /*REST_MAP_GET_SUCCESS*/, 1))
                .withRequestBody(containingParam("restmapgetfail" /*REST_MAP_GET_FAILURE*/, 2))
                .withRequestBody(containingParam("restmapdeletesucc" /*REST_MAP_DELETE_SUCCESS*/, 1))
                .withRequestBody(containingParam("restmapdeletefail" /*REST_MAP_DELETE_FAILURE*/, 0))
                .withRequestBody(containingParam("restmapct" /*REST_ACCESSED_MAP_COUNT*/, 2))
                .withRequestBody(containingParam("restqueuepostsucc" /*REST_QUEUE_POST_SUCCESS*/, 0))
                .withRequestBody(containingParam("restqueuepostfail" /*REST_QUEUE_POST_FAILURE*/, 0)));
    }

    @Test
    public void queueOperations() throws IOException {
        assertEquals(200, http.queueOffer("my-queue", "a"));
        assertEquals(200, http.queueOffer("my-queue", "b"));
        assertEquals(400, http.doPost(http.getUrl(URI_QUEUES)).responseCode);
        assertEquals(200, http.queuePoll("my-queue", 10).responseCode);
        assertEquals(200, http.queuePoll("my-queue", 10).responseCode);
        assertEquals(204, http.doDelete(http.getUrl(URI_QUEUES) + "my-queue/10").responseCode);
        assertEquals(400, http.doDelete(http.getUrl(URI_QUEUES) + "my-queue").responseCode);

        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("restmappostsucc" /*REST_MAP_POST_SUCCESS*/, 0))
                .withRequestBody(containingParam("restmappostfail" /*REST_MAP_POST_FAILURE*/, 0))
                .withRequestBody(containingParam("restmaprequestct" /*REST_MAP_TOTAL_REQUEST_COUNT*/, 0))
                .withRequestBody(containingParam("restqueuerequestct" /*REST_QUEUE_TOTAL_REQUEST_COUNT*/, 7))
                .withRequestBody(containingParam("restrequestct" /*REST_REQUEST_COUNT*/, 7))
                .withRequestBody(containingParam("restqueuepostsucc" /*REST_QUEUE_POST_SUCCESS*/, 2))
                .withRequestBody(containingParam("restqueuepostfail" /*REST_QUEUE_POST_FAILURE*/, 1))
                .withRequestBody(containingParam("restqueuedeletesucc" /*REST_QUEUE_DELETE_SUCCESS*/, 1))
                .withRequestBody(containingParam("restqueuedeletefail" /*REST_QUEUE_DELETE_FAILURE*/, 1))
                .withRequestBody(containingParam("restqueuegetsucc" /*REST_QUEUE_GET_SUCCESS*/, 2))
                .withRequestBody(containingParam("restqueuect" /*REST_ACCESSED_QUEUE_COUNT*/, 1)));
    }

    @Test
    public void configUpdateOperations() throws IOException {
        http.configReload(config.getClusterName(), "");
        http.configUpdate(config.getClusterName(), "", "hazelcast:\n");

        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("restconfigreloadsucc" /*REST_CONFIG_RELOAD_SUCCESS*/, 0))
                .withRequestBody(containingParam("restconfigreloadfail" /*REST_CONFIG_RELOAD_FAILURE*/, 1))
                .withRequestBody(containingParam("restconfigupdatesucc" /*REST_CONFIG_UPDATE_SUCCESS*/, 0))
                .withRequestBody(containingParam("restconfigupdatefail" /*REST_CONFIG_UPDATE_FAILURE*/, 1)));
    }
}
