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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.config.Config;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

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
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RESTClientPhoneHomeTest {

    protected final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private HazelcastInstance instance;

    private HTTPCommunicator http;

    private Config config;
    private int port;

    @Before
    public void setUp() {
        config = createConfigWithRestEnabled();
        instance = factory.newHazelcastInstance(config);
        http = new HTTPCommunicator(instance);
        stubFor(post(urlPathEqualTo("/ping")).willReturn(aResponse().withStatus(200)));
        port = wireMockRule.port();
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

    @Test
    public void mapOperations()
            throws IOException {
        assertEquals(200, http.mapPut("my-map", "key", "value"));
        assertEquals(200, http.mapPut("my-map", "key2", "value2"));
        assertEquals(400, http.doPost(http.getUrl(URI_MAPS), "value").responseCode);
        assertEquals(204, http.mapGet("my-other-map", "key").responseCode);
        assertEquals(400, http.doGet(http.getUrl(URI_MAPS)).responseCode);
        assertEquals(400, http.doGet(http.getUrl(URI_MAPS)).responseCode);
        assertEquals(200, http.mapDelete("my-map", "key"));

        PhoneHome phoneHome = new PhoneHome(getNode(instance), "http://localhost:" + port + "/ping");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("restenabled", "1"))
                .withRequestBody(containingParam("restmaprequestct", "7"))
                .withRequestBody(containingParam("restqueuerequestct", "0"))
                .withRequestBody(containingParam("restrequestct", "7"))
                .withRequestBody(containingParam("restuniqrequestct", "6"))
                .withRequestBody(containingParam("restmappostsucc", "2"))
                .withRequestBody(containingParam("restmappostfail", "1"))
                .withRequestBody(containingParam("restmapgetsucc", "1"))
                .withRequestBody(containingParam("restmapgetfail", "2"))
                .withRequestBody(containingParam("restmapdeletesucc", "1"))
                .withRequestBody(containingParam("restmapdeletefail", "0"))
                .withRequestBody(containingParam("restmapct", "2"))
                .withRequestBody(containingParam("restqueuepostsucc", "0"))
                .withRequestBody(containingParam("restqueuepostfail", "0"))
        );
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

        PhoneHome phoneHome = new PhoneHome(getNode(instance), "http://localhost:" + port + "/ping");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("restmappostsucc", "0"))
                .withRequestBody(containingParam("restmappostfail", "0"))
                .withRequestBody(containingParam("restmaprequestct", "0"))
                .withRequestBody(containingParam("restqueuerequestct", "7"))
                .withRequestBody(containingParam("restrequestct", "7"))
                .withRequestBody(containingParam("restqueuepostsucc", "2"))
                .withRequestBody(containingParam("restqueuepostfail", "1"))
                .withRequestBody(containingParam("restqueuedeletesucc", "1"))
                .withRequestBody(containingParam("restqueuedeletefail", "1"))
                .withRequestBody(containingParam("restqueuegetsucc", "2"))
                .withRequestBody(containingParam("restqueuect", "1"))
        );
    }

    @Test
    public void configUpdateOperations() throws IOException {
        http.configReload(config.getClusterName(), "");
        http.configUpdate(config.getClusterName(), "", "hazelcast:\n");

        PhoneHome phoneHome = new PhoneHome(getNode(instance), "http://localhost:" + port + "/ping");
        phoneHome.phoneHome(false);

        verify(1, postRequestedFor(urlPathEqualTo("/ping"))
                .withRequestBody(containingParam("restconfigreloadsucc", "0"))
                .withRequestBody(containingParam("restconfigreloadfail", "1"))
                .withRequestBody(containingParam("restconfigupdatesucc", "0"))
                .withRequestBody(containingParam("restconfigupdatefail", "1"))
        );
    }

}
