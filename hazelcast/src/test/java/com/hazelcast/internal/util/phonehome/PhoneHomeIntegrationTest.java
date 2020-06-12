/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastTestSupport;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.hazelcast.test.Accessors.getNode;

public class PhoneHomeIntegrationTest extends HazelcastTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule();

    @Test()
    public void testMapMetrics() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        PhoneHome phoneHome = new PhoneHome(node, "http://localhost:8080/ping");
        Map<String, String> map1 = hz.getMap("hazelcast");
        Map<String, String> map2 = hz.getMap("phonehome");
        map2.put("1", "hazelcast");
        node.getConfig().getMapConfig("hazelcast").setReadBackupData(true);
        node.getConfig().getMapConfig("phonehome").getMapStoreConfig().setEnabled(true);
        node.getConfig().getMapConfig("hazelcast").addQueryCacheConfig(new QueryCacheConfig("queryconfig"));
        node.getConfig().getMapConfig("hazelcast").getHotRestartConfig().setEnabled(true);


        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)));

        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping")).withQueryParam("mpct", equalTo("2"))
                .withQueryParam("mpbrct", equalTo("1"))
                .withQueryParam("mpmsct", equalTo("1"))
                .withQueryParam("mpaoqcct", equalTo("1"))
                .withQueryParam("mpaoict", equalTo("1"))
                .withQueryParam("mphect", equalTo("1")));

    }

}

