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

package com.hazelcast.gcp;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;

public class GcpMetadataApiTest {
    private static final String PROJECT = "project-1";
    private static final String ZONE = "us-east1-b";
    private static final String ACCESS_TOKEN = "ya29.c.Elr6BVAeC2CeahNthgBf6Nn8j66IfIfZV6eb0LTkDeoAzELseUL5pFmfq0K_ViJN8BaeVB6b16NNCiPB0YbWPnoHRC2I1ghmnknUTzL36t-79b_OitEF_q_C1GM";

    private GcpMetadataApi gcpMetadataApi;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        gcpMetadataApi = new GcpMetadataApi(String.format("http://localhost:%s", wireMockRule.port()));
    }

    @Test
    public void currentProject() {
        // given
        stubFor(get(urlEqualTo("/computeMetadata/v1/project/project-id"))
                .withHeader("Metadata-Flavor", equalTo("Google"))
                .willReturn(aResponse().withStatus(200).withBody(PROJECT)));

        // when
        String result = gcpMetadataApi.currentProject();

        // then
        assertEquals(PROJECT, result);
    }

    @Test
    public void currentZone() {
        // given
        stubFor(get(urlEqualTo("/computeMetadata/v1/instance/zone"))
                .withHeader("Metadata-Flavor", equalTo("Google"))
                .willReturn(aResponse().withStatus(200).withBody(zoneResponse(ZONE))));

        // when
        String result = gcpMetadataApi.currentZone();

        // then
        assertEquals(ZONE, result);
    }

    @Test
    public void currentRegion() {
        // given
        stubFor(get(urlEqualTo("/computeMetadata/v1/instance/zone"))
                .withHeader("Metadata-Flavor", equalTo("Google"))
                .willReturn(aResponse().withStatus(200).withBody(zoneResponse(ZONE))));

        // when
        String result = gcpMetadataApi.currentRegion();

        // then
        assertEquals("us-east1", result);
    }

    private static String zoneResponse(String zone) {
        String sampleProjectId = "183928891381";
        return String.format("projects/%s/zones/%s", sampleProjectId, zone);
    }

    @Test
    public void accessToken() {
        // given
        stubFor(get(urlEqualTo("/computeMetadata/v1/instance/service-accounts/default/token"))
                .withHeader("Metadata-Flavor", equalTo("Google"))
                .willReturn(aResponse().withStatus(200).withBody(accessTokenResponse(ACCESS_TOKEN))));

        // when
        String result = gcpMetadataApi.accessToken();

        // then
        assertEquals(ACCESS_TOKEN, result);
    }

    private static String accessTokenResponse(String accessToken) {
        return String.format("{\"access_token\":\"%s\",\"expires_in\":3599,\"token_type\":\"Bearer\"}", accessToken);
    }
}
