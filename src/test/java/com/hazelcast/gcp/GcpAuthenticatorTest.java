/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.gcp;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;

public class GcpAuthenticatorTest {
    private static final String PRIVATE_KEY_PATH = "src/test/resources/test-private-key.json";
    private static final long CURRENT_TIME_MS = 1328550785000L;
    // START_OF_REQUEST_BODY taken the Google's example: https://developers.google.com/identity/protocols/OAuth2ServiceAccount
    private static final String START_OF_REQUEST_BODY = "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI3NjEzMjY3OTgwNjktcjVtbGpsbG4xcmQ0bHJiaGc3NWVmZ2lncDM2bTc4ajVAZGV2ZWxvcGVyLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJzY29wZSI6Imh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL2F1dGgv";
    private static final String ACCESS_TOKEN = "ya29.c.EloCBmCSNRWOaprh2z8TPVd9V51_YwQ-CzqafGDi7TNUwfNNOoDx7T6Pv1pOnO2SaNz-d61KKE2FLA-sxb4alMuucHTdfFHRhRqto9O_MKbQrnqOuJSrpnGf_EE";

    private GcpAuthenticator gcpAuthenticator;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        gcpAuthenticator = new GcpAuthenticator(String.format("http://localhost:%s", wireMockRule.port()));
    }

    @Test
    public void refreshAccessToken() {
        // given
        stubFor(post("/")
                .withRequestBody(matching(START_OF_REQUEST_BODY + ".*"))
                .willReturn(aResponse().withStatus(200).withBody(responseBody(ACCESS_TOKEN))));

        // when
        String result = gcpAuthenticator.refreshAccessToken(PRIVATE_KEY_PATH, CURRENT_TIME_MS);

        // then
        assertEquals(ACCESS_TOKEN, result);

    }

    private static String responseBody(String accessToken) {
        return String.format("{\n"
                + "  \"access_token\" : \"%s\",\n"
                + "  \"token_type\" : \"Bearer\",\n"
                + "  \"expires_in\" : 3600\n"
                + "}", accessToken);
    }
}