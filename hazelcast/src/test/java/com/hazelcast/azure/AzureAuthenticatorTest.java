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

package com.hazelcast.azure;

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

public class AzureAuthenticatorTest {
    private static final String TENANT_ID = "test-tenant-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final String CLIENT_SECRET = "test-client-secret";
    private static final String START_OF_REQUEST_BODY = "grant_type=client_credentials&resource=https://management.azure.com&client_id=" + CLIENT_ID + "&client_secret=" + CLIENT_SECRET;
    private static final String ACCESS_TOKEN = "test-access-token";

    private AzureAuthenticator azureAuthenticator;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        azureAuthenticator = new AzureAuthenticator(String.format("http://localhost:%s", wireMockRule.port()));
    }

    @Test
    public void refreshAccessToken() {
        // given
        stubFor(post("/" + TENANT_ID + "/oauth2/token")
                .withRequestBody(matching(START_OF_REQUEST_BODY + ".*"))
                .willReturn(aResponse().withStatus(200).withBody(responseBody(ACCESS_TOKEN))));

        // when
        String result = azureAuthenticator.refreshAccessToken(TENANT_ID, CLIENT_ID, CLIENT_SECRET);

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
