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

package com.hazelcast.spi.utils;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.spi.exception.RestClientException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RestClientTest {
    private static final String API_ENDPOINT = "/some/endpoint";
    private static final String BODY_REQUEST = "some body request";
    private static final String BODY_RESPONSE = "some body response";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private String address;

    @Before
    public void setUp() {
        address = String.format("http://localhost:%s", wireMockRule.port());
    }

    @Test
    public void getSuccess() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT)).get().getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void getWithHeadersSuccess() {
        // given
        String headerKey = "Metadata-Flavor";
        String headerValue = "Google";
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .withHeader(headerKey, equalTo(headerValue))
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
            .withHeaders(singletonMap(headerKey, headerValue))
            .get()
            .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void getWithRetries() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .inScenario("Retry Scenario")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(500).withBody("Internal error"))
            .willSetStateTo("Second Try"));
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .inScenario("Retry Scenario")
            .whenScenarioStateIs("Second Try")
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
            .withReadTimeoutSeconds(1200)
            .withConnectTimeoutSeconds(1200)
            .withRetries(1)
            .get()
            .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test(expected = Exception.class)
    public void getFailure() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
            .willReturn(aResponse().withStatus(500).withBody("Internal error")));

        // when
        RestClient.create(String.format("%s%s", address, API_ENDPOINT)).get();

        // then
        // throw exception
    }

    @Test
    public void postSuccess() {
        // given
        stubFor(post(urlEqualTo(API_ENDPOINT))
            .withRequestBody(equalTo(BODY_REQUEST))
            .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
            .withBody(BODY_REQUEST)
            .post()
            .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void expectedResponseCode() {
        // given
        int expectedCode1 = 201;
        int expectedCode2 = 202;
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(expectedCode1).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                .withBody(BODY_REQUEST)
                .expectResponseCodes(expectedCode1, expectedCode2)
                .post()
                .getBody();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void expectHttpOkByDefault() {
        // given
        int responseCode = 201;
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(responseCode).withBody(BODY_RESPONSE)));

        // when
        RestClientException exception = assertThrows(RestClientException.class, () ->
                RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                        .withBody(BODY_REQUEST)
                        .get());

        // then
        assertEquals(exception.getHttpErrorCode(), responseCode);
    }

    @Test
    public void unexpectedResponseCode() {
        // given
        int expectedCode = 201;
        int unexpectedCode = 202;
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(unexpectedCode).withBody(BODY_RESPONSE)));

        // when
        RestClientException exception = assertThrows(RestClientException.class, () ->
                RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                        .withBody(BODY_REQUEST)
                        .expectResponseCodes(expectedCode)
                        .post());

        // then
        assertEquals(exception.getHttpErrorCode(), unexpectedCode);
    }

    @Test
    public void readErrorResponse() {
        // given
        int responseCode = 418;
        String responseMessage = "I'm a teapot";
        stubFor(post(urlEqualTo(API_ENDPOINT))
                .withRequestBody(equalTo(BODY_REQUEST))
                .willReturn(aResponse().withStatus(responseCode).withBody(responseMessage)));

        // when
        RestClient.Response response = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                .withBody(BODY_REQUEST)
                .expectResponseCodes(responseCode)
                .get();

        // then
        assertEquals(responseCode, response.getCode());
        assertEquals(responseMessage, response.getBody());
    }
}
