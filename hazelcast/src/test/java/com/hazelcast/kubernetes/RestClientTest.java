/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.kubernetes;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import java.io.File;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.hazelcast.kubernetes.KubernetesConfig.readFileContents;
import static org.junit.Assert.assertEquals;

public class RestClientTest {
    private static final String API_ENDPOINT = "/some/endpoint";
    private static final String BODY_REQUEST = "some body request";
    private static final String BODY_RESPONSE = "some body response";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig()
            .dynamicHttpsPort()
            .keystorePath(pathTo("kubernetes/keystore.jks"))
    );

    private String address;

    @Before
    public void setUp() {
        // disable hostname HTTPS verification for testing
        HttpsURLConnection.setDefaultHostnameVerifier(
                new HostnameVerifier() {
                    public boolean verify(String hostname, SSLSession sslSession) {
                        return true;
                    }
                });
        address = String.format("https://localhost:%s", wireMockRule.httpsPort());
    }

    @Test
    public void getSuccess() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
                .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                .withCaCertificates(readFile("kubernetes/ca.crt"))
                .get();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test
    public void getWithHeaderSuccess() {
        // given
        String headerKey = "Metadata-Flavor";
        String headerValue = "Google";
        stubFor(get(urlEqualTo(API_ENDPOINT))
                .withHeader(headerKey, equalTo(headerValue))
                .willReturn(aResponse().withStatus(200).withBody(BODY_RESPONSE)));

        // when
        String result = RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                .withHeader(headerKey, headerValue)
                .withCaCertificates(readFile("kubernetes/ca.crt"))
                .get();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    @Test(expected = RestClientException.class)
    public void getFailure() {
        // given
        stubFor(get(urlEqualTo(API_ENDPOINT))
                .willReturn(aResponse().withStatus(500).withBody("Internal error")));

        // when
        RestClient.create(String.format("%s%s", address, API_ENDPOINT))
                .withCaCertificates(readFile("kubernetes/ca.crt"))
                .get();

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
                .withCaCertificates(readFile("kubernetes/ca.crt"))
                .post();

        // then
        assertEquals(BODY_RESPONSE, result);
    }

    private String readFile(String filename) {
        return readFileContents(pathTo(filename));
    }

    private String pathTo(String filename) {
        return new File(getClass().getClassLoader().getResource(filename).getFile()).getAbsolutePath();
    }
}
