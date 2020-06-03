package com.hazelcast.internal.util.phonehome;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastTestSupport;

import org.junit.Rule;

import org.junit.Test;

import java.io.IOException;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.hazelcast.test.Accessors.getNode;

public class IntegrationTest extends HazelcastTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule();

    @Test()
    public void test() throws IOException {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        PhoneHome phoneHome = new PhoneHome(node, "http://localhost:8080/ping");

        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("content")));

        Map<String, String> parameters = phoneHome.phoneHome(false);

        parameters.forEach((k, v) -> verify(1, getRequestedFor(urlPathEqualTo("/ping")).withQueryParam(k, equalTo(v))));

    }
}

