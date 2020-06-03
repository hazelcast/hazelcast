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
        Map<String, String> map1 = hz.getMap("hazelcast");
        Map<String, String> map2 = hz.getMap("phonehome");

        stubFor(get(urlPathEqualTo("/ping"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("content")));

        phoneHome.phoneHome(false);

        verify(1, getRequestedFor(urlPathEqualTo("/ping")).withQueryParam("mpct", equalTo("2")));

    }
}

