package com.hazelcast.internal.util.phonehome;

import com.github.tomakehurst.wiremock.extension.Extension;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.Test;
import wiremock.org.apache.http.HttpResponse;
import wiremock.org.apache.http.client.methods.CloseableHttpResponse;
import wiremock.org.apache.http.client.methods.HttpGet;
import wiremock.org.apache.http.impl.client.CloseableHttpClient;
import wiremock.org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

public class IntegrationTest extends HazelcastTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule();

    @Test
    public void test() throws IOException {


        stubFor(get(urlPathMatching("/ping/.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("content")));


        verify(1,getRequestedFor(urlPathMatching("/ping/.*")));

    }

    private String convertHttpResponseToString(HttpResponse httpResponse) throws IOException {
        InputStream inputStream = httpResponse.getEntity().getContent();
        return convertInputStreamToString(inputStream);
    }

    private String convertInputStreamToString(InputStream inputStream) {
        Scanner scanner = new Scanner(inputStream, "UTF-8");
        String string = scanner.useDelimiter("\\Z").next();
        scanner.close();
        return string;
    }

}

