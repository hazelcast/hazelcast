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
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GcpDiscoveryStrategyFactoryTest {
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Test
    public void validConfiguration() {
        DiscoveryStrategy strategy = createStrategy("test-gcp-config.xml");
        assertTrue(strategy instanceof GcpDiscoveryStrategy);
    }

    @Test(expected = RuntimeException.class)
    public void invalidConfiguration() {
        createStrategy("test-gcp-config-invalid.xml");
    }

    private static DiscoveryStrategy createStrategy(String xmlFileName) {
        final InputStream xmlResource = GcpDiscoveryStrategyFactoryTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();

        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        DiscoveryServiceSettings settings = new DiscoveryServiceSettings().setDiscoveryConfig(discoveryConfig);
        DefaultDiscoveryService service = new DefaultDiscoveryService(settings);
        Iterator<DiscoveryStrategy> strategies = service.getDiscoveryStrategies().iterator();
        return strategies.next();
    }

    @Test
    public void isEndpointAvailable() {
        // given
        String endpoint = "/some-endpoint";
        String url = String.format("http://localhost:%d%s", wireMockRule.port(), endpoint);
        stubFor(get(urlEqualTo(endpoint)).willReturn(aResponse().withStatus(200).withBody("some-body")));

        // when
        boolean isAvailable = GcpDiscoveryStrategyFactory.isEndpointAvailable(url);

        // then
        assertTrue(isAvailable);
    }

    @Test
    public void readFileContents()
            throws IOException {
        // given
        String expectedContents = "Hello, world!\nThis is a test with Unicode ✓.";
        String testFile = createTestFile(expectedContents);

        // when
        String actualContents = GcpDiscoveryStrategyFactory.readFileContents(testFile);

        // then
        assertEquals(expectedContents, actualContents);
    }

    private static String createTestFile(String expectedContents)
            throws IOException {
        File temp = File.createTempFile("test", ".tmp");
        temp.deleteOnExit();
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(temp), StandardCharsets.UTF_8));
            bufferedWriter.write(expectedContents);
        } finally {
            IOUtil.closeResource(bufferedWriter);
        }
        return temp.getAbsolutePath();
    }
}