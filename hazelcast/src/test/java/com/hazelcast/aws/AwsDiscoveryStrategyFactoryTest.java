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

package com.hazelcast.aws;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AwsDiscoveryStrategyFactoryTest {
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    private static void createStrategy(Map<String, Comparable> props) {
        final AwsDiscoveryStrategyFactory factory = new AwsDiscoveryStrategyFactory();
        factory.newDiscoveryStrategy(null, null, props);
    }

    private static Config createConfig(String xmlFileName) {
        final InputStream xmlResource = AwsDiscoveryStrategyFactoryTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        return new XmlConfigBuilder(xmlResource).build();
    }

    @Test
    public void testEc2() {
        final Map<String, Comparable> props = new HashMap<>();
        props.put("access-key", "test-value");
        props.put("secret-key", "test-value");
        props.put("region", "us-east-1");
        props.put("host-header", "ec2.test-value");
        props.put("security-group-name", "test-value");
        props.put("tag-key", "test-value");
        props.put("tag-value", "test-value");
        props.put("connection-timeout-seconds", 10);
        props.put("hz-port", 1234);
        createStrategy(props);
    }

    @Test
    public void testEcs() {
        final Map<String, Comparable> props = new HashMap<>();
        props.put("access-key", "test-value");
        props.put("secret-key", "test-value");
        props.put("region", "us-east-1");
        props.put("host-header", "ecs.test-value");
        props.put("connection-timeout-seconds", 10);
        props.put("hz-port", 1234);
        props.put("cluster", "cluster-name");
        props.put("service-name", "service-name");
        createStrategy(props);
    }

    @Test
    public void parseAndCreateDiscoveryStrategyPasses() {
        final Config config = createConfig("aws/test-aws-config.xml");
        validateConfig(config);
    }

    private void validateConfig(final Config config) {
        final DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        final DiscoveryServiceSettings settings = new DiscoveryServiceSettings().setDiscoveryConfig(discoveryConfig);
        final DefaultDiscoveryService service = new DefaultDiscoveryService(settings);
        final Iterator<DiscoveryStrategy> strategies = service.getDiscoveryStrategies().iterator();

        assertTrue(strategies.hasNext());
        final DiscoveryStrategy strategy = strategies.next();
        assertTrue(strategy instanceof AwsDiscoveryStrategy);
    }

    @Test
    public void isEndpointAvailable() {
        // given
        String endpoint = "/some-endpoint";
        String url = String.format("http://localhost:%d%s", wireMockRule.port(), endpoint);
        stubFor(get(urlEqualTo(endpoint)).willReturn(aResponse().withStatus(200).withBody("some-body")));

        // when
        boolean isAvailable = AwsDiscoveryStrategyFactory.isEndpointAvailable(url);

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
        String actualContents = AwsDiscoveryStrategyFactory.readFileContents(testFile);

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
