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

package com.hazelcast.aws;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class AwsDiscoveryStrategyFactoryTest {

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
        final Config config = createConfig("test-aws-config.xml");
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
}
