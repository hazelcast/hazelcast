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
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AwsDiscoveryStrategyFactoryTest
        extends HazelcastTestSupport {

    private static DiscoveryStrategy createStrategy(Map<String, Comparable> props) {
        final AwsDiscoveryStrategyFactory factory = new AwsDiscoveryStrategyFactory();
        return factory.newDiscoveryStrategy(null, null, props);
    }

    private static Config createConfig(String xmlFileName) {
        final InputStream xmlResource = AwsDiscoveryStrategyFactoryTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        return new XmlConfigBuilder(xmlResource).build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void hostHeaderMalformed()
            throws Exception {
        final Map<String, Comparable> props = new HashMap<String, Comparable>();
        props.put("access-key", "test-value");
        props.put("secret-key", "test-value");
        props.put("host-header", "test-value");
        props.put("region", "us-east-1");
        createStrategy(props);
    }

    @Test
    public void testMinimalOk()
            throws Exception {
        final Map<String, Comparable> props = new HashMap<String, Comparable>();
        props.put("region", "us-east-1");
        createStrategy(props);
    }

    @Test
    public void testOnlyGivenRoleOk() {
        final Map<String, Comparable> props = new HashMap<String, Comparable>();
        props.put("iam-role", "test-value");
        props.put("region", "us-east-1");
        createStrategy(props);
    }

    @Test
    public void testOnlyAccessAndSecretKeyOk() {
        final Map<String, Comparable> props = new HashMap<String, Comparable>();
        props.put("secret-key", "test-value");
        props.put("access-key", "test-value");
        props.put("region", "us-east-1");
        createStrategy(props);
    }

    @Test
    public void testFull()
            throws Exception {
        final Map<String, Comparable> props = new HashMap<String, Comparable>();
        props.put("access-key", "test-value");
        props.put("secret-key", "test-value");
        props.put("region", "us-east-1");
        props.put("iam-role", "test-value");
        props.put("host-header", "ec2.test-value");
        props.put("security-group-name", "test-value");
        props.put("tag-key", "test-value");
        props.put("tag-value", "test-value");
        props.put("connection-timeout-seconds", 10);
        props.put("hz-port", 1234);
        createStrategy(props);
    }

    @Test
    public void parseAndCreateDiscoveryStrategyPasses() {
        final Config config = createConfig("test-aws-config.xml");
        validateConfig(config);
    }

    @Test
    public void parseMissingCredentialsConfigPasses() {
        final Config config = createConfig("missing-creds-aws-config.xml");
        validateConfig(config);
    }

    private void validateConfig(final Config config) {
        final DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        final DiscoveryServiceSettings settings = new DiscoveryServiceSettings().setDiscoveryConfig(discoveryConfig);
        final DefaultDiscoveryService service = new DefaultDiscoveryService(settings);
        final Iterator<DiscoveryStrategy> strategies = service.getDiscoveryStrategies().iterator();

        assertTrue(strategies.hasNext());
        final DiscoveryStrategy strategy = strategies.next();
        assertTrue(strategy != null && strategy instanceof AwsDiscoveryStrategy);
    }

    @Test
    public void parseDiscoveryStrategyConfigPasses() {
        final AwsDiscoveryStrategyFactory factory = new AwsDiscoveryStrategyFactory();
        final Config config = createConfig("test-aws-config.xml");
        final JoinConfig joinConfig = config.getNetworkConfig().getJoin();

        assertFalse(joinConfig.getAwsConfig().isEnabled());
        assertFalse(joinConfig.getTcpIpConfig().isEnabled());
        assertFalse(joinConfig.getMulticastConfig().isEnabled());

        final DiscoveryConfig discoveryConfig = joinConfig.getDiscoveryConfig();

        assertTrue(discoveryConfig.isEnabled());
        assertEquals(1, discoveryConfig.getDiscoveryStrategyConfigs().size());

        final DiscoveryStrategyConfig providerConfig = discoveryConfig.getDiscoveryStrategyConfigs().iterator().next();
        final Map<String, Comparable> providerProperties = providerConfig.getProperties();
        final Collection<PropertyDefinition> factoryConfigProperties = factory.getConfigurationProperties();

        assertEquals(factory.getDiscoveryStrategyType(), AwsDiscoveryStrategy.class);
        assertEquals(AwsDiscoveryStrategy.class.getName(), providerConfig.getClassName());
        assertEquals(factoryConfigProperties.size(), providerProperties.size());
        for (AwsProperties prop : AwsProperties.values()) {
            assertTrue(factoryConfigProperties.contains(prop.getDefinition()));
        }

        assertEquals("test-access-key", providerProperties.get("access-key"));
        assertEquals("test-secret-key", providerProperties.get("secret-key"));
        assertEquals("us-east-1", providerProperties.get("region"));
        assertEquals("test-iam-role", providerProperties.get("iam-role"));
        assertEquals("ec2.test-host-header", providerProperties.get("host-header"));
        assertEquals("test-security-group-name", providerProperties.get("security-group-name"));
        assertEquals("test-tag-key", providerProperties.get("tag-key"));
        assertEquals("test-tag-value", providerProperties.get("tag-value"));
        assertEquals("10", providerProperties.get("connection-timeout-seconds"));
        assertEquals("5702", providerProperties.get("hz-port"));
    }
}
