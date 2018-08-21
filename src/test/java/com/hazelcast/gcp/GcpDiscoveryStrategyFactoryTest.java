/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.gcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import org.junit.Test;

import java.io.InputStream;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;

public class GcpDiscoveryStrategyFactoryTest {

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
}