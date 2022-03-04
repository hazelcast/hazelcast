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

package com.hazelcast.kubernetes;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory.DiscoveryStrategyLevel;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastKubernetesDiscoveryStrategyFactoryTest {

    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final String API_TOKEN = "token";
    private static final String CA_CERTIFICATE = "ca-certificate";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void checkDiscoveryStrategyType() {
        HazelcastKubernetesDiscoveryStrategyFactory factory = new HazelcastKubernetesDiscoveryStrategyFactory();
        Class<? extends DiscoveryStrategy> strategyType = factory.getDiscoveryStrategyType();
        assertEquals(HazelcastKubernetesDiscoveryStrategy.class.getCanonicalName(), strategyType.getCanonicalName());
    }

    @Test
    public void checkProperties() {
        HazelcastKubernetesDiscoveryStrategyFactory factory = new HazelcastKubernetesDiscoveryStrategyFactory();
        Collection<PropertyDefinition> propertyDefinitions = factory.getConfigurationProperties();
        assertTrue(propertyDefinitions.contains(KubernetesProperties.SERVICE_DNS));
        assertTrue(propertyDefinitions.contains(KubernetesProperties.SERVICE_PORT));
    }

    @Test
    public void createDiscoveryStrategy() {
        HashMap<String, Comparable> properties = new HashMap<>();
        properties.put(KubernetesProperties.KUBERNETES_API_TOKEN.key(), API_TOKEN);
        properties.put(KubernetesProperties.KUBERNETES_CA_CERTIFICATE.key(), CA_CERTIFICATE);
        properties.put(String.valueOf(KubernetesProperties.SERVICE_PORT), 333);
        properties.put(KubernetesProperties.NAMESPACE.key(), "default");
        HazelcastKubernetesDiscoveryStrategyFactory factory = new HazelcastKubernetesDiscoveryStrategyFactory();
        DiscoveryStrategy strategy = factory.newDiscoveryStrategy(mock(DiscoveryNode.class), LOGGER, properties);
        assertTrue(strategy instanceof HazelcastKubernetesDiscoveryStrategy);
        strategy.start();
        strategy.destroy();
    }

    @Test
    public void autoDetection() throws Exception {
        // given
        String token = tempFolder.newFile("some-token").getAbsolutePath();
        HazelcastKubernetesDiscoveryStrategyFactory factory = new HazelcastKubernetesDiscoveryStrategyFactory(token);

        // when & then
        assertTrue(factory.tokenFileExists());
        assertEquals(DiscoveryStrategyLevel.PLATFORM, factory.discoveryStrategyLevel());
    }
}
