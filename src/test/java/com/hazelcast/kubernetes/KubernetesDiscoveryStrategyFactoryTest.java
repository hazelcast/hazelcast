package com.hazelcast.kubernetes;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ServiceEndpointResolver.class)
public class KubernetesDiscoveryStrategyFactoryTest {

    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final String API_TOKEN = "token";

    @Mock
    DiscoveryNode discoveryNode;

    @Mock
    private DefaultKubernetesClient client;

    @Before
    public void setup() throws Exception {
        PowerMockito.whenNew(DefaultKubernetesClient.class).withAnyArguments().thenReturn(client);
    }

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
    }

    @Test
    public void createDiscoveryStrategy() {
        HashMap<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put(KubernetesProperties.KUBERNETES_API_TOKEN.key(), API_TOKEN);
        HazelcastKubernetesDiscoveryStrategyFactory factory = new HazelcastKubernetesDiscoveryStrategyFactory();
        DiscoveryStrategy strategy   = factory.newDiscoveryStrategy(discoveryNode, LOGGER, properties);
        assertTrue(strategy instanceof  HazelcastKubernetesDiscoveryStrategy);
        strategy.start();
        strategy.destroy();
    }
}
