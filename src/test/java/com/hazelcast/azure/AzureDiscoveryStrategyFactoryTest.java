package com.hazelcast.azure;


import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.config.properties.PropertyDefinition;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.hazelcast.util.StringUtil.stringToBytes;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AzureDiscoveryStrategyFactoryTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void test_newDiscoveryFactory() throws Exception {

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", "test-value");
        properties.put("client-secret", "test-value");
        properties.put("subscription-id", "test-value");
        properties.put("hzlcst-cluster-id", "test-value");
        properties.put("group-name", "test-value");

        AzureDiscoveryStrategyFactory factory = new AzureDiscoveryStrategyFactory();
        factory.newDiscoveryStrategy(null, null, properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_missing_config_value() throws Exception {

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", "test-value");
        properties.put("client-secret", "test-value");
        properties.put("subscription-id", "test-value");
        properties.put("hzlcst-cluster-id", "test-value");
        properties.put("group-name", "test-value");

        AzureDiscoveryStrategyFactory factory = new AzureDiscoveryStrategyFactory();
        // should recognize tenant-id is missing
        factory.newDiscoveryStrategy(null, null, properties);
    }

    @Test
    public void test_getConfigurationProperties() {

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", "test-value");
        properties.put("client-secret", "test-value");
        properties.put("subscription-id", "test-value");
        properties.put("hzlcst-cluster-id", "test-value");
        properties.put("tenant-id", "test-value");
        properties.put("group-name", "test-value");

        AzureDiscoveryStrategyFactory factory = new AzureDiscoveryStrategyFactory();

        for (PropertyDefinition def : factory.getConfigurationProperties()) {
            // test each proprety actually maps to those defined above
            assertTrue(AzureProperties.getOrNull(def, properties) != null);
        }
        
    }

    @Test
    public void test_getDiscoveryStrategyType() {
        AzureDiscoveryStrategyFactory factory = new AzureDiscoveryStrategyFactory();
        assertEquals(factory.getDiscoveryStrategyType(), AzureDiscoveryStrategy.class);
    }
}
