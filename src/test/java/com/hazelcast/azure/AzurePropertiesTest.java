package com.hazelcast.azure;


import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.ValidationException;

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
public class AzurePropertiesTest extends HazelcastTestSupport {

    @Test
    public void test_newDiscoveryFactory() throws Exception {

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", "test-value");
        properties.put("client-secret", "test-value");
        properties.put("subscription-id", "test-value");
        properties.put("hzlcst-cluster-id", "test-value");
        properties.put("tenant-id", "test-value");
        properties.put("group-name", "test-value");

        assertTrue("Expected to find AzureProperties.CLIENT_ID", AzureProperties.getOrNull(AzureProperties.CLIENT_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.TENANT_ID", AzureProperties.getOrNull(AzureProperties.TENANT_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.SUBSCRIPTION_ID", AzureProperties.getOrNull(AzureProperties.SUBSCRIPTION_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.CLIENT_SECRET", AzureProperties.getOrNull(AzureProperties.CLIENT_SECRET, properties) != null);
        assertTrue("Expected to find AzureProperties.HZLCST_CLUSTER_ID", AzureProperties.getOrNull(AzureProperties.HZLCST_CLUSTER_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.GROUP_NAME", AzureProperties.getOrNull(AzureProperties.GROUP_NAME, properties) != null);
    }

    @Test(expected = ValidationException.class)
    public void test_PortValueValidator_validate_negative_val() throws Exception {

        AzureProperties.PortValueValidator validator = new AzureProperties.PortValueValidator();

        validator.validate(-1);
    }

    @Test(expected = ValidationException.class)
    public void test_PortValueValidator_validate_too_big() throws Exception {

        AzureProperties.PortValueValidator validator = new AzureProperties.PortValueValidator();

        validator.validate(65536);
    }

    @Test
    public void test_PortValueValidator_validate() throws Exception {

        AzureProperties.PortValueValidator validator = new AzureProperties.PortValueValidator();

        validator.validate(0);
        validator.validate(1000);
        validator.validate(65535);
    }

}
