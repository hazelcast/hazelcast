/*
 * Copyright (c) 2016, Microsoft Corporation. All Rights Reserved.
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
package com.hazelcast.azure.test;

import com.hazelcast.azure.AzureProperties;

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
    public void testNewDiscoveryFactory() throws Exception {

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", "test-value");
        properties.put("client-secret", "test-value");
        properties.put("subscription-id", "test-value");
        properties.put("cluster-id", "test-value");
        properties.put("tenant-id", "test-value");
        properties.put("group-name", "test-value");

        assertTrue("Expected to find AzureProperties.CLIENT_ID", AzureProperties.getOrNull(AzureProperties.CLIENT_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.TENANT_ID", AzureProperties.getOrNull(AzureProperties.TENANT_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.SUBSCRIPTION_ID", AzureProperties.getOrNull(AzureProperties.SUBSCRIPTION_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.CLIENT_SECRET", AzureProperties.getOrNull(AzureProperties.CLIENT_SECRET, properties) != null);
        assertTrue("Expected to find AzureProperties.CLUSTER_ID", AzureProperties.getOrNull(AzureProperties.CLUSTER_ID, properties) != null);
        assertTrue("Expected to find AzureProperties.GROUP_NAME", AzureProperties.getOrNull(AzureProperties.GROUP_NAME, properties) != null);
    }

    @Test(expected = ValidationException.class)
    public void testPortValueValidator_validate_negative_val() throws Exception {

        AzureProperties.PortValueValidator validator = new AzureProperties.PortValueValidator();

        validator.validate(-1);
    }

    @Test(expected = ValidationException.class)
    public void testPortValueValidatorValidateTooBig() throws Exception {

        AzureProperties.PortValueValidator validator = new AzureProperties.PortValueValidator();

        validator.validate(65536);
    }

    @Test
    public void testPortValueValidatorValidate() throws Exception {

        AzureProperties.PortValueValidator validator = new AzureProperties.PortValueValidator();

        validator.validate(0);
        validator.validate(1000);
        validator.validate(65535);
    }

}
