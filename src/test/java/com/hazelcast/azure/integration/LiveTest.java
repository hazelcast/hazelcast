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

package com.hazelcast.azure.integration;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.azure.AzureClientHelper;
import com.hazelcast.azure.AzureDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.test.HazelcastTestSupport;
import com.microsoft.azure.management.compute.implementation.ComputeManager;
import com.microsoft.azure.management.resources.Deployment;
import com.microsoft.azure.management.resources.DeploymentMode;
import com.microsoft.azure.management.resources.Deployments;
import com.microsoft.azure.management.resources.ResourceGroups;
import com.microsoft.azure.management.resources.implementation.ResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Ignore
// This test will be used for manual integration test.
// It deploys machine to azure then checks metadata using AzureDiscoveryStrategy
public class LiveTest extends HazelcastTestSupport {

    private static final String CLIENT_ID = System.getProperty("test.azure.client-id");
    private static final String CLIENT_SECRET = System.getProperty("test.azure.client-secret");
    private static final String TENANT_ID = System.getProperty("test.azure.tenant-id");
    private static final String SUBSCRIPTION_ID = System.getProperty("test.azure.subscription-id");
    private static final String GROUP_NAME = System.getProperty("test.azure.group-name");
    private static final String CLUSTER_ID = System.getProperty("test.azure.cluster-id");

    private Map<String, Comparable> getProperties() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", CLIENT_ID);
        properties.put("client-secret", CLIENT_SECRET);
        properties.put("tenant-id", TENANT_ID);
        properties.put("subscription-id", SUBSCRIPTION_ID);
        properties.put("cluster-id", CLUSTER_ID);
        properties.put("group-name", GROUP_NAME);

        return properties;
    }

    private static String generateRandomName(String prefix) {
        return "hzlcst-azure" + prefix + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    @Before
    public void deployVirtualMachines() throws Exception {
        String resourceGroupLocation = "westus";
        String deploymentName = generateRandomName("deployment");

        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("newStorageAccountName",
                ImmutableMap.of("value", UUID.randomUUID().toString().replace("-", "").substring(0, 20)));
        parameters.put("location", ImmutableMap.of("value", "West US"));
        parameters.put("adminUsername", ImmutableMap.of("value", "username"));
        parameters.put("adminPassword", ImmutableMap.of("value", "Password@123"));
        parameters.put("dnsNameForPublicIP", ImmutableMap.of("value", generateRandomName("vm")));
        parameters.put("ubuntuOSVersion", ImmutableMap.of("value", "14.04.2-LTS"));


        ComputeManager computeManager = AzureClientHelper.getComputeManager(getProperties());

        ResourceManager resourceManager = computeManager.resourceManager();
        ResourceGroups resourceGroups = resourceManager.resourceGroups();
        resourceGroups.define(GROUP_NAME)
                .withRegion(resourceGroupLocation)
                .create();


        Deployments deployments = resourceManager.deployments();
        Deployment deployment = deployments.define(deploymentName)
                .withExistingResourceGroup(GROUP_NAME)
                .withTemplateLink("https://raw.githubusercontent.com/sedouard/hazelcast-azure/master/src/test/java/com/hazelcast/azure/integration/azuredeploy.json", "1.0.0.0")
                .withParameters(parameters)
                .withMode(DeploymentMode.INCREMENTAL)
                .beginCreate();
        while (true) {
            deployment = deployments.getById(deployment.id());
            if (deployment.provisioningState().equals("Succeeded")) {
                break;
            }

            if (deployment.provisioningState().equals("Failed")) {
                throw new Exception("Azure provisioning failed");
            }
        }
    }

    @After
    public void cleanupVirtualMachines() {
        ComputeManager computeManager = AzureClientHelper.getComputeManager(getProperties());

        ResourceManager resourceManager = computeManager.resourceManager();
        ResourceGroups resourceGroups = resourceManager.resourceGroups();
        resourceGroups.deleteByName(GROUP_NAME);
    }

    @Test
    public void testDiscoveryStrategyDiscoverNodesLive() {
        Map<String, Comparable> properties = getProperties();
        properties.put("group", GROUP_NAME);
        AzureDiscoveryStrategy strategy = new AzureDiscoveryStrategy(properties);
        strategy.start();

        Iterator<DiscoveryNode> nodes = strategy.discoverNodes().iterator();

        assertNotNull(nodes);

        int count = 0;
        String ipBase = "10.0.1.10";
        while (nodes.hasNext()) {

            DiscoveryNode node = nodes.next();

            // first node in the test template has a public ip address
            if (count == 0) {
                assertTrue(!node.getPrivateAddress().getHost().equals(node.getPublicAddress().getHost()));
            }

            String ip = ipBase + count;

            assertEquals(ip, node.getPrivateAddress().getHost());
            assertEquals(5701, node.getPrivateAddress().getPort());
            assertEquals(5701, node.getPublicAddress().getPort());
            count++;
        }

        assertEquals(3, count);
    }
}
