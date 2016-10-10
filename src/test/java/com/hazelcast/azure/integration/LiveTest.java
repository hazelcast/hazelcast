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

import com.hazelcast.azure.AzureAuthHelper;

import com.microsoft.windowsazure.Configuration;

import com.hazelcast.azure.AzureDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.HazelcastTestSupport;

import com.microsoft.azure.utility.ComputeHelper;
import com.microsoft.azure.utility.ResourceHelper;
import com.microsoft.azure.utility.ResourceContext;

import com.microsoft.azure.management.resources.DeploymentOperations;
import com.microsoft.azure.management.resources.models.DeploymentMode;
import com.microsoft.azure.management.resources.models.DeploymentExtended;
import com.microsoft.azure.management.resources.ResourceManagementClient;
import com.microsoft.azure.management.resources.ResourceManagementService;
import com.microsoft.azure.management.resources.ResourceGroupOperations;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.Before;
import org.junit.After;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
// This test will be used for manual integration test.
// It deploys machine to azure then checks metadata using AzureDiscoveryStrategy
public class LiveTest extends HazelcastTestSupport {

    public static final String CLIENT_ID   =  System.getProperty("test.azure.client-id");
    public static final String CLIENT_SECRET =  System.getProperty("test.azure.client-secret");
    public static final String TENANT_ID =  System.getProperty("test.azure.tenant-id");
    public static final String SUBSCRIPTION_ID =  System.getProperty("test.azure.subscription-id");
    public static final String GROUP_NAME = System.getProperty("test.azure.group-name");
    public static final String CLUSTER_ID = System.getProperty("test.azure.cluster-id");

    protected Map<String, Comparable> getProperties() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", CLIENT_ID);
        properties.put("client-secret", CLIENT_SECRET);
        properties.put("tenant-id", TENANT_ID);
        properties.put("subscription-id", SUBSCRIPTION_ID);
        properties.put("cluster-id", CLUSTER_ID);
        properties.put("group-name", GROUP_NAME);

        return properties;
    }

    protected static String generateRandomName(String prefix) {
        return "hzlcst-azure" + prefix + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    @Before
    public void deployVirtualMachines() throws Exception {
      String resourceGroupName = GROUP_NAME;
      String resourceGroupLocation = "westus";
      String deploymentName = generateRandomName("deployment");

      Map<String, String> parameters = new HashMap<String, String>();
      parameters.put("newStorageAccountName",
              UUID.randomUUID().toString().replace("-", "").substring(0, 20));
      parameters.put("location", "West US");
      parameters.put("adminUsername", "userName");
      parameters.put("adminPassword", "Password@123");
      parameters.put("dnsNameForPublicIP", generateRandomName("vm"));

      Configuration config = AzureAuthHelper.getAzureConfiguration(getProperties());

      ResourceManagementClient client = ResourceManagementService.create(config);

      ResourceContext resourceContext = new ResourceContext(
                    resourceGroupLocation, resourceGroupName,
                    SUBSCRIPTION_ID, false);
      ComputeHelper.createOrUpdateResourceGroup(client, resourceContext);

      DeploymentExtended deployment = ResourceHelper.createTemplateDeploymentFromURI(
                    client,
                    resourceGroupName,
                    DeploymentMode.Incremental,
                    deploymentName,
                    "https://raw.githubusercontent.com/sedouard/hazelcast-azure/master/src/test/java/com/hazelcast/azure/integration/azuredeploy.json",
                    "1.0.0.0",
                    parameters);

      DeploymentOperations deployOps = client.getDeploymentsOperations();

      // wait for deployment to complete
      while (true) {
        DeploymentExtended extended = deployOps.get(resourceGroupName, deploymentName).getDeployment();
        String provisioningState = extended.getProperties().getProvisioningState();

        if (provisioningState.equals("Succeeded")) {
            break;
        }

        if (provisioningState.equals("Failed")) {
            throw new Exception("Azure provisioning failed");
        }
      }
    }

    @After
    public void cleanupVirtualMachines () throws Exception {
      Configuration config = AzureAuthHelper.getAzureConfiguration(getProperties());
      String resourceGroupName = GROUP_NAME;
      ResourceManagementClient client = ResourceManagementService.create(config);
      ResourceGroupOperations rgOps = client.getResourceGroupsOperations();
      rgOps.delete(resourceGroupName);
    }

    @Test
    public void testDiscoveryStrategyDiscoverNodesLive() throws Exception {
        Map<String, Comparable> properties = getProperties();
        properties.put("group", GROUP_NAME);
        AzureDiscoveryStrategy strategy = new AzureDiscoveryStrategy(properties);
        strategy.start();

        Iterator<DiscoveryNode> nodes = strategy.discoverNodes().iterator();
        
        assertTrue(nodes != null);

        int count = 0;
        String ipBase = "10.0.1.10";
        while(nodes.hasNext()) {
            
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
