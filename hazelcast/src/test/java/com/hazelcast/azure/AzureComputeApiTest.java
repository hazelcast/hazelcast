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

package com.hazelcast.azure;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.hazelcast.azure.AzureComputeApi.API_VERSION;
import static com.hazelcast.azure.AzureComputeApi.API_VERSION_SCALE_SET;
import static org.junit.Assert.assertEquals;

public class AzureComputeApiTest {
    private static final String SUBSCRIPTION_ID = "subscription-1";
    private static final String RESOURCE_GROUP = "resource-group-1";
    private static final String SCALE_SET = "scale-set-1";
    private static final Tag TAG = new Tag("key-1", "value-1");
    private static final String ACCESS_TOKEN = "access-token";

    private static final String INSTANCE_1_PRIVATE_IP = "10.240.0.2";
    private static final String INSTANCE_1_PUBLIC_IP = "35.207.0.219";
    private static final String INSTANCE_2_PRIVATE_IP = "10.240.0.3";
    private static final String INSTANCE_2_PUBLIC_IP = "35.237.227.147";
    private static final String INSTANCE_3_PRIVATE_IP = "10.240.0.4";
    private static final String INSTANCE_4_PRIVATE_IP = "10.240.0.5";

    private AzureComputeApi azureComputeApi;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        azureComputeApi = new AzureComputeApi(String.format("http://localhost:%s", wireMockRule.port()));
    }

    @Test
    public void instancesNoScaleSetNoTag() {
        // given
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                + "/networkInterfaces?api-version=%s", SUBSCRIPTION_ID, RESOURCE_GROUP, API_VERSION)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponseForNetworkInterfaces())));
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                + "/publicIPAddresses?api-version=%s", SUBSCRIPTION_ID, RESOURCE_GROUP, API_VERSION)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponseForPublicIPAddresses())));

        // when
        Collection<AzureAddress> result = azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, null, null, ACCESS_TOKEN);

        // then
        AzureAddress address1 = new AzureAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        AzureAddress address2 = new AzureAddress(INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP);
        AzureAddress address3 = new AzureAddress(INSTANCE_3_PRIVATE_IP, null);
        AzureAddress address4 = new AzureAddress(INSTANCE_4_PRIVATE_IP, null);
        Set<AzureAddress> expected = new LinkedHashSet<AzureAddress>(4);
        expected.add(address1);
        expected.add(address2);
        expected.add(address3);
        expected.add(address4);
        assertEquals(expected, result);
    }

    @Test
    public void instancesWithScaleSet() {
        // given
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute"
                        + "/virtualMachineScaleSets/%s/networkInterfaces?api-version=%s",
                        SUBSCRIPTION_ID, RESOURCE_GROUP, SCALE_SET, API_VERSION_SCALE_SET)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponseForNetworkInterfaces())));
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute"
                        + "/virtualMachineScaleSets/%s/publicIPAddresses?api-version=%s",
                        SUBSCRIPTION_ID, RESOURCE_GROUP, SCALE_SET, API_VERSION_SCALE_SET)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponseForPublicIPAddresses())));

        // when
        Collection<AzureAddress> result = azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, SCALE_SET, null, ACCESS_TOKEN);

        // then
        AzureAddress address1 = new AzureAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        AzureAddress address2 = new AzureAddress(INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP);
        AzureAddress address3 = new AzureAddress(INSTANCE_3_PRIVATE_IP, null);
        AzureAddress address4 = new AzureAddress(INSTANCE_4_PRIVATE_IP, null);
        Set<AzureAddress> expected = new LinkedHashSet<AzureAddress>(4);
        expected.add(address1);
        expected.add(address2);
        expected.add(address3);
        expected.add(address4);
        assertEquals(expected, result);
    }

    @Test
    public void instancesWithTag() {
        // given
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                + "/networkInterfaces?api-version=%s", SUBSCRIPTION_ID, RESOURCE_GROUP, API_VERSION)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponseForNetworkInterfaces())));
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                + "/publicIPAddresses?api-version=%s", SUBSCRIPTION_ID, RESOURCE_GROUP, API_VERSION)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponseForPublicIPAddresses())));

        // when
        Collection<AzureAddress> result = azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, null, TAG, ACCESS_TOKEN);

        // then
        AzureAddress address1 = new AzureAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        Set<AzureAddress> expected = new LinkedHashSet<AzureAddress>(2);
        expected.add(address1);
        assertEquals(expected, result);
    }

    /**
     * Response recorded from the real Cloud Compute API call.
     */
    private static String instancesResponseForNetworkInterfaces() {
        return String.format(
                "{\n"
                + "  \"value\": [\n"
                + "    {\n"
                + "      \"name\": \"test-nic\",\n"
                + "      \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic\",\n"
                + "      \"location\": \"eastus\",\n"
                + "      \"properties\": {\n"
                + "        \"provisioningState\": \"Succeeded\",\n"
                + "        \"ipConfigurations\": [\n"
                + "          {\n"
                + "            \"name\": \"ipconfig1\",\n"
                + "            \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic/ipConfigurations/ipconfig1\",\n"
                + "            \"properties\": {\n"
                + "              \"provisioningState\": \"Succeeded\",\n"
                + "              \"privateIPAddress\": \"%s\",\n"
                + "              \"privateIPAllocationMethod\": \"Dynamic\",\n"
                + "              \"publicIPAddress\": {\n"
                + "                \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip\"\n"
                + "              },\n"
                + "              \"subnet\": {\n"
                + "                \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet/subnets/default\"\n"
                + "              },\n"
                + "              \"primary\": true,\n"
                + "              \"privateIPAddressVersion\": \"IPv4\"\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"dnsSettings\": {\n"
                + "          \"dnsServers\": [],\n"
                + "          \"appliedDnsServers\": [],\n"
                + "          \"internalDomainNameSuffix\": \"test.bx.internal.cloudapp.net\"\n"
                + "        },\n"
                + "        \"macAddress\": \"00-0D-3A-1B-C7-21\",\n"
                + "        \"enableAcceleratedNetworking\": true,\n"
                + "        \"enableIPForwarding\": false,\n"
                + "        \"networkSecurityGroup\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg\"\n"
                + "        },\n"
                + "        \"primary\": true,\n"
                + "        \"virtualMachine\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"tags\": {\n"
                + "        \"%s\": \"%s\",\n"
                + "        \"tag2\": \"value2\"\n"
                + "      },\n"
                + "      \"type\": \"Microsoft.Network/networkInterfaces\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"test-nic2\",\n"
                + "      \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic2\",\n"
                + "      \"location\": \"eastus\",\n"
                + "      \"properties\": {\n"
                + "        \"provisioningState\": \"Succeeded\",\n"
                + "        \"ipConfigurations\": [\n"
                + "          {\n"
                + "            \"name\": \"ipconfig1\",\n"
                + "            \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic2/ipConfigurations/ipconfig1\",\n"
                + "            \"properties\": {\n"
                + "              \"provisioningState\": \"Succeeded\",\n"
                + "              \"privateIPAddress\": \"%s\",\n"
                + "              \"privateIPAllocationMethod\": \"Dynamic\",\n"
                + "              \"publicIPAddress\": {\n"
                + "                \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2\"\n"
                + "              },\n"
                + "              \"subnet\": {\n"
                + "                \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default\"\n"
                + "              },\n"
                + "              \"primary\": true,\n"
                + "              \"privateIPAddressVersion\": \"IPv4\"\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"dnsSettings\": {\n"
                + "          \"dnsServers\": [],\n"
                + "          \"appliedDnsServers\": [],\n"
                + "          \"internalDomainNameSuffix\": \"test2.bx.internal.cloudapp.net\"\n"
                + "        },\n"
                + "        \"macAddress\": \"00-0D-3A-1B-C7-22\",\n"
                + "        \"enableAcceleratedNetworking\": true,\n"
                + "        \"enableIPForwarding\": false,\n"
                + "        \"networkSecurityGroup\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg\"\n"
                + "        },\n"
                + "        \"primary\": true,\n"
                + "        \"virtualMachine\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm2\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"type\": \"Microsoft.Network/networkInterfaces\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"test-nic3\",\n"
                + "      \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic3\",\n"
                + "      \"location\": \"eastus\",\n"
                + "      \"properties\": {\n"
                + "        \"provisioningState\": \"Succeeded\",\n"
                + "        \"ipConfigurations\": [\n"
                + "          {\n"
                + "            \"name\": \"ipconfig1\",\n"
                + "            \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic3/ipConfigurations/ipconfig1\",\n"
                + "            \"properties\": {\n"
                + "              \"provisioningState\": \"Succeeded\",\n"
                + "              \"privateIPAddress\": \"%s\",\n"
                + "              \"privateIPAllocationMethod\": \"Dynamic\",\n"
                + "              \"subnet\": {\n"
                + "                \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default\"\n"
                + "              },\n"
                + "              \"primary\": true,\n"
                + "              \"privateIPAddressVersion\": \"IPv4\"\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"dnsSettings\": {\n"
                + "          \"dnsServers\": [],\n"
                + "          \"appliedDnsServers\": [],\n"
                + "          \"internalDomainNameSuffix\": \"test3.bx.internal.cloudapp.net\"\n"
                + "        },\n"
                + "        \"macAddress\": \"00-0D-3A-1B-C7-23\",\n"
                + "        \"enableAcceleratedNetworking\": true,\n"
                + "        \"enableIPForwarding\": false,\n"
                + "        \"networkSecurityGroup\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg\"\n"
                + "        },\n"
                + "        \"primary\": true,\n"
                + "        \"virtualMachine\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm3\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"type\": \"Microsoft.Network/networkInterfaces\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"test-nic4\",\n"
                + "      \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic4\",\n"
                + "      \"location\": \"eastus\",\n"
                + "      \"properties\": {\n"
                + "        \"provisioningState\": \"Succeeded\",\n"
                + "        \"ipConfigurations\": [\n"
                + "          {\n"
                + "            \"name\": \"ipconfig1\",\n"
                + "            \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic4/ipConfigurations/ipconfig1\",\n"
                + "            \"properties\": {\n"
                + "              \"provisioningState\": \"Succeeded\",\n"
                + "              \"privateIPAddress\": \"%s\",\n"
                + "              \"privateIPAllocationMethod\": \"Dynamic\",\n"
                + "              \"subnet\": {\n"
                + "                \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default\"\n"
                + "              },\n"
                + "              \"primary\": true,\n"
                + "              \"privateIPAddressVersion\": \"IPv4\"\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"dnsSettings\": {\n"
                + "          \"dnsServers\": [],\n"
                + "          \"appliedDnsServers\": [],\n"
                + "          \"internalDomainNameSuffix\": \"test4.bx.internal.cloudapp.net\"\n"
                + "        },\n"
                + "        \"macAddress\": \"00-0D-3A-1B-C7-43\",\n"
                + "        \"enableAcceleratedNetworking\": true,\n"
                + "        \"enableIPForwarding\": false,\n"
                + "        \"networkSecurityGroup\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg\"\n"
                + "        },\n"
                + "        \"primary\": true,\n"
                + "        \"virtualMachine\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm4\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"type\": \"Microsoft.Network/networkInterfaces\"\n"
                + "    }\n"
                + "  ]\n"
                + "}", INSTANCE_1_PRIVATE_IP, TAG.getKey(), TAG.getValue(), INSTANCE_2_PRIVATE_IP, INSTANCE_3_PRIVATE_IP, INSTANCE_4_PRIVATE_IP);
    }

    private String instancesResponseForPublicIPAddresses() {
        return String.format(
                "{\n"
                + "  \"value\": [\n"
                + "    {\n"
                + "      \"name\": \"ip02\",\n"
                + "      \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip\",\n"
                + "      \"location\": \"westus\",\n"
                + "      \"properties\": {\n"
                + "        \"provisioningState\": \"Succeeded\",\n"
                + "        \"ipAddress\": \"%s\",\n"
                + "        \"publicIPAddressVersion\": \"IPv4\",\n"
                + "        \"publicIPAllocationMethod\": \"Dynamic\",\n"
                + "        \"idleTimeoutInMinutes\": 4,\n"
                + "        \"dnsSettings\": {\n"
                + "          \"domainNameLabel\": \"testlbl1\",\n"
                + "          \"fqdn\": \"testlbl1.westus.cloudapp.azure.com\"\n"
                + "        },\n"
                + "        \"ipConfiguration\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/testLb/frontendIPConfigurations/LoadBalancerFrontEnd\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"type\": \"Microsoft.Network/publicIPAddresses\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"ip03\",\n"
                + "      \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2\",\n"
                + "      \"location\": \"westus\",\n"
                + "      \"properties\": {\n"
                + "        \"provisioningState\": \"Succeeded\",\n"
                + "        \"ipAddress\": \"%s\",\n"
                + "        \"publicIPAddressVersion\": \"IPv4\",\n"
                + "        \"publicIPAllocationMethod\": \"Dynamic\",\n"
                + "        \"idleTimeoutInMinutes\": 4,\n"
                + "        \"dnsSettings\": {\n"
                + "          \"domainNameLabel\": \"testlbl2\",\n"
                + "          \"fqdn\": \"testlbl2.westus.cloudapp.azure.com\"\n"
                + "        },\n"
                + "        \"ipConfiguration\": {\n"
                + "          \"id\": \"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/testLb/frontendIPConfigurations/LoadBalancerFrontEnd\"\n"
                + "        }\n"
                + "      },\n"
                + "      \"type\": \"Microsoft.Network/publicIPAddresses\"\n"
                + "    }\n"
                + "  ]\n"
                + "}", INSTANCE_1_PUBLIC_IP, INSTANCE_2_PUBLIC_IP);
    }
}
