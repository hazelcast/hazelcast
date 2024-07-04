/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import java.net.HttpURLConnection;
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
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(instancesResponseForNetworkInterfaces())));
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                + "/publicIPAddresses?api-version=%s", SUBSCRIPTION_ID, RESOURCE_GROUP, API_VERSION)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(instancesResponseForPublicIPAddresses())));

        // when
        Collection<AzureAddress> result = azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, null, null, ACCESS_TOKEN);

        // then
        AzureAddress address1 = new AzureAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        AzureAddress address2 = new AzureAddress(INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP);
        AzureAddress address3 = new AzureAddress(INSTANCE_3_PRIVATE_IP, null);
        AzureAddress address4 = new AzureAddress(INSTANCE_4_PRIVATE_IP, null);
        Set<AzureAddress> expected = new LinkedHashSet<>(4);
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
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(instancesResponseForNetworkInterfaces())));
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute"
                        + "/virtualMachineScaleSets/%s/publicIPAddresses?api-version=%s",
                        SUBSCRIPTION_ID, RESOURCE_GROUP, SCALE_SET, API_VERSION_SCALE_SET)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(instancesResponseForPublicIPAddresses())));

        // when
        Collection<AzureAddress> result = azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, SCALE_SET, null, ACCESS_TOKEN);

        // then
        AzureAddress address1 = new AzureAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        AzureAddress address2 = new AzureAddress(INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP);
        AzureAddress address3 = new AzureAddress(INSTANCE_3_PRIVATE_IP, null);
        AzureAddress address4 = new AzureAddress(INSTANCE_4_PRIVATE_IP, null);
        Set<AzureAddress> expected = new LinkedHashSet<>(4);
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
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(instancesResponseForNetworkInterfaces())));
        stubFor(get(urlEqualTo(String.format("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                + "/publicIPAddresses?api-version=%s", SUBSCRIPTION_ID, RESOURCE_GROUP, API_VERSION)))
                .withHeader("Authorization", equalTo(String.format("Bearer %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(instancesResponseForPublicIPAddresses())));

        // when
        Collection<AzureAddress> result = azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, null, TAG, ACCESS_TOKEN);

        // then
        AzureAddress address1 = new AzureAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        Set<AzureAddress> expected = new LinkedHashSet<>(2);
        expected.add(address1);
        assertEquals(expected, result);
    }

    /**
     * Response recorded from the real Cloud Compute API call.
     */
    private static String instancesResponseForNetworkInterfaces() {
        return String.format(
                """
                        {
                          "value": [
                            {
                              "name": "test-nic",
                              "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic",
                              "location": "eastus",
                              "properties": {
                                "provisioningState": "Succeeded",
                                "ipConfigurations": [
                                  {
                                    "name": "ipconfig1",
                                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic/ipConfigurations/ipconfig1",
                                    "properties": {
                                      "provisioningState": "Succeeded",
                                      "privateIPAddress": "%s",
                                      "privateIPAllocationMethod": "Dynamic",
                                      "publicIPAddress": {
                                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip"
                                      },
                                      "subnet": {
                                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet/subnets/default"
                                      },
                                      "primary": true,
                                      "privateIPAddressVersion": "IPv4"
                                    }
                                  }
                                ],
                                "dnsSettings": {
                                  "dnsServers": [],
                                  "appliedDnsServers": [],
                                  "internalDomainNameSuffix": "test.bx.internal.cloudapp.net"
                                },
                                "macAddress": "00-0D-3A-1B-C7-21",
                                "enableAcceleratedNetworking": true,
                                "enableIPForwarding": false,
                                "networkSecurityGroup": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg"
                                },
                                "primary": true,
                                "virtualMachine": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"
                                }
                              },
                              "tags": {
                                "%s": "%s",
                                "tag2": "value2"
                              },
                              "type": "Microsoft.Network/networkInterfaces"
                            },
                            {
                              "name": "test-nic2",
                              "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic2",
                              "location": "eastus",
                              "properties": {
                                "provisioningState": "Succeeded",
                                "ipConfigurations": [
                                  {
                                    "name": "ipconfig1",
                                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic2/ipConfigurations/ipconfig1",
                                    "properties": {
                                      "provisioningState": "Succeeded",
                                      "privateIPAddress": "%s",
                                      "privateIPAllocationMethod": "Dynamic",
                                      "publicIPAddress": {
                                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2"
                                      },
                                      "subnet": {
                                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default"
                                      },
                                      "primary": true,
                                      "privateIPAddressVersion": "IPv4"
                                    }
                                  }
                                ],
                                "dnsSettings": {
                                  "dnsServers": [],
                                  "appliedDnsServers": [],
                                  "internalDomainNameSuffix": "test2.bx.internal.cloudapp.net"
                                },
                                "macAddress": "00-0D-3A-1B-C7-22",
                                "enableAcceleratedNetworking": true,
                                "enableIPForwarding": false,
                                "networkSecurityGroup": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg"
                                },
                                "primary": true,
                                "virtualMachine": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm2"
                                }
                              },
                              "type": "Microsoft.Network/networkInterfaces"
                            },
                            {
                              "name": "test-nic3",
                              "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic3",
                              "location": "eastus",
                              "properties": {
                                "provisioningState": "Succeeded",
                                "ipConfigurations": [
                                  {
                                    "name": "ipconfig1",
                                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic3/ipConfigurations/ipconfig1",
                                    "properties": {
                                      "provisioningState": "Succeeded",
                                      "privateIPAddress": "%s",
                                      "privateIPAllocationMethod": "Dynamic",
                                      "subnet": {
                                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default"
                                      },
                                      "primary": true,
                                      "privateIPAddressVersion": "IPv4"
                                    }
                                  }
                                ],
                                "dnsSettings": {
                                  "dnsServers": [],
                                  "appliedDnsServers": [],
                                  "internalDomainNameSuffix": "test3.bx.internal.cloudapp.net"
                                },
                                "macAddress": "00-0D-3A-1B-C7-23",
                                "enableAcceleratedNetworking": true,
                                "enableIPForwarding": false,
                                "networkSecurityGroup": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg"
                                },
                                "primary": true,
                                "virtualMachine": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm3"
                                }
                              },
                              "type": "Microsoft.Network/networkInterfaces"
                            },
                            {
                              "name": "test-nic4",
                              "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic4",
                              "location": "eastus",
                              "properties": {
                                "provisioningState": "Succeeded",
                                "ipConfigurations": [
                                  {
                                    "name": "ipconfig1",
                                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic4/ipConfigurations/ipconfig1",
                                    "properties": {
                                      "provisioningState": "Succeeded",
                                      "privateIPAddress": "%s",
                                      "privateIPAllocationMethod": "Dynamic",
                                      "subnet": {
                                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default"
                                      },
                                      "primary": true,
                                      "privateIPAddressVersion": "IPv4"
                                    }
                                  }
                                ],
                                "dnsSettings": {
                                  "dnsServers": [],
                                  "appliedDnsServers": [],
                                  "internalDomainNameSuffix": "test4.bx.internal.cloudapp.net"
                                },
                                "macAddress": "00-0D-3A-1B-C7-43",
                                "enableAcceleratedNetworking": true,
                                "enableIPForwarding": false,
                                "networkSecurityGroup": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg"
                                },
                                "primary": true,
                                "virtualMachine": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm4"
                                }
                              },
                              "type": "Microsoft.Network/networkInterfaces"
                            }
                          ]
                        }""", INSTANCE_1_PRIVATE_IP, TAG.getKey(), TAG.getValue(), INSTANCE_2_PRIVATE_IP, INSTANCE_3_PRIVATE_IP, INSTANCE_4_PRIVATE_IP);
    }

    private String instancesResponseForPublicIPAddresses() {
        return String.format(
                """
                        {
                          "value": [
                            {
                              "name": "ip02",
                              "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip",
                              "location": "westus",
                              "properties": {
                                "provisioningState": "Succeeded",
                                "ipAddress": "%s",
                                "publicIPAddressVersion": "IPv4",
                                "publicIPAllocationMethod": "Dynamic",
                                "idleTimeoutInMinutes": 4,
                                "dnsSettings": {
                                  "domainNameLabel": "testlbl1",
                                  "fqdn": "testlbl1.westus.cloudapp.azure.com"
                                },
                                "ipConfiguration": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/testLb/frontendIPConfigurations/LoadBalancerFrontEnd"
                                }
                              },
                              "type": "Microsoft.Network/publicIPAddresses"
                            },
                            {
                              "name": "ip03",
                              "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2",
                              "location": "westus",
                              "properties": {
                                "provisioningState": "Succeeded",
                                "ipAddress": "%s",
                                "publicIPAddressVersion": "IPv4",
                                "publicIPAllocationMethod": "Dynamic",
                                "idleTimeoutInMinutes": 4,
                                "dnsSettings": {
                                  "domainNameLabel": "testlbl2",
                                  "fqdn": "testlbl2.westus.cloudapp.azure.com"
                                },
                                "ipConfiguration": {
                                  "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/testLb/frontendIPConfigurations/LoadBalancerFrontEnd"
                                }
                              },
                              "type": "Microsoft.Network/publicIPAddresses"
                            }
                          ]
                        }""", INSTANCE_1_PUBLIC_IP, INSTANCE_2_PUBLIC_IP);
    }
}
