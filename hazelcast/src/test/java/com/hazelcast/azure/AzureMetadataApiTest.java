/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import java.util.HashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.hazelcast.azure.AzureMetadataApi.API_VERSION;
import static com.hazelcast.azure.AzureMetadataApi.RESOURCE;
import static org.junit.Assert.assertEquals;

public class AzureMetadataApiTest {
    private static final String ACCESS_TOKEN = "access-token-1";
    private static final String LOCATION = "lcoation-1";
    private static final String PLATFORM_FAULT_DOMAIN = "platform-fault-domain-1";
    private static final String RESOURCE_GROUP_NAME = "resource-group-name-1";
    private static final String SUBSCRIPTION_ID = "subscription-id-1";
    private static final String SCALE_SET_NAME = "scale-set-name-1";
    private static final String ZONE = "us-east1-b";

    private AzureMetadataApi azureMetadataApi;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        azureMetadataApi = new AzureMetadataApi(String.format("http://localhost:%s", wireMockRule.port()), new HashMap<>());
        //given
        stubFor(get(urlEqualTo(String.format("/metadata/instance/compute?api-version=%s", API_VERSION)))
                .withHeader("Metadata", equalTo("true"))
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(metadataResponse())));
    }

    @Test
    public void subscriptionId() {
        // when
        String result = azureMetadataApi.subscriptionId();

        // then
        assertEquals(SUBSCRIPTION_ID, result);
    }

    @Test
    public void resourceGroupName() {
        // when
        String result = azureMetadataApi.resourceGroupName();

        // then
        assertEquals(RESOURCE_GROUP_NAME, result);
    }

    @Test
    public void location() {
        // when
        String result = azureMetadataApi.location();

        // then
        assertEquals(LOCATION, result);
    }

    @Test
    public void availabilityZone() {
        // when
        String result = azureMetadataApi.availabilityZone();

        // then
        assertEquals(ZONE, result);
    }

    @Test
    public void faultDomain() {
        // when
        String result = azureMetadataApi.faultDomain();

        // then
        assertEquals(PLATFORM_FAULT_DOMAIN, result);
    }

    @Test
    public void scaleSet() {
        // when
        String result = azureMetadataApi.scaleSet();

        // then
        assertEquals(SCALE_SET_NAME, result);
    }

    @Test
    public void accessToken() {
        // given
        stubFor(get(urlEqualTo(String.format("/metadata/identity/oauth2/token?api-version=%s&resource=%s",
                API_VERSION, RESOURCE)))
                .withHeader("Metadata", equalTo("true"))
                .willReturn(aResponse().withStatus(HttpURLConnection.HTTP_OK).withBody(accessTokenResponse())));

        // when
        String result = azureMetadataApi.accessToken();

        // then
        assertEquals(ACCESS_TOKEN, result);
    }

    private static String accessTokenResponse() {
        return String.format(
                """
                        {
                          "access_token": "%s",
                          "refresh_token": "",
                          "expires_in": "3599",
                          "expires_on": "1506484173",
                          "not_before": "1506480273",
                          "resource": "https://management.azure.com/",
                          "token_type": "Bearer"
                        }""", ACCESS_TOKEN);
    }

    private String metadataResponse() {
        return String.format(
                """
                        {
                          "azEnvironment": "AzurePublicCloud",
                          "customData": "",
                          "location": "%s",
                          "name": "negasonic",
                          "offer": "lampstack",
                          "osType": "Linux",
                          "placementGroupId": "",
                          "plan": {
                            "name": "5-6",
                            "product": "lampstack",
                            "publisher": "bitnami"
                          },
                          "platformFaultDomain": "%s",
                          "platformUpdateDomain": "0",
                          "provider": "Microsoft.Compute",
                          "publicKeys": [],
                          "publisher": "bitnami",
                          "resourceGroupName": "%s",
                          "resourceId": "/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/myrg/providers/Microsoft.Compute/virtualMachines/negasonic",
                          "sku": "5-6",
                          "storageProfile": {
                            "dataDisks": [
                              {
                                "caching": "None",
                                "createOption": "Empty",
                                "diskSizeGB": "1024",
                                "image": {
                                  "uri": ""
                                },
                                "lun": "0",
                                "managedDisk": {
                                  "id": "/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/macikgo-test-may-23/providers/Microsoft.Compute/disks/exampledatadiskname",
                                  "storageAccountType": "Standard_LRS"
                                },
                                "name": "exampledatadiskname",
                                "vhd": {
                                  "uri": ""
                                },
                                "writeAcceleratorEnabled": "false"
                              }
                            ],
                            "imageReference": {
                              "id": "",
                              "offer": "UbuntuServer",
                              "publisher": "Canonical",
                              "sku": "16.04.0-LTS",
                              "version": "latest"
                            },
                            "osDisk": {
                              "caching": "ReadWrite",
                              "createOption": "FromImage",
                              "diskSizeGB": "30",
                              "diffDiskSettings": {
                                "option": "Local"
                              },
                              "encryptionSettings": {
                                "enabled": "false"
                              },
                              "image": {
                                "uri": ""
                              },
                              "managedDisk": {
                                "id": "/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/macikgo-test-may-23/providers/Microsoft.Compute/disks/exampleosdiskname",
                                "storageAccountType": "Standard_LRS"
                              },
                              "name": "exampleosdiskname",
                              "osType": "Linux",
                              "vhd": {
                                "uri": ""
                              },
                              "writeAcceleratorEnabled": "false"
                            }
                          },
                          "subscriptionId": "%s",
                          "tags": "Department:IT;Environment:Test;Role:WebRole",
                          "version": "7.1.1902271506",
                          "vmId": "13f56399-bd52-4150-9748-7190aae1ff21",
                          "vmScaleSetName": "%s",
                          "vmSize": "Standard_A1_v2",
                          "zone": "%s"
                        }""", LOCATION, PLATFORM_FAULT_DOMAIN, RESOURCE_GROUP_NAME, SUBSCRIPTION_ID, SCALE_SET_NAME, ZONE);
    }
}
