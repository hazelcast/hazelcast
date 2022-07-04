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
        azureMetadataApi = new AzureMetadataApi(String.format("http://localhost:%s", wireMockRule.port()), new HashMap<String, String>());
        //given
        stubFor(get(urlEqualTo(String.format("/metadata/instance/compute?api-version=%s", API_VERSION)))
                .withHeader("Metadata", equalTo("true"))
                .willReturn(aResponse().withStatus(200).withBody(metadataResponse())));
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
                .willReturn(aResponse().withStatus(200).withBody(accessTokenResponse())));

        // when
        String result = azureMetadataApi.accessToken();

        // then
        assertEquals(ACCESS_TOKEN, result);
    }

    private static String accessTokenResponse() {
        return String.format(
                "{\n"
                + "  \"access_token\": \"%s\",\n"
                + "  \"refresh_token\": \"\",\n"
                + "  \"expires_in\": \"3599\",\n"
                + "  \"expires_on\": \"1506484173\",\n"
                + "  \"not_before\": \"1506480273\",\n"
                + "  \"resource\": \"https://management.azure.com/\",\n"
                + "  \"token_type\": \"Bearer\"\n"
                + "}", ACCESS_TOKEN);
    }

    private String metadataResponse() {
        return String.format(
                "{\n"
                + "  \"azEnvironment\": \"AzurePublicCloud\",\n"
                + "  \"customData\": \"\",\n"
                + "  \"location\": \"%s\",\n"
                + "  \"name\": \"negasonic\",\n"
                + "  \"offer\": \"lampstack\",\n"
                + "  \"osType\": \"Linux\",\n"
                + "  \"placementGroupId\": \"\",\n"
                + "  \"plan\": {\n"
                + "    \"name\": \"5-6\",\n"
                + "    \"product\": \"lampstack\",\n"
                + "    \"publisher\": \"bitnami\"\n"
                + "  },\n"
                + "  \"platformFaultDomain\": \"%s\",\n"
                + "  \"platformUpdateDomain\": \"0\",\n"
                + "  \"provider\": \"Microsoft.Compute\",\n"
                + "  \"publicKeys\": [],\n"
                + "  \"publisher\": \"bitnami\",\n"
                + "  \"resourceGroupName\": \"%s\",\n"
                + "  \"resourceId\": \"/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/myrg/providers/Microsoft.Compute/virtualMachines/negasonic\",\n"
                + "  \"sku\": \"5-6\",\n"
                + "  \"storageProfile\": {\n"
                + "    \"dataDisks\": [\n"
                + "      {\n"
                + "        \"caching\": \"None\",\n"
                + "        \"createOption\": \"Empty\",\n"
                + "        \"diskSizeGB\": \"1024\",\n"
                + "        \"image\": {\n"
                + "          \"uri\": \"\"\n"
                + "        },\n"
                + "        \"lun\": \"0\",\n"
                + "        \"managedDisk\": {\n"
                + "          \"id\": \"/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/macikgo-test-may-23/providers/Microsoft.Compute/disks/exampledatadiskname\",\n"
                + "          \"storageAccountType\": \"Standard_LRS\"\n"
                + "        },\n"
                + "        \"name\": \"exampledatadiskname\",\n"
                + "        \"vhd\": {\n"
                + "          \"uri\": \"\"\n"
                + "        },\n"
                + "        \"writeAcceleratorEnabled\": \"false\"\n"
                + "      }\n"
                + "    ],\n"
                + "    \"imageReference\": {\n"
                + "      \"id\": \"\",\n"
                + "      \"offer\": \"UbuntuServer\",\n"
                + "      \"publisher\": \"Canonical\",\n"
                + "      \"sku\": \"16.04.0-LTS\",\n"
                + "      \"version\": \"latest\"\n"
                + "    },\n"
                + "    \"osDisk\": {\n"
                + "      \"caching\": \"ReadWrite\",\n"
                + "      \"createOption\": \"FromImage\",\n"
                + "      \"diskSizeGB\": \"30\",\n"
                + "      \"diffDiskSettings\": {\n"
                + "        \"option\": \"Local\"\n"
                + "      },\n"
                + "      \"encryptionSettings\": {\n"
                + "        \"enabled\": \"false\"\n"
                + "      },\n"
                + "      \"image\": {\n"
                + "        \"uri\": \"\"\n"
                + "      },\n"
                + "      \"managedDisk\": {\n"
                + "        \"id\": \"/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/macikgo-test-may-23/providers/Microsoft.Compute/disks/exampleosdiskname\",\n"
                + "        \"storageAccountType\": \"Standard_LRS\"\n"
                + "      },\n"
                + "      \"name\": \"exampleosdiskname\",\n"
                + "      \"osType\": \"Linux\",\n"
                + "      \"vhd\": {\n"
                + "        \"uri\": \"\"\n"
                + "      },\n"
                + "      \"writeAcceleratorEnabled\": \"false\"\n"
                + "    }\n"
                + "  },\n"
                + "  \"subscriptionId\": \"%s\",\n"
                + "  \"tags\": \"Department:IT;Environment:Test;Role:WebRole\",\n"
                + "  \"version\": \"7.1.1902271506\",\n"
                + "  \"vmId\": \"13f56399-bd52-4150-9748-7190aae1ff21\",\n"
                + "  \"vmScaleSetName\": \"%s\",\n"
                + "  \"vmSize\": \"Standard_A1_v2\",\n"
                + "  \"zone\": \"%s\"\n"
                + "}", LOCATION, PLATFORM_FAULT_DOMAIN, RESOURCE_GROUP_NAME, SUBSCRIPTION_ID, SCALE_SET_NAME, ZONE);
    }
}
