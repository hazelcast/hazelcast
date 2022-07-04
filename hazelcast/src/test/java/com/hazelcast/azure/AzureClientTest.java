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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AzureClientTest {

    private static final String SUBSCRIPTION_ID = "subscription-1";
    private static final String RESOURCE_GROUP = "resource-group-1";
    private static final String SCALE_SET = "scale-set-1";
    private static final Tag TAG = new Tag("key-1", "value-1");
    private static final String ACCESS_TOKEN = "access-token";
    private static final String ZONE = "1";

    private static final List<AzureAddress> ADDRESSES = asList(
            new AzureAddress("10.240.0.2", "35.207.0.219"),
            new AzureAddress("10.240.0.3", "35.237.227.147"),
            new AzureAddress("10.240.0.4", "35.237.227.148"),
            new AzureAddress("10.240.0.5", "35.237.227.149"));

    @Mock
    private AzureMetadataApi azureMetadataApi;
    @Mock
    private AzureComputeApi azureComputeApi;
    @Mock
    private AzureAuthenticator azureAuthenticator;

    @Before
    public void setUp() {
        when(azureMetadataApi.accessToken()).thenReturn(ACCESS_TOKEN);
        when(azureMetadataApi.subscriptionId()).thenReturn(SUBSCRIPTION_ID);
        when(azureMetadataApi.resourceGroupName()).thenReturn(RESOURCE_GROUP);
        when(azureMetadataApi.scaleSet()).thenReturn(SCALE_SET);
    }

    @Test
    public void getAddressesCurrentSubscriptionCurrentResourceGroupCurrentScaleSetNoTag() {
        // given
        given(azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, SCALE_SET, null, ACCESS_TOKEN)).willReturn(ADDRESSES);

        AzureConfig azureConfig = AzureConfig.builder().setInstanceMetadataAvailable(true).build();
        AzureClient azureClient = new AzureClient(azureMetadataApi, azureComputeApi, azureAuthenticator, azureConfig);

        // when
        Collection<AzureAddress> result = azureClient.getAddresses();

        // then
        assertEquals(ADDRESSES, result);
    }

    @Test
    public void getAddressesCurrentSubscriptionCurrentResourceGroupCurrentScaleSetWithTag() {
        // given
        given(azureComputeApi.instances(SUBSCRIPTION_ID, RESOURCE_GROUP, SCALE_SET, TAG, ACCESS_TOKEN)).willReturn(ADDRESSES);

        AzureConfig azureConfig = AzureConfig.builder().setInstanceMetadataAvailable(true).setTag(TAG).build();
        AzureClient azureClient = new AzureClient(azureMetadataApi, azureComputeApi, azureAuthenticator, azureConfig);

        // when
        Collection<AzureAddress> result = azureClient.getAddresses();

        // then
        assertEquals(ADDRESSES, result);
    }

    @Test
    public void getAddressesWithConfiguredSettings() {
        // given
        String tenantId = "tenant-id";
        String clientId = "client-id";
        String clientSecret = "client-secret";
        given(azureAuthenticator.refreshAccessToken(tenantId, clientId, clientSecret)).willReturn(ACCESS_TOKEN);
        String subscriptionId = "subscription-2";
        String resourceGroup = "resource-group-2";
        String scaleSet = "scale-set-2";
        given(azureComputeApi.instances(subscriptionId, resourceGroup, scaleSet, TAG, ACCESS_TOKEN)).willReturn(ADDRESSES);

        AzureConfig azureConfig = AzureConfig.builder()
                                       .setClientId(clientId)
                                       .setTenantId(tenantId)
                                       .setClientSecret(clientSecret)
                                       .setSubscriptionId(subscriptionId)
                                       .setResourceGroup(resourceGroup)
                                       .setScaleSet(scaleSet)
                                       .setInstanceMetadataAvailable(false)
                                       .setTag(TAG)
                                       .build();
        AzureClient azureClient = new AzureClient(azureMetadataApi, azureComputeApi, azureAuthenticator, azureConfig);

        // when
        Collection<AzureAddress> result = azureClient.getAddresses();

        // then
        assertEquals(ADDRESSES, result);
    }

    @Test
    public void getAvailabilityZone() {
        // given
        String location = "location-1";
        given(azureMetadataApi.location()).willReturn(location);
        given(azureMetadataApi.availabilityZone()).willReturn(ZONE);
        AzureConfig azureConfig = AzureConfig.builder().setInstanceMetadataAvailable(true).build();
        AzureClient azureClient = new AzureClient(azureMetadataApi, azureComputeApi, azureAuthenticator, azureConfig);

        // when
        String result = azureClient.getAvailabilityZone();

        // then
        assertEquals(String.format("%s-%s", location, ZONE), result);
    }

}
