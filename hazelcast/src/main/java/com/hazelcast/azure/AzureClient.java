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


import com.hazelcast.spi.utils.RetryUtils;

import java.util.Collection;
import java.util.logging.Logger;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Responsible for fetching the discovery information from Azure APIs.
 */
class AzureClient {
    private static final Logger LOGGER = Logger.getLogger(AzureClient.class.getSimpleName());

    private static final int RETRIES = 2;

    private final AzureMetadataApi azureMetadataApi;
    private final AzureComputeApi azureComputeApi;
    private final AzureAuthenticator azureAuthenticator;

    private final AzureConfig azureConfig;
    private final Tag tag;

    private final String subscriptionId;
    private final String resourceGroup;
    private final String scaleSet;

    AzureClient(AzureMetadataApi azureMetadataApi, AzureComputeApi azureComputeApi,
                AzureAuthenticator azureAuthenticator, AzureConfig azureConfig) {
        this.azureMetadataApi = azureMetadataApi;
        this.azureComputeApi = azureComputeApi;
        this.azureAuthenticator = azureAuthenticator;
        this.azureConfig = azureConfig;

        this.subscriptionId = subscriptionIdFromConfigOrMetadataApi();
        this.resourceGroup = resourceGroupFromConfigOrMetadataApi();
        this.scaleSet = scaleSetFromConfigOrMetadataApi();
        this.tag = azureConfig.getTag();
    }

    private String subscriptionIdFromConfigOrMetadataApi() {
        if (!isNullOrEmptyAfterTrim(azureConfig.getSubscriptionId())) {
            return azureConfig.getSubscriptionId();
        }
        LOGGER.finest("Property 'subscriptionId' not configured, fetching from the VM metadata service");
        return RetryUtils.retry(azureMetadataApi::subscriptionId, RETRIES);
    }

    private String resourceGroupFromConfigOrMetadataApi() {
        if (!azureConfig.isInstanceMetadataAvailable()) {
            return azureConfig.getResourceGroup();
        }
        LOGGER.finest("Property 'resourceGroup' not configured, fetching from the VM metadata service");
        return RetryUtils.retry(azureMetadataApi::resourceGroupName, RETRIES);
    }

    private String scaleSetFromConfigOrMetadataApi() {
        if (!azureConfig.isInstanceMetadataAvailable()) {
            return azureConfig.getScaleSet();
        }
        LOGGER.finest("Property 'scaleSet' not configured, fetching from the VM metadata service");
        return RetryUtils.retry(azureMetadataApi::scaleSet, RETRIES);
    }

    Collection<AzureAddress> getAddresses() {
        LOGGER.finest("Fetching OAuth Access Token");
        final String accessToken = fetchAccessToken();
        LOGGER.finest(String.format("Fetching instances for subscription '%s' and resourceGroup '%s'",
                subscriptionId, resourceGroup));
        Collection<AzureAddress> addresses = azureComputeApi.instances(subscriptionId, resourceGroup,
                scaleSet, tag, accessToken);
        LOGGER.finest(String.format("Found the following instances for project '%s' and zone '%s': %s",
                subscriptionId, resourceGroup,
                addresses));
        return addresses;
    }

    private String fetchAccessToken() {
        if (azureConfig.isInstanceMetadataAvailable()) {
            return azureMetadataApi.accessToken();
        } else {
            return azureAuthenticator.refreshAccessToken(azureConfig.getTenantId(), azureConfig.getClientId(),
                    azureConfig.getClientSecret());
        }
    }

    /**
     * This method creates an availability zone string by joining Azure Location and Zone properties because availability zones
     * are defined in locations in Azure environment.
     *
     * @see <a href="https://docs.microsoft.com/en-us/azure/availability-zones/az-overview">Availability Zones in Azure</a>
     *
     * @return Availability zone string in "LOCATION-ZONE" format
     */
    String getAvailabilityZone() {
        String zone = azureMetadataApi.availabilityZone();
        if (isNullOrEmptyAfterTrim(zone)) {
            return azureMetadataApi.faultDomain();
        } else {
            return String.format("%s-%s", azureMetadataApi.location(), zone);
        }
    }
}
