/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.config.InvalidConfigurationException;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import static com.hazelcast.azure.Utils.isAllBlank;
import static com.hazelcast.azure.Utils.isAllNotBlank;
import static com.hazelcast.azure.Utils.isBlank;

/**
 * Responsible for fetching the discovery information from Azure APIs.
 */
class AzureClient {
    private static final Logger LOGGER = Logger.getLogger(AzureDiscoveryStrategy.class.getSimpleName());

    private static final int RETRIES = 2;

    private final AzureMetadataApi azureMetadataApi;
    private final AzureComputeApi azureComputeApi;
    private final AzureAuthenticator azureAuthenticator;

    private final String tenantId;
    private final String clientId;
    private final String clientSecret;
    private final String subscriptionId;
    private final String resourceGroup;
    private final String scaleSet;
    private final Tag tag;

    private final boolean hasSMIRight;

    AzureClient(AzureMetadataApi azureMetadataApi, AzureComputeApi azureComputeApi,
                AzureAuthenticator azureAuthenticator, AzureConfig azureConfig) {
        this.azureMetadataApi = azureMetadataApi;
        this.azureComputeApi = azureComputeApi;
        this.azureAuthenticator = azureAuthenticator;

        this.tenantId = azureConfig.getTenantId();
        this.clientId = azureConfig.getClientId();
        this.clientSecret = azureConfig.getClientSecret();
        this.hasSMIRight = hasSMIRight(tenantId, clientId, clientSecret);
        this.subscriptionId = subscriptionIdFromConfigOrMetadataApi(azureConfig);
        this.resourceGroup = resourceGroupFromConfigOrMetadataApi(azureConfig);
        this.scaleSet = scaleSetFromConfigOrMetadataApi(azureConfig);
        this.tag = azureConfig.getTag();
    }

    private boolean hasSMIRight(String tenantId, String clientId, String clientSecret) {
        boolean hasSMIRight = isAllBlank(tenantId, clientId, clientSecret);
        if (!hasSMIRight && !isAllNotBlank(tenantId, clientId, clientSecret)) {
            //All 3 property must be defined & not empty
            throw new InvalidConfigurationException("Invalid Azure Discovery config: "
                    + "All of tenantId, clientId & clientSecret must defined or none");
        }
        return hasSMIRight;
    }

    private String subscriptionIdFromConfigOrMetadataApi(final AzureConfig azureConfig) {
        if (!isBlank(azureConfig.getSubscriptionId())) {
            return azureConfig.getSubscriptionId();
        }
        LOGGER.finest("Property 'subscriptionId' not configured, fetching from the VM metadata service");
        return RetryUtils.retry(new Callable<String>() {
            @Override
            public String call() {
                return azureMetadataApi.subscriptionId();
            }
        }, RETRIES);
    }

    private String resourceGroupFromConfigOrMetadataApi(final AzureConfig azureConfig) {
        if (!isBlank(azureConfig.getResourceGroup()) || azureConfig.isClient()) {
            return azureConfig.getResourceGroup();
        }
        LOGGER.finest("Property 'resourceGroup' not configured, fetching from the VM metadata service");
        return RetryUtils.retry(new Callable<String>() {
            @Override
            public String call() {
                return azureMetadataApi.resourceGroupName();
            }
        }, RETRIES);
    }

    private String scaleSetFromConfigOrMetadataApi(final AzureConfig azureConfig) {
        if (!isBlank(azureConfig.getScaleSet()) || azureConfig.isClient()) {
            return azureConfig.getScaleSet();
        }
        LOGGER.finest("Property 'scaleSet' not configured, fetching from the VM metadata service");
        return RetryUtils.retry(new Callable<String>() {
            @Override
            public String call() {
                return azureMetadataApi.scaleSet();
            }
        }, RETRIES);
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
        if (hasSMIRight) {
            return azureMetadataApi.accessToken();
        } else {
            return azureAuthenticator.refreshAccessToken(tenantId, clientId, clientSecret);
        }
    }

    String getAvailabilityZone() {
        String zone = azureMetadataApi.availabilityZone();
        if (isBlank(zone)) {
            return azureMetadataApi.faultDomain();
        } else {
            return String.format("%s-%s", azureMetadataApi.location(), zone);
        }
    }
}
