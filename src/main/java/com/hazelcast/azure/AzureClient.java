/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Collection;
import java.util.concurrent.Callable;

import static com.hazelcast.azure.Utils.isAllBlank;
import static com.hazelcast.azure.Utils.isBlank;

/**
 * Responsible for fetching the discovery information from Azure APIs.
 */
final class AzureClient {
    private static final ILogger LOGGER = Logger.getLogger(AzureDiscoveryStrategy.class);

    private static final int RETRIES = 10;

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

    private final boolean hasSIM;

    AzureClient(AzureMetadataApi azureMetadataApi, AzureComputeApi azureComputeApi,
                AzureAuthenticator azureAuthenticator, AzureConfig azureConfig) {
        this.azureMetadataApi = azureMetadataApi;
        this.azureComputeApi = azureComputeApi;
        this.azureAuthenticator = azureAuthenticator;

        this.tenantId = azureConfig.getTenantId();
        this.clientId = azureConfig.getClientId();
        this.clientSecret = azureConfig.getClientSecret();
        this.subscriptionId = subscriptionIdFromConfigOrMetadataApi(azureConfig);
        this.resourceGroup = resourceGroupFromConfigOrMetadataApi(azureConfig);
        this.scaleSet = scaleSetFromConfigOrMetadataApi(azureConfig);
        this.tag = azureConfig.getTag();

        this.hasSIM = isAllBlank(tenantId, clientId, clientSecret);
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
        if (!isBlank(azureConfig.getResourceGroup())) {
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
        if (hasSIM) {
            return azureMetadataApi.accessToken();
        }
        return azureAuthenticator.refreshAccessToken(tenantId, clientId, clientSecret);
    }

    String getAvailabilityZone() {
        String zone = azureMetadataApi.availabilityZone();
        if (!isBlank(zone)) {
            return String.format("%s-%s", azureMetadataApi.location(), zone);
        } else {
            return azureMetadataApi.faultDomain();
        }
    }
}
