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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.exception.NoCredentialsException;
import com.hazelcast.spi.exception.RestClientException;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.spi.utils.PortRange;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.azure.AzureProperties.CLIENT_ID;
import static com.hazelcast.azure.AzureProperties.CLIENT_SECRET;
import static com.hazelcast.azure.AzureProperties.INSTANCE_METADATA_AVAILABLE;
import static com.hazelcast.azure.AzureProperties.PORT;
import static com.hazelcast.azure.AzureProperties.RESOURCE_GROUP;
import static com.hazelcast.azure.AzureProperties.SCALE_SET;
import static com.hazelcast.azure.AzureProperties.SUBSCRIPTION_ID;
import static com.hazelcast.azure.AzureProperties.TENANT_ID;
import static com.hazelcast.internal.util.StringUtil.isAllNullOrEmptyAfterTrim;
import static com.hazelcast.internal.util.StringUtil.isAnyNullOrEmptyAfterTrim;

/**
 * Azure implementation of {@link DiscoveryStrategy}
 */
public class AzureDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private static final ILogger LOGGER = Logger.getLogger(AzureDiscoveryStrategy.class);

    private static final int HTTP_FORBIDDEN = 403;

    private final AzureClient azureClient;
    private final PortRange portRange;
    private final Map<String, String> memberMetadata = new HashMap<>();

    private boolean isKnownExceptionAlreadyLogged;

    AzureDiscoveryStrategy(Map<String, Comparable> properties) {
        super(LOGGER, properties);
        try {
            AzureConfig azureConfig = createAzureConfig();
            AzureMetadataApi azureMetadataApi = new AzureMetadataApi();
            AzureComputeApi azureComputeApi = new AzureComputeApi();
            AzureAuthenticator azureAuthenticator = new AzureAuthenticator();
            this.azureClient = new AzureClient(azureMetadataApi, azureComputeApi, azureAuthenticator, azureConfig);
            this.portRange = azureConfig.getHzPort();
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Invalid Azure Discovery Strategy configuration", e);
        }
    }

    /**
     * For test purposes only.
     */
    AzureDiscoveryStrategy(Map<String, Comparable> properties, AzureClient azureClient) {
        super(LOGGER, properties);
        this.azureClient = azureClient;
        this.portRange = createAzureConfig().getHzPort();
    }

    private AzureConfig createAzureConfig() {
        AzureConfig azureConfig = AzureConfig.builder().setTenantId(getOrNull(TENANT_ID))
                                             .setClientId(getOrNull(CLIENT_ID))
                                             .setClientSecret(getOrNull(CLIENT_SECRET))
                                             .setSubscriptionId(getOrNull(SUBSCRIPTION_ID))
                                             .setResourceGroup(getOrNull(RESOURCE_GROUP))
                                             .setScaleSet(getOrNull(SCALE_SET))
                                             .setTag(tagOrNull())
                                             .setHzPort(
                                                     new PortRange((String) getOrDefault(PORT.getDefinition(),
                                                             PORT.getDefaultValue())))
                                             .setInstanceMetadataAvailable(
                                                     (Boolean) getOrDefault(INSTANCE_METADATA_AVAILABLE.getDefinition(),
                                                             INSTANCE_METADATA_AVAILABLE.getDefaultValue()))
                                             .build();
        validate(azureConfig);
        return azureConfig;
    }

    private Tag tagOrNull() {
        String tagString = getOrNull(AzureProperties.TAG);
        if (tagString != null) {
            return new Tag(tagString);
        }
        return null;
    }

    private String getOrNull(AzureProperties azureProperties) {
        return getOrNull(azureProperties.getDefinition());
    }

    private void validate(AzureConfig azureConfig) {
        if (!azureConfig.isInstanceMetadataAvailable()) {
            LOGGER.info("instance-metadata-available is set to false, validating other properties...");
            if (!isAllNullOrEmptyAfterTrim(azureConfig.getTenantId(),
                    azureConfig.getClientId(),
                    azureConfig.getClientSecret(),
                    azureConfig.getSubscriptionId(),
                    azureConfig.getResourceGroup())) {
                throw new InvalidConfigurationException("Invalid Azure Discovery config: "
                        + "useInstanceMetada property is configured as `false`. Please configure all of tenantId, clientId, "
                        + "clientSecret, subscriptionId, and resourceGroup properties.");
            }
        } else {
            if (isAnyNullOrEmptyAfterTrim(azureConfig.getTenantId(),
                    azureConfig.getClientId(),
                    azureConfig.getClientSecret(),
                    azureConfig.getSubscriptionId(),
                    azureConfig.getResourceGroup(),
                    azureConfig.getScaleSet())) {
                throw new InvalidConfigurationException("Invalid Azure Discovery config: "
                        + "useInstanceMetada property is configured as `true`. Please DO NOT configure any of tenantId, "
                        + "clientId, clientSecret, subscriptionId, resourceGroup, and scaleSet properties.");
            }
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            Collection<AzureAddress> azureAddresses = azureClient.getAddresses();
            logAzureAddresses(azureAddresses);
            List<DiscoveryNode> result = new ArrayList<>();
            for (AzureAddress azureAddress : azureAddresses) {
                for (int port = portRange.getFromPort(); port <= portRange.getToPort(); port++) {
                    result.add(createDiscoveryNode(azureAddress, port));
                }
            }
            return result;
        } catch (NoCredentialsException e) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("No Azure credentials found! Starting standalone. To use Hazelcast Azure discovery, configure"
                        + " properties (client-id, tenant-id, client-secret) or assign a managed identity to the Azure Compute"
                        + " instance");
                LOGGER.finest(e);
                isKnownExceptionAlreadyLogged = true;
            }
        } catch (RestClientException e) {
            if (e.getHttpErrorCode() == HTTP_FORBIDDEN) {
                if (!isKnownExceptionAlreadyLogged) {
                    LOGGER.warning("Required role is not assigned to service principal! To use Hazelcast Azure discovery assign"
                           + " a role to service principal with correct 'Read' permissions. Starting standalone.");
                    isKnownExceptionAlreadyLogged = true;
                }
                LOGGER.finest(e);
            } else {
                LOGGER.warning("Cannot discover nodes. Starting standalone.", e);
            }
        }  catch (Exception e) {
            LOGGER.warning("Cannot discover nodes. Starting standalone.", e);
        }
        return Collections.emptyList();
    }

    private static void logAzureAddresses(Collection<AzureAddress> azureAddresses) {
        if (LOGGER.isFinestEnabled()) {
            StringBuilder stringBuilder = new StringBuilder("Found the following Azure instances: ");
            for (AzureAddress azureAddress : azureAddresses) {
                stringBuilder.append(String.format("%s, ", azureAddress));
            }
            LOGGER.finest(stringBuilder.toString());
        }
    }

    private static DiscoveryNode createDiscoveryNode(AzureAddress azureAddress, int port)
            throws UnknownHostException {
        Address privateAddress = new Address(azureAddress.getPrivateAddress(), port);
        Address publicAddress = new Address(azureAddress.getPublicAddress(), port);
        return new SimpleDiscoveryNode(privateAddress, publicAddress);
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        if (memberMetadata.isEmpty()) {
            memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_ZONE, azureClient.getAvailabilityZone());
        }
        return memberMetadata;
    }
}
