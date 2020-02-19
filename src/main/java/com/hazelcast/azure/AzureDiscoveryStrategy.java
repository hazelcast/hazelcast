/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.azure.AzureProperties.CLIENT_ID;
import static com.hazelcast.azure.AzureProperties.CLIENT_SECRET;
import static com.hazelcast.azure.AzureProperties.PORT;
import static com.hazelcast.azure.AzureProperties.RESOURCE_GROUP;
import static com.hazelcast.azure.AzureProperties.SCALE_SET;
import static com.hazelcast.azure.AzureProperties.SUBSCRIPTION_ID;
import static com.hazelcast.azure.AzureProperties.TAG;
import static com.hazelcast.azure.AzureProperties.TENANT_ID;
import static com.hazelcast.azure.Utils.isAllBlank;
import static com.hazelcast.azure.Utils.isAllNotBlank;

/**
 * Azure implementation of {@link DiscoveryStrategy}
 */
public class AzureDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private static final ILogger LOGGER = Logger.getLogger(AzureDiscoveryStrategy.class);

    private final AzureClient azureClient;
    private final PortRange portRange;
    private final Map<String, String> memberMetadata = new HashMap<String, String>();

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
                                             .setTag(tagOrNull(TAG))
                                             .setHzPort(
                                                     new PortRange((String) getOrDefault(PORT.getDefinition(),
                                                             PORT.getDefaultValue())))
                                             .build();
        String tenantId = azureConfig.getTenantId();
        String clientId = azureConfig.getClientId();
        String clientSecret = azureConfig.getClientSecret();
        if (!(isAllBlank(tenantId, clientId, clientSecret) || isAllNotBlank(tenantId, clientId, clientSecret))) {
            //All 3 property must be defined & not empty
            throw new InvalidConfigurationException("Invalid Azure Discovery config: "
                    + "All of tenantId, clientId & clientSecret must defined or none");
        }
        return azureConfig;
    }

    private Tag tagOrNull(AzureProperties azureProperties) {
        String tagString = getOrNull(azureProperties);
        if (tagString != null) {
            return new Tag(tagString);
        }
        return null;
    }

    private String getOrNull(AzureProperties azureProperties) {
        return getOrNull(azureProperties.getDefinition());
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            Collection<AzureAddress> azureAddresses = azureClient.getAddresses();
            logAzureAddresses(azureAddresses);
            List<DiscoveryNode> result = new ArrayList<DiscoveryNode>();
            for (AzureAddress azureAddress : azureAddresses) {
                for (int port = portRange.getFromPort(); port <= portRange.getToPort(); port++) {
                    result.add(createDiscoveryNode(azureAddress, port));
                }
            }
            return result;
        } catch (Exception e) {
            LOGGER.warning("Cannot discover nodes, returning empty list", e);
            return Collections.emptyList();
        }
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
