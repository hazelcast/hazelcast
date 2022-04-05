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

package com.hazelcast.gcp;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.spi.utils.PortRange;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.gcp.GcpProperties.LABEL;
import static com.hazelcast.gcp.GcpProperties.PORT;
import static com.hazelcast.gcp.GcpProperties.PRIVATE_KEY_PATH;
import static com.hazelcast.gcp.GcpProperties.PROJECTS;
import static com.hazelcast.gcp.GcpProperties.REGION;
import static com.hazelcast.gcp.GcpProperties.ZONES;
import static com.hazelcast.gcp.Utils.splitByComma;

/**
 * GCP implementation of {@link DiscoveryStrategy}.
 */
public class GcpDiscoveryStrategy
        extends AbstractDiscoveryStrategy {
    private static final ILogger LOGGER = Logger.getLogger(GcpDiscoveryStrategy.class);

    private final GcpClient gcpClient;
    private final PortRange portRange;

    private final Map<String, String> memberMetadata = new HashMap<>();

    GcpDiscoveryStrategy(Map<String, Comparable> properties) {
        super(LOGGER, properties);
        try {
            GcpConfig gcpConfig = createGcpConfig();
            GcpMetadataApi gcpMetadataApi = new GcpMetadataApi();
            GcpComputeApi gcpComputeApi = new GcpComputeApi();
            GcpAuthenticator gcpAuthenticator = new GcpAuthenticator();
            this.gcpClient = new GcpClient(gcpMetadataApi, gcpComputeApi, gcpAuthenticator, gcpConfig);
            this.portRange = gcpConfig.getHzPort();
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Invalid GCP Discovery Strategy configuration", e);
        }
    }

    /**
     * For test purposes only.
     */
    GcpDiscoveryStrategy(Map<String, Comparable> properties, GcpClient gcpClient) {
        super(LOGGER, properties);
        this.gcpClient = gcpClient;
        this.portRange = createGcpConfig().getHzPort();
    }

    private GcpConfig createGcpConfig() {
        return GcpConfig.builder()
                        .setPrivateKeyPath(getOrNull(PRIVATE_KEY_PATH))
                        .setProjects(splitByComma(getOrNull(PROJECTS)))
                        .setZones(splitByComma((getOrNull(ZONES))))
                        .setLabel(labelOrNull(LABEL))
                        .setHzPort(new PortRange((String) getOrDefault(PORT.getDefinition(), PORT.getDefaultValue())))
                        .setRegion(getOrNull(REGION))
                        .build();
    }

    private Label labelOrNull(GcpProperties gcpProperties) {
        String labelString = getOrNull(gcpProperties);
        if (labelString != null) {
            return new Label(labelString);
        }
        return null;
    }

    private String getOrNull(GcpProperties gcpProperties) {
        return getOrNull(gcpProperties.getDefinition());
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        if (memberMetadata.isEmpty()) {
            memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_ZONE, gcpClient.getAvailabilityZone());
        }
        return memberMetadata;
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            List<GcpAddress> gcpAddresses = gcpClient.getAddresses();
            logGcpAddresses(gcpAddresses);

            List<DiscoveryNode> result = new ArrayList<>();
            for (GcpAddress gcpAddress : gcpAddresses) {
                for (int port = portRange.getFromPort(); port <= portRange.getToPort(); port++) {
                    result.add(createDiscoveryNode(gcpAddress, port));
                }
            }

            return result;
        } catch (Exception e) {
            LOGGER.warning("Cannot discover nodes, returning empty list", e);
            return Collections.emptyList();
        }
    }

    private static DiscoveryNode createDiscoveryNode(GcpAddress gcpAddress, int port)
            throws UnknownHostException {
        Address privateAddress = new Address(gcpAddress.getPrivateAddress(), port);
        Address publicAddress = new Address(gcpAddress.getPublicAddress(), port);
        return new SimpleDiscoveryNode(privateAddress, publicAddress);
    }

    private static void logGcpAddresses(List<GcpAddress> gcpAddresses) {
        if (LOGGER.isFinestEnabled()) {
            StringBuilder stringBuilder = new StringBuilder("Found the following GCP instance: ");
            for (GcpAddress gcpAddress : gcpAddresses) {
                stringBuilder.append(String.format("%s, ", gcpAddress));
            }
            LOGGER.finest(stringBuilder.toString());
        }
    }
}
