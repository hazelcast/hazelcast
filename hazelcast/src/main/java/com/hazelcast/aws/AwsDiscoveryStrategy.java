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

package com.hazelcast.aws;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.PropertyDefinition;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.hazelcast.aws.AwsProperties.ACCESS_KEY;
import static com.hazelcast.aws.AwsProperties.CLUSTER;
import static com.hazelcast.aws.AwsProperties.CONNECTION_RETRIES;
import static com.hazelcast.aws.AwsProperties.CONNECTION_TIMEOUT_SECONDS;
import static com.hazelcast.aws.AwsProperties.FAMILY;
import static com.hazelcast.aws.AwsProperties.HOST_HEADER;
import static com.hazelcast.aws.AwsProperties.IAM_ROLE;
import static com.hazelcast.aws.AwsProperties.PORT;
import static com.hazelcast.aws.AwsProperties.READ_TIMEOUT_SECONDS;
import static com.hazelcast.aws.AwsProperties.REGION;
import static com.hazelcast.aws.AwsProperties.SECRET_KEY;
import static com.hazelcast.aws.AwsProperties.SECURITY_GROUP_NAME;
import static com.hazelcast.aws.AwsProperties.SERVICE_NAME;
import static com.hazelcast.aws.AwsProperties.TAG_KEY;
import static com.hazelcast.aws.AwsProperties.TAG_VALUE;

/**
 * AWS implementation of {@link DiscoveryStrategy}.
 *
 * @see AwsClient
 */
public class AwsDiscoveryStrategy
        extends AbstractDiscoveryStrategy {
    private static final ILogger LOGGER = Logger.getLogger(AwsDiscoveryStrategy.class);

    private static final int HTTP_FORBIDDEN = 403;

    private static final String DEFAULT_PORT_RANGE = "5701-5708";
    private static final Integer DEFAULT_CONNECTION_RETRIES = 3;
    private static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10;
    private static final int DEFAULT_READ_TIMEOUT_SECONDS = 10;

    private final AwsClient awsClient;
    private final PortRange portRange;

    private final Map<String, String> memberMetadata = new HashMap<>();

    private boolean isKnownExceptionAlreadyLogged;
    private boolean isEmptyAddressListAlreadyLogged;

    AwsDiscoveryStrategy(Map<String, Comparable> properties) {
        super(LOGGER, properties);

        AwsConfig awsConfig = createAwsConfig();
        LOGGER.info("Using AWS discovery plugin with configuration: " + awsConfig);

        this.awsClient = AwsClientConfigurator.createAwsClient(awsConfig);
        this.portRange = awsConfig.getHzPort();
    }

    /**
     * For test purposes only.
     */
    AwsDiscoveryStrategy(Map<String, Comparable> properties, AwsClient client) {
        super(LOGGER, properties);
        this.awsClient = client;
        this.portRange = createAwsConfig().getHzPort();
    }

    private AwsConfig createAwsConfig() {
        try {
            return AwsConfig.builder()
                .setAccessKey(getOrNull(ACCESS_KEY)).setSecretKey(getOrNull(SECRET_KEY))
                .setRegion(getOrDefault(REGION.getDefinition(), null))
                .setIamRole(getOrNull(IAM_ROLE))
                .setHostHeader(getOrNull(HOST_HEADER.getDefinition()))
                .setSecurityGroupName(getOrNull(SECURITY_GROUP_NAME)).setTagKey(getOrNull(TAG_KEY))
                .setTagValue(getOrNull(TAG_VALUE))
                .setConnectionTimeoutSeconds(getOrDefault(CONNECTION_TIMEOUT_SECONDS.getDefinition(),
                    DEFAULT_CONNECTION_TIMEOUT_SECONDS))
                .setConnectionRetries(getOrDefault(CONNECTION_RETRIES.getDefinition(), DEFAULT_CONNECTION_RETRIES))
                .setReadTimeoutSeconds(getOrDefault(READ_TIMEOUT_SECONDS.getDefinition(), DEFAULT_READ_TIMEOUT_SECONDS))
                .setHzPort(new PortRange(getPortRange()))
                .setCluster(getOrNull(CLUSTER))
                .setFamily(getOrNull(FAMILY))
                .setServiceName(getOrNull(SERVICE_NAME))
                .build();
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("AWS configuration is not valid", e);
        }
    }

    /**
     * Returns port range from properties or default value if the property does not exist.
     * <p>
     * Note that {@link AbstractDiscoveryStrategy#getOrDefault(PropertyDefinition, Comparable)} cannot be reused, since
     * the "hz-port" property can be either {@code String} or {@code Integer}.
     */
    private String getPortRange() {
        Object portRange = getOrNull(PORT.getDefinition());
        if (portRange == null) {
            return DEFAULT_PORT_RANGE;
        }
        return portRange.toString();
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        if (memberMetadata.isEmpty()) {
            String availabilityZone = awsClient.getAvailabilityZone();
            LOGGER.info(String.format("Availability zone found: '%s'", availabilityZone));
            memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_ZONE, availabilityZone);

            getPlacementGroup().ifPresent(pg ->
                    memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_PLACEMENT, availabilityZone + '-' + pg));
        }
        return memberMetadata;
    }

    /**
     * Resolves the placement group of the resource if it belongs to any.
     * <p>
     * If the placement group is Cluster Placement Group or Spread Placement Group, then returns
     * the group name. If it is Partition Placement Group, then returns the group name with the
     * partition number prefixed by '-' appended.
     * <p>
     * When forming partition groups, this name should be combined with zone name. Otherwise
     * two resources in different zones but in the same placement group will be assumed as
     * a single group.
     *
     * @see AwsClient#getPlacementGroup()
     * @see AwsClient#getPlacementPartitionNumber()
     * @return  Placement group name if exists, empty otherwise.
     */
    private Optional<String> getPlacementGroup() {
        Optional<String> placementGroup = awsClient.getPlacementGroup();
        if (!placementGroup.isPresent()) {
            LOGGER.fine("No placement group is found.");
            return Optional.empty();
        }
        StringBuilder result = new StringBuilder(placementGroup.get());
        awsClient.getPlacementPartitionNumber().ifPresent(ppn -> result.append('-').append(ppn));
        LOGGER.info(String.format("Placement group found: '%s'", result));
        return Optional.of(result.toString());
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            Map<String, String> addresses = awsClient.getAddresses();
            logResult(addresses);

            List<DiscoveryNode> result = new ArrayList<>();
            for (Map.Entry<String, String> entry : addresses.entrySet()) {
                for (int port = portRange.getFromPort(); port <= portRange.getToPort(); port++) {
                    Address privateAddress = new Address(entry.getKey(), port);
                    Address publicAddress = new Address(entry.getValue(), port);
                    result.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
                }
            }
            return result;
        } catch (NoCredentialsException e) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("No AWS credentials found! Starting standalone. To use Hazelcast AWS discovery, configure"
                        + " properties (access-key, secret-key) or assign the required IAM Role to your EC2 instance");
                LOGGER.finest(e);
                isKnownExceptionAlreadyLogged = true;
            }
        } catch (RestClientException e) {
            if (e.getHttpErrorCode() == HTTP_FORBIDDEN) {
                if (!isKnownExceptionAlreadyLogged) {
                    LOGGER.warning("AWS IAM Role Policy missing 'ec2:DescribeInstances' Action! Starting standalone.");
                    isKnownExceptionAlreadyLogged = true;
                }
                LOGGER.finest(e);
            } else {
                LOGGER.warning("Cannot discover nodes. Starting standalone.", e);
            }
        } catch (Exception e) {
            LOGGER.warning("Cannot discover nodes. Starting standalone.", e);
        }
        return Collections.emptyList();
    }

    private void logResult(Map<String, String> addresses) {
        if (addresses.isEmpty() && !isEmptyAddressListAlreadyLogged) {
            LOGGER.warning("No IP addresses found! Starting standalone.");
            isEmptyAddressListAlreadyLogged = true;
        }

        LOGGER.fine(String.format("Found the following (private => public) addresses: %s", addresses));
    }

    private String getOrNull(AwsProperties awsProperties) {
        return getOrNull(awsProperties.getDefinition());
    }
}
