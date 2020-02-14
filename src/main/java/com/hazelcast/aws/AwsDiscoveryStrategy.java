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

package com.hazelcast.aws;

import com.hazelcast.aws.utility.MetadataUtil;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.internal.util.StringUtil;

import java.util.*;
import java.util.regex.Pattern;

import static com.hazelcast.aws.AwsProperties.ACCESS_KEY;
import static com.hazelcast.aws.AwsProperties.CONNECTION_RETRIES;
import static com.hazelcast.aws.AwsProperties.CONNECTION_TIMEOUT_SECONDS;
import static com.hazelcast.aws.AwsProperties.HOST_HEADER;
import static com.hazelcast.aws.AwsProperties.IAM_ROLE;
import static com.hazelcast.aws.AwsProperties.PORT;
import static com.hazelcast.aws.AwsProperties.REGION;
import static com.hazelcast.aws.AwsProperties.SECRET_KEY;
import static com.hazelcast.aws.AwsProperties.SECURITY_GROUP_NAME;
import static com.hazelcast.aws.AwsProperties.TAG_KEY;
import static com.hazelcast.aws.AwsProperties.TAG_VALUE;

/**
 * AWS implementation of {@link DiscoveryStrategy}.
 *
 * @see AWSClient
 */
public class AwsDiscoveryStrategy
        extends AbstractDiscoveryStrategy {
    private static final ILogger LOGGER = Logger.getLogger(AwsDiscoveryStrategy.class);
    private static final String DEFAULT_PORT_RANGE = "5701-5708";
    private static final Integer DEFAULT_CONNECTION_RETRIES = 3;
    private static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10;
    private static final String DEFAULT_HOST_HEADER = "ec2.amazonaws.com";
    private static final String AWS_REGION_REGEX = "\\w{2}(-gov-|-)(north|northeast|east|southeast|south|southwest|west|northwest|central)-\\d(?!.+)";
    private static final Pattern AWS_REGION_PATTERN = Pattern.compile(AWS_REGION_REGEX);

    private final AwsConfig awsConfig;
    private final AWSClient awsClient;

    private final Map<String, String> memberMetadata = new HashMap<String, String>();

    public AwsDiscoveryStrategy(Map<String, Comparable> properties) {
        super(LOGGER, properties);
        this.awsConfig = getAwsConfig();
        try {
            this.awsClient = new AWSClient(awsConfig);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("AWS configuration is not valid", e);
        }
    }

    /**
     * For test purposes only.
     */
    AwsDiscoveryStrategy(Map<String, Comparable> properties, AWSClient client) {
        super(LOGGER, properties);
        this.awsConfig = getAwsConfig();
        this.awsClient = client;
    }

    /**
     * For test purposes only.
     */
    AwsDiscoveryStrategy(Map<String, Comparable> properties, AwsConfig awsConfig, AWSClient client) {
        super(LOGGER, properties);
        this.awsConfig = awsConfig;
        this.awsClient = client;
    }

    AwsConfig getAwsConfig()
            throws IllegalArgumentException {
        Integer connectionRetries = getOrDefault(CONNECTION_RETRIES.getDefinition(), DEFAULT_CONNECTION_RETRIES);
        Integer connectionTimeoutSeconds = getOrDefault(CONNECTION_TIMEOUT_SECONDS.getDefinition(),
                DEFAULT_CONNECTION_TIMEOUT_SECONDS);
        String region = getOrDefault(REGION.getDefinition(), null);
        //to prevent unnecessary metadata call when region is set
        if (region == null) {
            region = getCurrentRegion(connectionTimeoutSeconds, connectionRetries);
            getLogger().info("Region not set, using the current region: " + region);
        }

        validateRegion(region);

        final AwsConfig config = AwsConfig.builder().setAccessKey(getOrNull(ACCESS_KEY)).setSecretKey(getOrNull(SECRET_KEY))
                                          .setRegion(region)
                                          .setIamRole(getOrNull(IAM_ROLE))
                                          .setHostHeader(getOrDefault(HOST_HEADER.getDefinition(), DEFAULT_HOST_HEADER))
                                          .setSecurityGroupName(getOrNull(SECURITY_GROUP_NAME)).setTagKey(getOrNull(TAG_KEY))
                                          .setTagValue(getOrNull(TAG_VALUE))
                                          .setConnectionTimeoutSeconds(connectionTimeoutSeconds)
                                          .setConnectionRetries(connectionRetries)
                                          .setHzPort(new PortRange(getPortRange())).build();

        reviewConfiguration(config);
        return config;
    }

    String getCurrentRegion(int connectionTimeoutSeconds, int connectionRetries) {
        String availabilityZone = MetadataUtil.getAvailabilityZone(connectionTimeoutSeconds, connectionRetries);
        return availabilityZone.substring(0, availabilityZone.length() - 1);
    }

    void validateRegion(String region) {
        if (!AWS_REGION_PATTERN.matcher(region).matches()) {
            String message = String.format("The provided region %s is not a valid AWS region.", region);
            throw new InvalidConfigurationException(message);
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

    private void reviewConfiguration(AwsConfig config) {
        if (StringUtil.isNullOrEmptyAfterTrim(config.getSecretKey()) || StringUtil
                .isNullOrEmptyAfterTrim(config.getAccessKey())) {

            if (!StringUtil.isNullOrEmptyAfterTrim(config.getIamRole())) {
                getLogger().info("Describe instances will be queried with iam-role, "
                        + "please make sure given iam-role have ec2:DescribeInstances policy attached.");
            } else {
                getLogger().warning("Describe instances will be queried with iam-role assigned to EC2 instance, "
                        + "please make sure given iam-role have ec2:DescribeInstances policy attached.");
            }
        } else {
            if (!StringUtil.isNullOrEmptyAfterTrim(config.getIamRole())) {
                getLogger().info("No need to define iam-role, when access and secret keys are configured!");
            }
        }
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        if (memberMetadata.isEmpty()) {
            memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_ZONE, awsClient.getAvailabilityZone());
        }
        return memberMetadata;
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            final Map<String, String> privatePublicIpAddressPairs = awsClient.getAddresses();
            if (privatePublicIpAddressPairs.isEmpty()) {
                getLogger().warning("No EC2 instances found!");
                return Collections.emptyList();
            }

            if (getLogger().isFinestEnabled()) {
                final StringBuilder sb = new StringBuilder("Found the following EC2 instances:\n");
                for (Map.Entry<String, String> entry : privatePublicIpAddressPairs.entrySet()) {
                    sb.append("    ").append(entry.getKey()).append(" : ").append(entry.getValue()).append("\n");
                }
                getLogger().finest(sb.toString());
            }

            final ArrayList<DiscoveryNode> nodes = new ArrayList<DiscoveryNode>(privatePublicIpAddressPairs.size());
            for (Map.Entry<String, String> entry : privatePublicIpAddressPairs.entrySet()) {
                for (int port = awsConfig.getHzPort().getFromPort(); port <= awsConfig.getHzPort().getToPort(); port++) {
                    nodes.add(new SimpleDiscoveryNode(new Address(entry.getKey(), port), new Address(entry.getValue(), port)));
                }
            }

            return nodes;
        } catch (Exception e) {
            LOGGER.warning("Cannot discover nodes, returning empty list", e);
            return Collections.emptyList();
        }
    }

    private String getOrNull(AwsProperties awsProperties) {
        return getOrNull(awsProperties.getDefinition());
    }
}
