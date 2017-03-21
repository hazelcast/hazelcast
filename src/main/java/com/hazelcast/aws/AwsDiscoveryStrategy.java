/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static com.hazelcast.aws.AwsProperties.ACCESS_KEY;
import static com.hazelcast.aws.AwsProperties.CONNECTION_TIMEOUT_SECONDS;
import static com.hazelcast.aws.AwsProperties.HOST_HEADER;
import static com.hazelcast.aws.AwsProperties.IAM_ROLE;
import static com.hazelcast.aws.AwsProperties.PORT;
import static com.hazelcast.aws.AwsProperties.REGION;
import static com.hazelcast.aws.AwsProperties.SECRET_KEY;
import static com.hazelcast.aws.AwsProperties.SECURITY_GROUP_NAME;
import static com.hazelcast.aws.AwsProperties.TAG_KEY;
import static com.hazelcast.aws.AwsProperties.TAG_VALUE;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * AWS implementation of {@link DiscoveryStrategy}.
 *
 * @see AWSClient
 */
public class AwsDiscoveryStrategy extends AbstractDiscoveryStrategy {
    private static final ILogger LOGGER = Logger.getLogger(AwsDiscoveryStrategy.class);
    private final AWSClient aws;
    private final int port;

    public AwsDiscoveryStrategy(Map<String, Comparable> properties) {
        super(LOGGER, properties);
        this.port = getOrDefault(PORT.getDefinition(), NetworkConfig.DEFAULT_PORT);
        try {
            this.aws = new AWSClient(getAwsConfig());
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("AWS configuration is not valid", e);
        }
    }

    private AwsConfig getAwsConfig() throws IllegalArgumentException {
        final AwsConfig config = new AwsConfig()
                .setEnabled(true)
                .setAccessKey(getOrNull(ACCESS_KEY))
                .setSecretKey(getOrNull(SECRET_KEY))
                .setSecurityGroupName(getOrNull(SECURITY_GROUP_NAME))
                .setTagKey(getOrNull(TAG_KEY))
                .setTagValue(getOrNull(TAG_VALUE))
                .setIamRole(getOrNull(IAM_ROLE));

        final Integer timeout = getOrNull(CONNECTION_TIMEOUT_SECONDS.getDefinition());
        if (timeout != null) {
            config.setConnectionTimeoutSeconds(timeout);
        }

        final String region = getOrNull(REGION);
        if (region != null) {
            config.setRegion(region);
        }

        final String hostHeader = getOrNull(HOST_HEADER);
        if (hostHeader != null) {
            config.setHostHeader(hostHeader);
        }
        return config;
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            final Map<String, String> privatePublicIpAddressPairs = aws.getAddresses();
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
                nodes.add(new SimpleDiscoveryNode(new Address(entry.getKey(), port), new Address(entry.getValue(), port)));
            }

            return nodes;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private String getOrNull(AwsProperties awsProperties) {
        return getOrNull(awsProperties.getDefinition());
    }
}
