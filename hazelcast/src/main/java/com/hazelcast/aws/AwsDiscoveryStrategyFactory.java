/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.utils.RestClient;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Factory class which returns {@link AwsDiscoveryStrategy} to Discovery SPI
 */
public class AwsDiscoveryStrategyFactory
        implements DiscoveryStrategyFactory {
    private static final ILogger LOGGER = Logger.getLogger(AwsDiscoveryStrategyFactory.class);

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return AwsDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                  Map<String, Comparable> properties) {
        return new AwsDiscoveryStrategy(discoveryNode, properties);
    }

    @Override
    public Collection<PropertyDefinition> getConfigurationProperties() {
        final AwsProperties[] props = AwsProperties.values();
        final ArrayList<PropertyDefinition> definitions = new ArrayList<>(props.length);
        for (AwsProperties prop : props) {
            definitions.add(prop.getDefinition());
        }
        return definitions;
    }

    /**
     * Checks if Hazelcast is running on an AWS EC2 instance.
     * <p>
     * Note that this method returns {@code false} for any ECS environment, since currently there is no way to autoconfigure
     * Hazelcast network interfaces (required for ECS).
     * <p>
     * To check if Hazelcast is running on EC2, we first check that the machine uuid starts with "ec2" or "EC2". There is
     * a small chance that a non-AWS machine has uuid starting with the mentioned prefix. That is why, to be sure, we make
     * an API call to a local, non-routable address http://169.254.169.254/latest/dynamic/instance-identity/. Finally, we also
     * check if an IAM Role is attached to the EC2 instance, because without any IAM Role the Hazelcast AWS discovery won't work.
     *
     * @return true if running on EC2 Instance which has an IAM Role attached
     * @see <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/identify_ec2_instances.html">AWS Docs</a>
     */
    @Override
    public boolean isAutoDetectionApplicable() {
        return isRunningOnEc2() && !isRunningOnEcs();
    }

    private static boolean isRunningOnEc2() {
        return uuidWithEc2Prefix() && instanceIdentityExists() && iamRoleAttached();
    }

    private static boolean uuidWithEc2Prefix() {
        String uuidPath = "/sys/hypervisor/uuid";
        if (new File(uuidPath).exists()) {
            String uuid = readFileContents(uuidPath);
            return uuid.startsWith("ec2") || uuid.startsWith("EC2");
        }
        return false;
    }

    static String readFileContents(String fileName) {
        try {
            File file = new File(fileName);
            return Files.readString(file.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Could not get " + fileName, e);
        }
    }

    private static boolean instanceIdentityExists() {
        return isEndpointAvailable("http://169.254.169.254/latest/dynamic/instance-identity/");
    }

    private static boolean iamRoleAttached() {
        try {
            return isEndpointAvailable("http://169.254.169.254/latest/meta-data/iam/security-credentials/");
        } catch (Exception e) {
            LOGGER.warning("Hazelcast running on EC2 instance, but no IAM Role attached. Cannot use Hazelcast AWS discovery.");
            LOGGER.finest(e);
            return false;
        }
    }

    static boolean isEndpointAvailable(String url) {
        return !RestClient.create(url, 1)
                .withRequestTimeoutSeconds(1)
                .withRetries(1)
                .get()
                .getBody()
                .isEmpty();
    }

    private static boolean isRunningOnEcs() {
        return new Environment().isRunningOnEcs();
    }

    @Override
    public DiscoveryStrategyLevel discoveryStrategyLevel() {
        return DiscoveryStrategyLevel.CLOUD_VM;
    }
}
