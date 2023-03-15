/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;

class AwsEc2Client implements AwsClient {

    private static final ILogger LOGGER = Logger.getLogger(AwsEc2Client.class);
    private final AwsEc2Api awsEc2Api;
    private final AwsEcsApi awsEcsApi;
    private final AwsMetadataApi awsMetadataApi;
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final AwsConfig awsConfig;

    AwsEc2Client(AwsEc2Api awsEc2Api, AwsEcsApi awsEcsApi, AwsMetadataApi awsMetadataApi,
                 AwsCredentialsProvider awsCredentialsProvider, AwsConfig awsConfig) {
        this.awsEc2Api = awsEc2Api;
        this.awsEcsApi = awsEcsApi;
        this.awsMetadataApi = awsMetadataApi;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.awsConfig = awsConfig;
    }

    @Override
    public Map<String, String> getAddresses() {
        AwsCredentials credentials = awsCredentialsProvider.credentials();
        Map<String, String> instances = Collections.emptyMap();
        if (!awsConfig.anyOfEcsPropertiesConfigured()) {
            instances = awsEc2Api.describeInstances(credentials);
        }
        if (awsConfig.anyOfEc2PropertiesConfigured()) {
            return instances;
        }
        if (instances.isEmpty() && DiscoveryMode.Client == awsConfig.getDiscoveryMode()) {
            return getEcsAddresses(credentials);
        }
        return instances;
    }

    private Map<String, String> getEcsAddresses(AwsCredentials credentials) {
        if (isNullOrEmptyAfterTrim(awsConfig.getCluster())) {
            throw new InvalidConfigurationException("You must define 'cluster' property if not running inside ECS cluster");
        }
        List<String> taskAddresses = awsEcsApi.listTaskPrivateAddresses(awsConfig.getCluster(), credentials);
        LOGGER.fine(String.format("AWS ECS DescribeTasks found the following addresses: %s", taskAddresses));
        return awsEc2Api.describeNetworkInterfaces(taskAddresses, credentials);
    }

    @Override
    public String getAvailabilityZone() {
        return awsMetadataApi.availabilityZoneEc2();
    }

    @Override
    public Optional<String> getPlacementGroup() {
        return awsMetadataApi.placementGroupEc2();
    }

    @Override
    public Optional<String> getPlacementPartitionNumber() {
        return awsMetadataApi.placementPartitionNumberEc2();
    }
}
