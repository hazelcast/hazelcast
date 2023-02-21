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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

class AwsEcsClient implements AwsClient {
    private static final ILogger LOGGER = Logger.getLogger(AwsClient.class);

    private final AwsEcsApi awsEcsApi;
    private final AwsEc2Api awsEc2Api;
    private final AwsMetadataApi awsMetadataApi;
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final String cluster;
    private final AwsConfig awsConfig;

    AwsEcsClient(String cluster, AwsConfig awsConfig, AwsEcsApi awsEcsApi, AwsEc2Api awsEc2Api, AwsMetadataApi awsMetadataApi,
                 AwsCredentialsProvider awsCredentialsProvider) {
        this.cluster = cluster;
        this.awsEcsApi = awsEcsApi;
        this.awsEc2Api = awsEc2Api;
        this.awsMetadataApi = awsMetadataApi;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.awsConfig = awsConfig;
    }

    @Override
    public Map<String, String> getAddresses() {
        AwsCredentials credentials = awsCredentialsProvider.credentials();
        List<String> taskAddresses = emptyList();
        if (!awsConfig.anyOfEc2PropertiesConfigured()) {
            taskAddresses = awsEcsApi.listTaskPrivateAddresses(cluster, credentials);
            LOGGER.fine(String.format("AWS ECS DescribeTasks found the following addresses: %s", taskAddresses));
        }
        if (!taskAddresses.isEmpty()) {
            return awsEc2Api.describeNetworkInterfaces(taskAddresses, credentials);
        } else if (DiscoveryMode.Client == awsConfig.getDiscoveryMode() && !awsConfig.anyOfEcsPropertiesConfigured()) {
            LOGGER.fine(String.format("No tasks found in ECS cluster: '%s'. Trying AWS EC2 Discovery.", cluster));
            return awsEc2Api.describeInstances(credentials);
        }
        return emptyMap();
    }

    @Override
    public String getAvailabilityZone() {
        return  awsMetadataApi.availabilityZoneEcs();
    }
}
