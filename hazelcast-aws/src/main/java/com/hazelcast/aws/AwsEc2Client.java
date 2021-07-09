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

import java.util.Map;
import java.util.Optional;

class AwsEc2Client implements AwsClient {
    private final AwsEc2Api awsEc2Api;
    private final AwsMetadataApi awsMetadataApi;
    private final AwsCredentialsProvider awsCredentialsProvider;

    AwsEc2Client(AwsEc2Api awsEc2Api, AwsMetadataApi awsMetadataApi, AwsCredentialsProvider awsCredentialsProvider) {
        this.awsEc2Api = awsEc2Api;
        this.awsMetadataApi = awsMetadataApi;
        this.awsCredentialsProvider = awsCredentialsProvider;
    }

    @Override
    public Map<String, String> getAddresses() {
        return awsEc2Api.describeInstances(awsCredentialsProvider.credentials());
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
