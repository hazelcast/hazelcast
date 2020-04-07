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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Responsible for connecting to AWS EC2 and ECS Instance Metadata API.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html">EC2 Instance Metatadata</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html">ECS Task Metadata</a>
 */
class AwsMetadataApi {
    private static final ILogger LOGGER = Logger.getLogger(AwsMetadataApi.class);

    private static final String EC2_METADATA_ENDPOINT = "http://169.254.169.254/latest/meta-data";
    private static final String ECS_METADATA_ENDPOINT = "http://169.254.170.2";

    private static final String SECURITY_CREDENTIALS_URI = "/iam/security-credentials/";
    private static final String AVAILABILITY_ZONE_URI = "/placement/availability-zone/";

    private final String ec2Endpoint;
    private final String ecsEndpoint;
    private final AwsConfig awsConfig;

    AwsMetadataApi(AwsConfig awsConfig) {
        this.ec2Endpoint = EC2_METADATA_ENDPOINT;
        this.ecsEndpoint = ECS_METADATA_ENDPOINT;
        this.awsConfig = awsConfig;
    }

    /**
     * For test purposes only.
     */
    AwsMetadataApi(String ec2Endpoint, String ecsEndpoint, AwsConfig awsConfig) {
        this.ec2Endpoint = ec2Endpoint;
        this.ecsEndpoint = ecsEndpoint;
        this.awsConfig = awsConfig;
    }

    String availabilityZone() {
        String uri = ec2Endpoint.concat(AVAILABILITY_ZONE_URI);
        return retrieveMetadataFromURI(uri);
    }

    String defaultIamRole() {
        String uri = ec2Endpoint.concat(SECURITY_CREDENTIALS_URI);
        return retrieveMetadataFromURI(uri);
    }

    AwsCredentials credentials(String iamRole) {
        String uri = ec2Endpoint.concat(SECURITY_CREDENTIALS_URI).concat(iamRole);
        String response = retrieveMetadataFromURI(uri);
        return parseCredentials(response);
    }

    AwsCredentials credentialsFromEcs(String relativeUrl) {
        String uri = ecsEndpoint + relativeUrl;
        String response = retrieveMetadataFromURI(uri);
        return parseCredentials(response);
    }

    private static AwsCredentials parseCredentials(String response) {
        JsonObject role = Json.parse(response).asObject();
        return AwsCredentials.builder()
            .setAccessKey(role.getString("AccessKeyId", null))
            .setSecretKey(role.getString("SecretAccessKey", null))
            .setToken(role.getString("Token", null))
            .build();
    }

    /**
     * Performs the HTTP request to retrieve AWS Instance Metadata from the given URI.
     */
    private String retrieveMetadataFromURI(final String uri) {
        return RetryUtils.retry(() -> RestClient.create(uri)
                .withConnectTimeoutSeconds(awsConfig.getConnectionTimeoutSeconds())
                .withReadTimeoutSeconds(awsConfig.getReadTimeoutSeconds())
                .get()
            , awsConfig.getConnectionRetries());
    }
}
