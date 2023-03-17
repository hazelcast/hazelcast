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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.utils.RestClient;
import com.hazelcast.spi.exception.RestClientException;

import java.util.Optional;
import java.time.Instant;

import static com.hazelcast.aws.AwsRequestUtils.createRestClient;
import static com.hazelcast.spi.utils.RestClient.HTTP_NOT_FOUND;
import static com.hazelcast.spi.utils.RestClient.HTTP_OK;

/**
 * Responsible for connecting to AWS EC2 and ECS Metadata API.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html">EC2 Instance Metatadata</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html">ECS Task IAM Role Metadata</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html">ECS Task Metadata</a>
 */
class AwsMetadataApi {
    private static final ILogger LOGGER = Logger.getLogger(AwsMetadataApi.class);
    private static final String EC2_METADATA_ENDPOINT = "http://169.254.169.254/latest/meta-data";
    private static final String ECS_IAM_ROLE_METADATA_ENDPOINT = "http://169.254.170.2" + System.getenv(
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
    private static final String EC2_METADATA_TOKEN_ENDPOINT = "http://169.254.169.254/latest/api/token";
    private static final String ECS_TASK_METADATA_ENDPOINT = System.getenv("ECS_CONTAINER_METADATA_URI");

    private static final String SECURITY_CREDENTIALS_URI = "/iam/security-credentials/";
    private static final long METADATA_TOKEN_TTL_SECONDS = 21600;

    private final String ec2MetadataEndpoint;
    private final String ec2MetadataTokenEndpoint;
    private final String ecsIamRoleEndpoint;
    private final String ecsTaskMetadataEndpoint;
    private final AwsConfig awsConfig;

    private String metadataToken;
    private Instant metadataExpiry;

    AwsMetadataApi(AwsConfig awsConfig) {
        this.ec2MetadataEndpoint = EC2_METADATA_ENDPOINT;
        this.ec2MetadataTokenEndpoint = EC2_METADATA_TOKEN_ENDPOINT;
        this.ecsIamRoleEndpoint = ECS_IAM_ROLE_METADATA_ENDPOINT;
        this.ecsTaskMetadataEndpoint = ECS_TASK_METADATA_ENDPOINT;
        this.awsConfig = awsConfig;
    }

    /**
     * For test purposes only.
     */
    AwsMetadataApi(String ec2MetadataEndpoint, String ecsIamRoleEndpoint, String ecsTaskMetadataEndpoint,
                   String ec2MetadataTokenEndpoint, AwsConfig awsConfig) {
        this.ec2MetadataEndpoint = ec2MetadataEndpoint;
        this.ec2MetadataTokenEndpoint = ec2MetadataTokenEndpoint;
        this.ecsIamRoleEndpoint = ecsIamRoleEndpoint;
        this.ecsTaskMetadataEndpoint = ecsTaskMetadataEndpoint;
        this.awsConfig = awsConfig;
    }

    String availabilityZoneEc2() {
        String uri = ec2MetadataEndpoint.concat("/placement/availability-zone/");
        return metadataClient(uri, awsConfig).get().getBody();
    }

    String availabilityZoneEcs() {
        return getTaskMetadata().get("AvailabilityZone").asString();
    }

    Optional<String> placementGroupEc2() {
        return getOptionalMetadata(ec2MetadataEndpoint.concat("/placement/group-name/"), "placement group");
    }

    Optional<String> placementPartitionNumberEc2() {
        return getOptionalMetadata(ec2MetadataEndpoint.concat("/placement/partition-number/"), "partition number");
    }

    String clusterEcs() {
        return getTaskMetadata().get("Cluster").asString();
    }

    /**
     * Resolves an optional metadata that exists for some instances only.
     * HTTP_OK and HTTP_NOT_FOUND responses are assumed valid. Any other
     * response code or an exception thrown during retries will yield
     * a warning log and an empty result will be returned.
     *
     * @param uri  Metadata URI
     * @param loggedName  Metadata name to be used when logging.
     * @return  The metadata if the endpoint exists, empty otherwise.
     */
    private Optional<String> getOptionalMetadata(String uri, String loggedName) {
        RestClient.Response response;
        try {
            response = metadataClient(uri, awsConfig)
                    .expectResponseCodes(HTTP_OK, HTTP_NOT_FOUND)
                    .get();
        } catch (Exception e) {
            // Failed to get a response with code OK or NOT_FOUND after retries
            LOGGER.warning(String.format("Could not resolve the %s metadata", loggedName));
            return Optional.empty();
        }
        int responseCode = response.getCode();
        if (responseCode == HTTP_OK) {
            return Optional.of(response.getBody());
        } else if (responseCode == HTTP_NOT_FOUND) {
            LOGGER.fine(String.format("No %s information is found.", loggedName));
            return Optional.empty();
        } else {
            throw new RuntimeException(String.format("Unexpected response code: %d", responseCode));
        }
    }

    private JsonObject getTaskMetadata() {
        String uri = ecsTaskMetadataEndpoint.concat("/task");
        String response = createRestClient(uri, awsConfig).get().getBody();
        return Json.parse(response).asObject();
    }

    String defaultIamRoleEc2() {
        String uri = ec2MetadataEndpoint.concat(SECURITY_CREDENTIALS_URI);
        return metadataClient(uri, awsConfig).get().getBody();
    }

    AwsCredentials credentialsEc2(String iamRole) {
        String uri = ec2MetadataEndpoint.concat(SECURITY_CREDENTIALS_URI).concat(iamRole);
        String response = metadataClient(uri, awsConfig).get().getBody();
        return parseCredentials(response);
    }

    AwsCredentials credentialsEcs() {
        String response = createRestClient(ecsIamRoleEndpoint, awsConfig).get().getBody();
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

    RestClient metadataClient(String url, AwsConfig awsConfig) {
        try {
            return createRestClient(url, awsConfig)
                .withHeader("X-aws-ec2-metadata-token", metadataToken());
        } catch (RestClientException ignored) {
            // rest client without token
            return createRestClient(url, awsConfig);
        }
    }

    String metadataToken() {
        if (!tokenValid()) {
            metadataToken = retrieveToken();
        }
        return metadataToken;
    }

    String retrieveToken() {
        String response = createRestClient(ec2MetadataTokenEndpoint, awsConfig)
            .withHeader("X-aws-ec2-metadata-token-ttl-seconds", Long.toString(METADATA_TOKEN_TTL_SECONDS))
            .put()
            .getBody();
        // we want to refresh token before it expires
        metadataExpiry = Instant.now().plusSeconds(METADATA_TOKEN_TTL_SECONDS / 2);
        return response;
    }

    boolean tokenValid() {
        return metadataExpiry != null && metadataExpiry.isAfter(Instant.now());
    }
}
