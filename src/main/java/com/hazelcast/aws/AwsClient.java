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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Map;

import static com.hazelcast.aws.RegionValidator.validateRegion;

/**
 * Responsible for fetching discovery information from AWS APIs.
 */
class AwsClient {
    private static final ILogger LOGGER = Logger.getLogger(AwsClient.class);

    private static final String ECS_CREDENTIALS_ENV_VAR_NAME = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";

    private final AwsMetadataApi awsMetadataApi;
    private final AwsDescribeInstancesApi awsDescribeInstancesApi;
    private final AwsConfig awsConfig;
    private final Environment environment;

    private final String region;
    private final String endpoint;
    private final String iamRole;

    AwsClient(AwsMetadataApi awsMetadataApi, AwsDescribeInstancesApi awsDescribeInstancesApi, AwsConfig awsConfig,
              Environment environment) {
        this.awsMetadataApi = awsMetadataApi;
        this.awsDescribeInstancesApi = awsDescribeInstancesApi;
        this.awsConfig = awsConfig;
        this.environment = environment;

        this.region = resolveRegion();
        this.endpoint = resolveEndpoint();
        this.iamRole = resolveIamRole();

        validateRegion(region);
    }

    private String resolveRegion() {
        if (StringUtil.isNotEmpty(awsConfig.getRegion())) {
            return awsConfig.getRegion();
        }

        String availabilityZone = awsMetadataApi.availabilityZone();
        return availabilityZone.substring(0, availabilityZone.length() - 1);
    }

    private String resolveEndpoint() {
        if (!awsConfig.getHostHeader().startsWith("ec2.")) {
            throw new InvalidConfigurationException("HostHeader should start with \"ec2.\" prefix");
        }
        return awsConfig.getHostHeader().replace("ec2.", "ec2." + region + ".");
    }

    private String resolveIamRole() {
        if (StringUtil.isNotEmpty(awsConfig.getAccessKey())) {
            // no need to resolve IAM Role, since using hardcoded Access/Secret keys takes precedence
            return null;
        }
        if (StringUtil.isNotEmpty(awsConfig.getIamRole()) && !"DEFAULT".equals(awsConfig.getIamRole())) {
            return awsConfig.getIamRole();
        }
        return awsMetadataApi.defaultIamRole();
    }

    Map<String, String> getAddresses() {
        return awsDescribeInstancesApi.addresses(region, endpoint, prepareCredentials());
    }

    private AwsCredentials prepareCredentials() {
        if (StringUtil.isNotEmpty(awsConfig.getAccessKey())) {
            // authenticate using access key and secret key from the configuration
            return AwsCredentials.builder()
                .setAccessKey(awsConfig.getAccessKey())
                .setSecretKey(awsConfig.getSecretKey())
                .build();
        }

        if (StringUtil.isNotEmpty(iamRole)) {
            // authenticate using IAM Role
            LOGGER.info(String.format("Fetching credentials using IAM Role: %s", iamRole));
            try {
                return awsMetadataApi.credentials(iamRole);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Unable to retrieve credentials from IAM Role: "
                    + awsConfig.getIamRole(), e);
            }
        }

        // authenticate using ECS Endpoint
        // TODO: I believe this part is never executed, but it's to be sorted out while working on ECS/Fargate
        return fetchCredentialsFromEcs();
    }

    private AwsCredentials fetchCredentialsFromEcs() {
        // before giving up, attempt to discover whether we're running in an ECS Container,
        // in which case, AWS_CONTAINER_CREDENTIALS_RELATIVE_URI will exist as an env var.
        String relativePath = environment.getEnvVar(ECS_CREDENTIALS_ENV_VAR_NAME);
        if (relativePath == null) {
            throw new InvalidConfigurationException("Could not acquire credentials! "
                + "Did not find declared AWS access key or IAM Role, and could not discover IAM Task Role or default role.");
        }
        try {
            return awsMetadataApi.credentialsFromEcs(relativePath);
        } catch (Exception e) {
            throw new InvalidConfigurationException(
                "Unable to retrieve credentials from IAM Task Role. " + "URI: " + relativePath);
        }
    }

    String getAvailabilityZone() {
        return awsMetadataApi.availabilityZone();
    }
}
