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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.NoCredentialsException;
import com.hazelcast.spi.exception.RestClientException;

class AwsCredentialsProvider {
    private static final ILogger LOGGER = Logger.getLogger(AwsCredentialsProvider.class);

    private static final int HTTP_NOT_FOUND = 404;

    private final AwsConfig awsConfig;
    private final AwsMetadataApi awsMetadataApi;
    private final Environment environment;
    private final String ec2IamRole;

    AwsCredentialsProvider(AwsConfig awsConfig, AwsMetadataApi awsMetadataApi, Environment environment) {
        this.awsConfig = awsConfig;
        this.awsMetadataApi = awsMetadataApi;
        this.environment = environment;
        this.ec2IamRole = resolveEc2IamRole();
    }

    private String resolveEc2IamRole() {
        if (!StringUtil.isNullOrEmptyAfterTrim(awsConfig.getAccessKey())) {
            // no need to resolve IAM Role, since using hardcoded Access/Secret keys takes precedence
            return null;
        }

        if (!StringUtil.isNullOrEmptyAfterTrim(awsConfig.getIamRole()) && !"DEFAULT".equals(awsConfig.getIamRole())) {
            return awsConfig.getIamRole();
        }

        if (environment.isRunningOnEcs()) {
            // ECS has only one role assigned and no need to resolve it here
            LOGGER.info("Using IAM Task Role attached to ECS Task");
            return null;
        }

        try {
            String ec2IamRole = awsMetadataApi.defaultIamRoleEc2();
            LOGGER.info(String.format("Using IAM Role attached to EC2 Instance: '%s'", ec2IamRole));
            return ec2IamRole;
        } catch (RestClientException e) {
            if (e.getHttpErrorCode() == HTTP_NOT_FOUND) {
                // no IAM Role attached to EC2 instance, no need to log any warning at this point
                LOGGER.finest("IAM Role not found", e);
            } else {
                LOGGER.warning("Couldn't retrieve IAM Role from EC2 instance", e);
            }
        } catch (Exception e) {
            LOGGER.warning("Couldn't retrieve IAM Role from EC2 instance", e);
        }
        return null;
    }

    AwsCredentials credentials() {
        if (!StringUtil.isNullOrEmptyAfterTrim(awsConfig.getAccessKey())) {
            return AwsCredentials.builder()
                .setAccessKey(awsConfig.getAccessKey())
                .setSecretKey(awsConfig.getSecretKey())
                .build();
        }
        if (!StringUtil.isNullOrEmptyAfterTrim(ec2IamRole)) {
            return fetchCredentialsFromEc2();
        }
        if (environment.isRunningOnEcs()) {
            return fetchCredentialsFromEcs();
        }
        throw new NoCredentialsException();
    }

    private AwsCredentials fetchCredentialsFromEc2() {
        LOGGER.fine(String.format("Fetching AWS Credentials using EC2 IAM Role: %s", ec2IamRole));

        try {
            return awsMetadataApi.credentialsEc2(ec2IamRole);
        } catch (Exception e) {
            throw new InvalidConfigurationException(String.format("Unable to retrieve credentials from IAM Role: "
                + "'%s', please make sure it's attached to your EC2 Instance", awsConfig.getIamRole()), e);
        }
    }

    private AwsCredentials fetchCredentialsFromEcs() {
        LOGGER.fine("Fetching AWS Credentials from ECS IAM Task Role");

        try {
            return awsMetadataApi.credentialsEcs();
        } catch (Exception e) {
            throw new InvalidConfigurationException("Unable to retrieve credentials from IAM Role attached to ECS Task,"
                + " please check your configuration");
        }
    }
}
