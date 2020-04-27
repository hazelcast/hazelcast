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

import static com.hazelcast.aws.AwsRequestUtils.createRestClient;

/**
 * Responsible for connecting to AWS EC2 and ECS Metadata API.
 *
 * @see <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html">EC2 Instance Metatadata</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html">ECS Task IAM Role Metadata</a>
 * @see <a href="https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html">ECS Task Metadata</a>
 */
class AwsMetadataApi {
    private static final String EC2_METADATA_ENDPOINT = "http://169.254.169.254/latest/meta-data";
    private static final String ECS_IAM_ROLE_METADATA_ENDPOINT = "http://169.254.170.2" + System.getenv(
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
    private static final String ECS_TASK_METADATA_ENDPOINT = System.getenv("ECS_CONTAINER_METADATA_URI");

    private static final String SECURITY_CREDENTIALS_URI = "/iam/security-credentials/";

    private final String ec2MetadataEndpoint;
    private final String ecsIamRoleEndpoint;
    private final String ecsTaskMetadataEndpoint;
    private final AwsConfig awsConfig;

    AwsMetadataApi(AwsConfig awsConfig) {
        this.ec2MetadataEndpoint = EC2_METADATA_ENDPOINT;
        this.ecsIamRoleEndpoint = ECS_IAM_ROLE_METADATA_ENDPOINT;
        this.ecsTaskMetadataEndpoint = ECS_TASK_METADATA_ENDPOINT;
        this.awsConfig = awsConfig;
    }

    /**
     * For test purposes only.
     */
    AwsMetadataApi(String ec2MetadataEndpoint, String ecsIamRoleEndpoint, String ecsTaskMetadataEndpoint,
                   AwsConfig awsConfig) {
        this.ec2MetadataEndpoint = ec2MetadataEndpoint;
        this.ecsIamRoleEndpoint = ecsIamRoleEndpoint;
        this.ecsTaskMetadataEndpoint = ecsTaskMetadataEndpoint;
        this.awsConfig = awsConfig;
    }

    String availabilityZoneEc2() {
        String uri = ec2MetadataEndpoint.concat("/placement/availability-zone/");
        return createRestClient(uri, awsConfig).get();
    }

    String defaultIamRoleEc2() {
        String uri = ec2MetadataEndpoint.concat(SECURITY_CREDENTIALS_URI);
        return createRestClient(uri, awsConfig).get();
    }

    AwsCredentials credentialsEc2(String iamRole) {
        String uri = ec2MetadataEndpoint.concat(SECURITY_CREDENTIALS_URI).concat(iamRole);
        String response = createRestClient(uri, awsConfig).get();
        return parseCredentials(response);
    }

    AwsCredentials credentialsEcs() {
        String response = createRestClient(ecsIamRoleEndpoint, awsConfig).get();
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

    EcsMetadata metadataEcs() {
        String response = createRestClient(ecsTaskMetadataEndpoint, awsConfig).get();
        return parseEcsMetadata(response);
    }

    private EcsMetadata parseEcsMetadata(String response) {
        JsonObject metadata = Json.parse(response).asObject();
        JsonObject labels = metadata.get("Labels").asObject();
        String taskArn = labels.get("com.amazonaws.ecs.task-arn").asString();
        String clusterArn = labels.get("com.amazonaws.ecs.cluster").asString();
        return new EcsMetadata(taskArn, clusterArn);
    }

    static class EcsMetadata {
        private final String taskArn;
        private final String clusterArn;

        EcsMetadata(String taskArn, String clusterArn) {
            this.taskArn = taskArn;
            this.clusterArn = clusterArn;
        }

        String getTaskArn() {
            return taskArn;
        }

        String getClusterArn() {
            return clusterArn;
        }
    }
}
