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

import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;

import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 * Configuration properties for the Hazelcast Discovery Plugin for AWS. For more information
 * see {@link AwsConfig}.
 */
enum AwsProperties {

    /**
     * Access key of your account on EC2
     */
    ACCESS_KEY("access-key", STRING, true),

    /**
     * Secret key of your account on EC2
     */
    SECRET_KEY("secret-key", STRING, true),

    /**
     * The region where your members are running. Default value is us-east-1. You need to specify this if the region is other
     * than the default one.
     */
    REGION("region", STRING, true),

    /**
     * IAM roles are used to make secure requests from your clients. You can provide the name
     * of your IAM role that you created previously on your AWS console.
     */
    IAM_ROLE("iam-role", STRING, true),

    /**
     * The URL that is the entry point for a web service (the address where the EC2 API can be found).
     * Default value is ec2.amazonaws.com.
     */
    HOST_HEADER("host-header", STRING, true),

    /**
     * Name of the security group you specified at the EC2 management console. It is used to narrow the Hazelcast members to
     * be within this group. It is optional.
     */
    SECURITY_GROUP_NAME("security-group-name", STRING, true),

    /**
     * Tag key as specified in the EC2 console. It is used to narrow the members returned by the discovery mechanism.
     */
    TAG_KEY("tag-key", STRING, true),

    /**
     * Tag value as specified in the EC2 console. It is used to narrow the members returned by the discovery mechanism.
     */
    TAG_VALUE("tag-value", STRING, true),

    /**
     * Sets the connect timeout in seconds. See {@link TcpIpConfig#setConnectionTimeoutSeconds(int)} for more information.
     * Its default value is 5.
     */
    CONNECTION_TIMEOUT_SECONDS("connection-timeout-seconds", INTEGER, true),

    /**
     * Number of retries while connecting to AWS Services. Its default value is 3.
     * <p>
     * Hazelcast AWS plugin uses two AWS services: Describe Instances and EC2 Instance Metadata.
     */
    CONNECTION_RETRIES("connection-retries", INTEGER, true),

    /**
     * Sets the read timeout in seconds. Its default value is 10.
     */
    READ_TIMEOUT_SECONDS("read-timeout-seconds", INTEGER, true),

    /**
     * The discovery mechanism will discover only IP addresses. You can define the port or the port range on which Hazelcast is
     * expected to be running.
     * <p>
     * Sample values: "5701", "5701-5710".
     * <p>
     * The default value is "5701-5708".
     */
    PORT("hz-port", STRING, true);

    private final PropertyDefinition propertyDefinition;

    AwsProperties(String key, PropertyTypeConverter typeConverter, boolean optional) {
        this.propertyDefinition = new SimplePropertyDefinition(key, optional, typeConverter);
    }

    public PropertyDefinition getDefinition() {
        return propertyDefinition;
    }
}
