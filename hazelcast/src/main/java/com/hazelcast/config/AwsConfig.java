/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import static com.hazelcast.util.Preconditions.checkHasText;

/**
 * The AWSConfig contains the configuration for AWS join mechanism.
 * <p>
 * what happens behind the scenes is that data about the running AWS instances in a specific region are downloaded using the
 * accesskey/secretkey and are potential Hazelcast members.
 * <h1>Filtering</h1>
 * There are 2 mechanisms for filtering out AWS instances and these mechanisms can be combined (AND).
 * <ol>
 * <li>If a securityGroup is configured, only instances within that security group are selected.</li>
 * <li>If a tag key/value is set, only instances with that tag key/value will be selected.</li>
 * </ol>
 * Once Hazelcast has figured out which instances are available, it will use the private IP addresses of these
 * instances to create a TCP/IP-cluster.
 */
public class AwsConfig {

    private static final int CONNECTION_TIMEOUT = 5;

    private boolean enabled;
    private String accessKey;
    private String secretKey;
    private String region = "us-east-1";
    private String securityGroupName;
    private String tagKey;
    private String tagValue;
    private String hostHeader = "ec2.amazonaws.com";
    private String iamRole;
    private int connectionTimeoutSeconds = CONNECTION_TIMEOUT;

    /**
     * Gets the access key to access AWS. Returns null if no access key is configured.
     *
     * @return the access key to access AWS
     * @see #setAccessKey(String)
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Sets the access key to access AWS.
     *
     * @param accessKey the access key to access AWS
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if accessKey is {@code null} or empty
     * @see #getAccessKey()
     * @see #setSecretKey(String)
     */
    public AwsConfig setAccessKey(String accessKey) {
        this.accessKey = checkHasText(accessKey, "accessKey must contain text");
        return this;
    }

    /**
     * Gets the secret key to access AWS. Returns {@code null} if no access key is configured.
     *
     * @return the secret key
     * @see #setSecretKey(String)
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets the secret key to access AWS.
     *
     * @param secretKey the secret key to access AWS
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if secretKey is {@code null} or empty
     * @see #getSecretKey()
     * @see #setAccessKey(String)
     */
    public AwsConfig setSecretKey(String secretKey) {
        this.secretKey = checkHasText(secretKey, "secretKey must contain text");
        return this;
    }

    /**
     * Gets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @return the region where the EC2 instances running the Hazelcast members will be running
     * @see #setRegion(String)
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @param region the region where the EC2 instances running the Hazelcast members will be running
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if region is {@code null} or empty
     */
    public AwsConfig setRegion(String region) {
        this.region = checkHasText(region, "region must contain text");
        return this;
    }

    /**
     * Gets the host header; the address where the EC2 API can be found.
     *
     * @return the host header; the address where the EC2 API can be found
     */
    public String getHostHeader() {
        return hostHeader;
    }

    /**
     * Sets the host header; the address where the EC2 API can be found.
     *
     * @param hostHeader the new host header; the address where the EC2 API can be found
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if hostHeader is {@code null} or an empty string
     */
    public AwsConfig setHostHeader(String hostHeader) {
        this.hostHeader = checkHasText(hostHeader, "hostHeader must contain text");
        return this;
    }

    /**
     * Enables or disables the AWS join mechanism.
     *
     * @param enabled {@code true} if enabled, {@code false} otherwise
     * @return the updated AwsConfig
     */
    public AwsConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Checks if the AWS join mechanism is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the security group name. See the filtering section above for more information.
     *
     * @param securityGroupName the security group name
     * @return the updated AwsConfig
     * @see #getSecurityGroupName()
     */
    public AwsConfig setSecurityGroupName(String securityGroupName) {
        this.securityGroupName = securityGroupName;
        return this;
    }

    /**
     * Gets the security group name. If nothing has been configured, {@code null} is returned.
     *
     * @return the security group name; {@code null} if nothing has been configured
     */
    public String getSecurityGroupName() {
        return securityGroupName;
    }

    /**
     * Sets the tag key. See the filtering section above for more information.
     *
     * @param tagKey the tag key (see the filtering section above for more information)
     * @return the updated AwsConfig
     * @see #setTagKey(String)
     */
    public AwsConfig setTagKey(String tagKey) {
        this.tagKey = tagKey;
        return this;
    }

    /**
     * Sets the tag value. See the filtering section above for more information.
     *
     * @param tagValue the tag value (see the filtering section above for more information)
     * @return the updated AwsConfig
     * @see #setTagKey(String)
     * @see #getTagValue()
     */
    public AwsConfig setTagValue(String tagValue) {
        this.tagValue = tagValue;
        return this;
    }

    /**
     * Gets the tag key. If nothing is specified, {@code null} is returned.
     *
     * @return the tag key or {@code null} if nothing is returned
     */
    public String getTagKey() {
        return tagKey;
    }

    /**
     * Gets the tag value. If nothing is specified, {@code null} is returned.
     *
     * @return the tag value or {@code null} if nothing is returned
     */
    public String getTagValue() {
        return tagValue;
    }

    /**
     * Gets the connection timeout in seconds.
     *
     * @return the connectionTimeoutSeconds; connection timeout in seconds
     * @see #setConnectionTimeoutSeconds(int)
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * Sets the connect timeout in seconds. See {@link TcpIpConfig#setConnectionTimeoutSeconds(int)} for more information.
     *
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds (connection timeout in seconds) to set
     * @return the updated AwsConfig
     * @see #getConnectionTimeoutSeconds()
     * @see TcpIpConfig#setConnectionTimeoutSeconds(int)
     */
    public AwsConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        if (connectionTimeoutSeconds < 0) {
            throw new IllegalArgumentException("connection timeout can't be smaller than 0");
        }
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    /**
     * Gets the iamRole name
     *
     * @return the iamRole or {@code null} if nothing is returned
     * @see #setIamRole(String) (int)
     */
    public String getIamRole() {
        return iamRole;
    }

    /**
     * Sets the tag value. See the filtering section above for more information.
     *
     * @param iamRole the IAM Role name
     * @return the updated AwsConfig
     * @see #getIamRole()
     */
    public AwsConfig setIamRole(String iamRole) {
        this.iamRole = iamRole;
        return this;
    }

    @Override
    public String toString() {
        return "AwsConfig{"
                + "enabled=" + enabled
                + ", region='" + region + '\''
                + ", securityGroupName='" + securityGroupName + '\''
                + ", tagKey='" + tagKey + '\''
                + ", tagValue='" + tagValue + '\''
                + ", hostHeader='" + hostHeader + '\''
                + ", iamRole='" + iamRole + '\''
                + ", connectionTimeoutSeconds=" + connectionTimeoutSeconds + '}';
    }
}
