/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.ValidationUtil.hasText;

/**
 * The AWSConfig contains the configuration for AWS join mechanism.
 * <p/>
 * what happens behind the scenes is that data about the running AWS instances in a specific region are downloaded using the
 * accesskey/secretkey and are potential Hazelcast members.
 * <p/>
 * <h1>Filtering</h1>
 * There are 2 mechanisms for filtering out AWS instances and these mechanisms can be combined (AND).
 * <ol>
 * <li>If a securityGroup is configured only instanced within that security group are selected.
 * </li>
 * <li>
 * If a tag key/value is set only instances with that tag key/value will be selected.
 * </li>
 * </ol>
 * <p/>
 * Once Hazelcast has figured out which instances are available, it will use the private ip addresses of these
 * instances to create a tcp/ip-cluster.
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
    private int connectionTimeoutSeconds = CONNECTION_TIMEOUT;

    /**
     * Gets the access key to access AWS. Returns null if no access key is configured.
     *
     * @return the access key.
     * @see #setAccessKey(String)
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Sets the access key to access AWS.
     *
     * @param accessKey the access key.
     * @return the updated AwsConfig.
     * @throws IllegalArgumentException if accessKey is null or empty.
     * @see #getAccessKey()
     * @see #setSecretKey(String)
     */
    public AwsConfig setAccessKey(String accessKey) {
        this.accessKey = hasText(accessKey, "accessKey");
        return this;
    }

    /**
     * Gets the secret key to access AWS. Returns null if no access key is configured.
     *
     * @return the secret key.
     * @see #setSecretKey(String)
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets the secret key to access AWS.
     *
     * @param secretKey the secret key
     * @return the updated AwsConfig.
     * @throw IllegalArgumentException if secretKey is null or empty.
     * @see #getSecretKey()
     * @see #setAccessKey(String)
     */
    public AwsConfig setSecretKey(String secretKey) {
        this.secretKey = hasText(secretKey, "secretKey");
        return this;
    }

    /**
     * Gets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @return the region
     * @see #setRegion(String)
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @param region the region
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if region is null or empty.
     */
    public AwsConfig setRegion(String region) {
        this.region = hasText(region, "region");
        return this;
    }

    /**
     * Gets the host header; the address the EC2 API can be found.
     *
     * @return the host header.
     */
    public String getHostHeader() {
        return hostHeader;
    }

    /**
     * Sets the host header; the address the EC2 API can be found.
     *
     * @param hostHeader the new host header
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if hostHeader is null or an empty string.
     */
    public AwsConfig setHostHeader(String hostHeader) {
        this.hostHeader = hasText(hostHeader, "hostHeader");
        return this;
    }

    /**
     * Enables or disables the aws join mechanism.
     *
     * @param enabled true if enabled, false otherwise.
     * @return the updated AwsConfig.
     */
    public AwsConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Checks if the aws join mechanism is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the security group name. See the filtering section for more information.
     *
     * @param securityGroupName the security group name.
     * @return the updated AwsConfig.
     * @see #getSecurityGroupName()
     */
    public AwsConfig setSecurityGroupName(String securityGroupName) {
        this.securityGroupName = securityGroupName;
        return this;
    }

    /**
     * Gets the security group name. If nothing has been configured, null is returned.
     *
     * @return the security group name
     */
    public String getSecurityGroupName() {
        return securityGroupName;
    }

    /**
     * Sets the tag key. See the filtering section for more information.
     *
     * @param tagKey the tag key.
     * @return the updated AwsConfig.
     * @see #setTagKey(String)
     */
    public AwsConfig setTagKey(String tagKey) {
        this.tagKey = tagKey;
        return this;
    }

    /**
     * Sets the tag value. see the filtering section for more information.
     *
     * @param tagValue the tag value.
     * @return the updated AwsConfig.
     * @see #setTagKey(String)
     * @see #getTagValue()
     */
    public AwsConfig setTagValue(String tagValue) {
        this.tagValue = tagValue;
        return this;
    }

    /**
     * Gets the tag key; if nothing is specified null is returned.
     *
     * @return the tag key.
     */
    public String getTagKey() {
        return tagKey;
    }

    /**
     * Gets the tag value. If nothing is specified null is returned.
     *
     * @return the tag value.
     */
    public String getTagValue() {
        return tagValue;
    }

    /**
     * Gets the connection timeout in seconds.
     *
     * @return the connectionTimeoutSeconds
     * @see #setConnectionTimeoutSeconds(int)
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * Sets the connect timeout in seconds. See {@link TcpIpConfig#setConnectionTimeoutSeconds(int)} for more information.
     *
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds to set
     * @return the updated AwsConfig.
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AwsConfig{");
        sb.append("enabled=").append(enabled);
        sb.append(", region='").append(region).append('\'');
        sb.append(", securityGroupName='").append(securityGroupName).append('\'');
        sb.append(", tagKey='").append(tagKey).append('\'');
        sb.append(", tagValue='").append(tagValue).append('\'');
        sb.append(", hostHeader='").append(hostHeader).append('\'');
        sb.append(", connectionTimeoutSeconds=").append(connectionTimeoutSeconds);
        sb.append('}');
        return sb.toString();
    }
}
