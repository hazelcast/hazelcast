/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
public class AwsConfig
        extends AliasedDiscoveryConfig<AwsConfig> {
    private static final int CONNECTION_TIMEOUT = 5;

    public AwsConfig() {
        super("aws");
    }

    public AwsConfig(AwsConfig awsConfig) {
        super(awsConfig);
    }

    /**
     * Gets the access key to access AWS. Returns null if no access key is configured.
     *
     * @return the access key to access AWS
     * @see #setAccessKey(String)
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getAccessKey() {
        return getProperties().get("access-key");
    }

    /**
     * Sets the access key to access AWS.
     *
     * @param accessKey the access key to access AWS
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if accessKey is {@code null} or empty
     * @see #getAccessKey()
     * @see #setSecretKey(String)
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setAccessKey(String accessKey) {
        this.getProperties().put("access-key", checkHasText(accessKey, "accessKey must contain text"));
        return this;
    }

    /**
     * Gets the secret key to access AWS. Returns {@code null} if no access key is configured.
     *
     * @return the secret key
     * @see #setSecretKey(String)
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getSecretKey() {
        return getProperties().get("secret-key");
    }

    /**
     * Sets the secret key to access AWS.
     *
     * @param secretKey the secret key to access AWS
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if secretKey is {@code null} or empty
     * @see #getSecretKey()
     * @see #setAccessKey(String)
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setSecretKey(String secretKey) {
        this.getProperties().put("secret-key", checkHasText(secretKey, "secretKey must contain text"));
        return this;
    }

    /**
     * Gets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @return the region where the EC2 instances running the Hazelcast members will be running
     * @see #setRegion(String)
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getRegion() {
        return getProperties().get("region");
    }

    /**
     * Sets the region where the EC2 instances running the Hazelcast members will be running.
     *
     * @param region the region where the EC2 instances running the Hazelcast members will be running
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if region is {@code null} or empty
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setRegion(String region) {
        this.getProperties().put("region", checkHasText(region, "region must contain text"));
        return this;
    }

    /**
     * Gets the host header; the address where the EC2 API can be found.
     *
     * @return the host header; the address where the EC2 API can be found
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getHostHeader() {
        return getProperties().get("host-header");
    }

    /**
     * Sets the host header; the address where the EC2 API can be found.
     *
     * @param hostHeader the new host header; the address where the EC2 API can be found
     * @return the updated AwsConfig
     * @throws IllegalArgumentException if hostHeader is {@code null} or an empty string
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setHostHeader(String hostHeader) {
        this.getProperties().put("host-header", checkHasText(hostHeader, "hostHeader must contain text"));
        return this;
    }

    /**
     * Gets the security group name. If nothing has been configured, {@code null} is returned.
     *
     * @return the security group name; {@code null} if nothing has been configured
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getSecurityGroupName() {
        return getProperties().get("security-group-name");
    }

    /**
     * Sets the security group name. See the filtering section above for more information.
     *
     * @param securityGroupName the security group name
     * @return the updated AwsConfig
     * @see #getSecurityGroupName()
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setSecurityGroupName(String securityGroupName) {
        this.getProperties().put("security-group-name", securityGroupName);
        return this;
    }

    /**
     * Gets the tag key. If nothing is specified, {@code null} is returned.
     *
     * @return the tag key or {@code null} if nothing is returned
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getTagKey() {
        return getProperties().get("tag-key");
    }

    /**
     * Sets the tag key. See the filtering section above for more information.
     *
     * @param tagKey the tag key (see the filtering section above for more information)
     * @return the updated AwsConfig
     * @see #setTagKey(String)
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    public AwsConfig setTagKey(String tagKey) {
        this.getProperties().put("tag-key", tagKey);
        return this;
    }

    /**
     * Gets the tag value. If nothing is specified, {@code null} is returned.
     *
     * @return the tag value or {@code null} if nothing is returned
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getTagValue() {
        return getProperties().get("tag-value");
    }

    /**
     * Sets the tag value. See the filtering section above for more information.
     *
     * @param tagValue the tag value (see the filtering section above for more information)
     * @return the updated AwsConfig
     * @see #setTagKey(String)
     * @see #getTagValue()
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setTagValue(String tagValue) {
        this.getProperties().put("tag-value", tagValue);
        return this;
    }

    /**
     * Gets the connection timeout in seconds.
     *
     * @return the connectionTimeoutSeconds; connection timeout in seconds
     * @see #setConnectionTimeoutSeconds(int)
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public int getConnectionTimeoutSeconds() {
        if (!getProperties().containsKey("connection-timeout-seconds")) {
            return CONNECTION_TIMEOUT;
        }
        return Integer.parseInt(getProperties().get("connection-timeout-seconds"));
    }

    /**
     * Sets the connect timeout in seconds. See {@link TcpIpConfig#setConnectionTimeoutSeconds(int)} for more information.
     *
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds (connection timeout in seconds) to set
     * @return the updated AwsConfig
     * @see #getConnectionTimeoutSeconds()
     * @see TcpIpConfig#setConnectionTimeoutSeconds(int)
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        if (connectionTimeoutSeconds < 0) {
            throw new IllegalArgumentException("connection timeout can't be smaller than 0");
        }
        this.getProperties().put("connection-timeout-seconds", String.valueOf(connectionTimeoutSeconds));
        return this;
    }

    /**
     * Gets the iamRole name
     *
     * @return the iamRole or {@code null} if nothing is returned
     * @see #setIamRole(String) (int)
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getIamRole() {
        return getProperties().get("iam-role");
    }

    /**
     * Sets the tag value. See the filtering section above for more information.
     *
     * @param iamRole the IAM Role name
     * @return the updated AwsConfig
     * @see #getIamRole()
     * @deprecated use {@link AliasedDiscoveryConfig#setProperty(String, String)} instead
     */
    @Deprecated
    public AwsConfig setIamRole(String iamRole) {
        this.getProperties().put("iam-role", iamRole);
        return this;
    }

    /**
     * Gets Hazelcast port.
     *
     * @deprecated Use {@link AliasedDiscoveryConfig#getProperty(String)} instead.
     */
    @Deprecated
    public String getHzPort() {
        return getProperties().get("hz-port");
    }

    /**
     * Enables or disables the AWS join mechanism.
     *
     * @param enabled {@code true} if enabled, {@code false} otherwise
     * @return the updated AwsConfig
     */
    @Override
    public AwsConfig setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        return this;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.AWS_CONFIG;
    }
}
