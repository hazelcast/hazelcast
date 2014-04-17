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
 * This class provides the operations needed for cloud-based join mechanism.
 * Given many providers block traffic such as multicast or broadcast, we need to
 * use each provider API to look for instances that may become part of the
 * cluster.
 * <p>
 * What happens behind the scenes is that data about the running instances in a
 * specific region are downloaded using the accesskey/secretkey and necome
 * potential Hazelcast members.
 *
 * <h1>Filtering</h1> There are two mechanisms for filtering out instances. The
 * same filters can be combined (AND). <ol> <li>If a group is configured, only
 * instances within that group are selected. </li> <li> If a tag key/value is
 * configured, only instances with that tag key/value will be selected. </li>
 * </ol>
 *
 * Once Hazelcast has figured out which instances are available, it will use
 * each instance list of private IP addresse to create a TCP/IP-cluster.
 */
public class CloudConfig {

    private boolean enabled = false;
    private String provider;
    private String accessKey;
    private String secretKey;
    private String region;
    private String groupName;
    private String tagKey;
    private String tagValue;
    private int connectionTimeoutSeconds = 5;

    /**
     * Gets the provider identifier. Returns null if no provider identifier is
     * configured.
     *
     * @return the provider identifier.
     * @see #setProvider(String)
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Sets the provider identifier.
     *
     * @param provider the provider identifier.
     * @return the updated CloudConfig.
     * @throws IllegalArgumentException if provider identifier is null or empty.
     * @see #getProvider()
     * @see #setProvider(String)
     */
    public CloudConfig setProvider(String provider) {
        this.provider = hasText(provider, "provider");
        return this;
    }

    /**
     * Gets the access key to access the provider. Returns null if no access key
     * is configured.
     *
     * @return the access key.
     * @see #setAccessKey(String)
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Sets the access key to access the provider.
     *
     * @param accessKey the access key.
     * @return the updated CloudConfig.
     * @throws IllegalArgumentException if accessKey is null or empty.
     * @see #getAccessKey()
     * @see #setSecretKey(String)
     */
    public CloudConfig setAccessKey(String accessKey) {
        this.accessKey = hasText(accessKey, "accessKey");
        return this;
    }

    /**
     * Gets the secret key to access provider. Returns null if no access key is
     * configured.
     *
     * @return the secret key.
     * @see #setSecretKey(String)
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets the secret key to access the provider.
     *
     * @param secretKey the secret key
     * @return the updated CloudConfig.
     * @throw IllegalArgumentException if secretKey is null or empty.
     * @see #getSecretKey()
     * @see #setAccessKey(String)
     */
    public CloudConfig setSecretKey(String secretKey) {
        this.secretKey = hasText(secretKey, "secretKey");
        return this;
    }

    /**
     * Gets the region where the possible Hazelcast member instances are
     * running.
     *
     * @return the region
     * @see #setRegion(String)
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the region where the possible Hazelcast member instances are
     * running.
     *
     * @param region the region
     * @return the updated CloudConfig
     * @throws IllegalArgumentException if region is null or empty.
     */
    public CloudConfig setRegion(String region) {
        this.region = hasText(region, "region");
        return this;
    }

    /**
     * Enables or disables the join mechanism.
     *
     * @param enabled true if enabled, false otherwise.
     * @return the updated CloudConfig.
     */
    public CloudConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Checks if the join mechanism is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the group name. See the filtering section for more information.
     *
     * @param groupName the group name.
     * @return the updated CloudConfig.
     * @see #getGroupName()
     */
    public CloudConfig setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    /**
     * Gets the group name. If nothing has been configured, null is returned.
     *
     * @return the group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Sets the tag key. See the filtering section for more information.
     *
     * @param tagKey the tag key.
     * @return the updated CloudConfig.
     * @see #setTagKey(String)
     */
    public CloudConfig setTagKey(String tagKey) {
        this.tagKey = tagKey;
        return this;
    }

    /**
     * Sets the tag value. see the filtering section for more information.
     *
     * @param tagValue the tag value.
     * @return the updated CloudConfig.
     * @see #setTagKey(String)
     * @see #getTagValue()
     */
    public CloudConfig setTagValue(String tagValue) {
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
     * Sets the connect timeout in seconds. See
     * {@link TcpIpConfig#setConnectionTimeoutSeconds(int)} for more
     * information.
     *
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds to set
     * @return the updated CloudConfig.
     * @see #getConnectionTimeoutSeconds()
     * @see TcpIpConfig#setConnectionTimeoutSeconds(int)
     */
    public CloudConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        if (connectionTimeoutSeconds < 0) {
            throw new IllegalArgumentException("connection timeout can't be smaller than 0");
        }
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CloudConfig{");
        sb.append("enabled=").append(enabled);
        sb.append(", region='").append(region).append('\'');
        sb.append(", securityGroupName='").append(groupName).append('\'');
        sb.append(", tagKey='").append(tagKey).append('\'');
        sb.append(", tagValue='").append(tagValue).append('\'');
        sb.append(", connectionTimeoutSeconds=").append(connectionTimeoutSeconds);
        sb.append('}');
        return sb.toString();
    }

}
