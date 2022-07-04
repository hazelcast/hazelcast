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
import com.hazelcast.spi.utils.PortRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * AWS Discovery Strategy configuration that corresponds to the properties passed in the Hazelcast configuration and listed in
 * {@link AwsProperties}.
 * <p>
 * This class is immutable.
 */
final class AwsConfig {
    private final String region;
    private final String hostHeader;
    private final String securityGroupName;
    private final List<Tag> tags;
    private final int connectionTimeoutSeconds;
    private final int connectionRetries;
    private final int readTimeoutSeconds;
    private final PortRange hzPort;
    private final String accessKey;
    private final String secretKey;
    private final String iamRole;
    private final String cluster;
    private final String family;
    private final String serviceName;

    @SuppressWarnings("checkstyle:parameternumber")
    // Constructor has a lot of parameters, but it's private.
    private AwsConfig(String accessKey, String secretKey, String region, String iamRole, String hostHeader,
                      String securityGroupName, String tagKey, String tagValue, int connectionTimeoutSeconds,
                      int connectionRetries, int readTimeoutSeconds, PortRange hzPort, String cluster, String family,
                      String serviceName) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.iamRole = iamRole;
        this.hostHeader = hostHeader;
        this.securityGroupName = securityGroupName;
        this.tags = createTags(tagKey, tagValue);
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        this.connectionRetries = connectionRetries;
        this.readTimeoutSeconds = readTimeoutSeconds;
        this.hzPort = hzPort;
        this.cluster = cluster;
        this.family = family;
        this.serviceName = serviceName;

        validateConfig();
    }

    /**
     * Creates tag key and value pairs represented by {@link Tag}.
     *
     * @param tagKeys   single or multiple tag keys separated by commas (e.g. {@code "TagKeyA,TagKeyB"}).
     * @param tagValues single or multiple tag values separated by commas (e.g. {@code "TagValueA,TagValueB"}).
     */
    private List<Tag> createTags(String tagKeys, String tagValues) {
        Iterator<String> keys = splitValue(tagKeys).iterator();
        Iterator<String> values = splitValue(tagValues).iterator();

        List<Tag> tags = new ArrayList<>();

        while (keys.hasNext() || values.hasNext()) {
            if (keys.hasNext() && values.hasNext()) {
                tags.add(new Tag(keys.next(), values.next()));
            } else if (keys.hasNext()) {
                tags.add(new Tag(keys.next(), null));
            } else {
                tags.add(new Tag(null, values.next()));
            }
        }

        return tags;
    }

    private static List<String> splitValue(String value) {
        return isNullOrEmptyAfterTrim(value) ? Collections.emptyList() : Arrays.asList(value.split(","));
    }

    private void validateConfig() {
        if (anyOfEc2PropertiesConfigured() && anyOfEcsPropertiesConfigured()) {
            throw new InvalidConfigurationException(
                "You have to configure either EC2 properties ('iam-role', 'security-group-name', 'tag-key', 'tag-value')"
                    + " or ECS properties ('cluster', 'family', 'service-name'). You cannot define both of them"
            );
        }
        if (!isNullOrEmptyAfterTrim(family) && !isNullOrEmptyAfterTrim(serviceName)) {
            throw new InvalidConfigurationException(
                "You cannot configure ECS discovery with both 'family' and 'service-name', these filters are mutually"
                    + " exclusive"
            );
        }
        if (!isNullOrEmptyAfterTrim(iamRole)
                && (!isNullOrEmptyAfterTrim(accessKey) || !isNullOrEmptyAfterTrim(secretKey))) {
            throw new InvalidConfigurationException(
                "You cannot define both 'iam-role' and 'access-key'/'secret-key'. Choose how you want to authenticate"
                    + " with AWS API, either with IAM Role or with hardcoded AWS Credentials");
        }
        if ((isNullOrEmptyAfterTrim(accessKey) && !isNullOrEmptyAfterTrim(secretKey))
                || (!isNullOrEmptyAfterTrim(accessKey) && isNullOrEmptyAfterTrim(secretKey))) {
            throw new InvalidConfigurationException(
                "You have to either define both ('access-key', 'secret-key') or none of them");
        }
    }

    private boolean anyOfEc2PropertiesConfigured() {
        return !isNullOrEmptyAfterTrim(iamRole) || !isNullOrEmptyAfterTrim(securityGroupName) || hasTags(tags);
    }

    private boolean hasTags(List<Tag> tags) {
        return tags != null && !tags.isEmpty();
    }

    private boolean anyOfEcsPropertiesConfigured() {
        return !isNullOrEmptyAfterTrim(cluster) || !isNullOrEmptyAfterTrim(family) || !isNullOrEmptyAfterTrim(serviceName);
    }

    static Builder builder() {
        return new Builder();
    }

    String getAccessKey() {
        return accessKey;
    }

    String getSecretKey() {
        return secretKey;
    }

    String getRegion() {
        return region;
    }

    String getIamRole() {
        return iamRole;
    }

    String getHostHeader() {
        return hostHeader;
    }

    String getSecurityGroupName() {
        return securityGroupName;
    }

    List<Tag> getTags() {
        return tags;
    }

    int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    int getConnectionRetries() {
        return connectionRetries;
    }

    int getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    PortRange getHzPort() {
        return hzPort;
    }

    String getCluster() {
        return cluster;
    }

    String getFamily() {
        return family;
    }

    String getServiceName() {
        return serviceName;
    }

    @Override
    public String toString() {
        return "AwsConfig{"
            + "accessKey='***'"
            + ", secretKey='***'"
            + ", iamRole='" + iamRole + '\''
            + ", region='" + region + '\''
            + ", hostHeader='" + hostHeader + '\''
            + ", securityGroupName='" + securityGroupName + '\''
            + ", tags='" + tags + '\''
            + ", hzPort=" + hzPort
            + ", cluster='" + cluster + '\''
            + ", family='" + family + '\''
            + ", serviceName='" + serviceName + '\''
            + ", connectionTimeoutSeconds=" + connectionTimeoutSeconds
            + ", connectionRetries=" + connectionRetries
            + ", readTimeoutSeconds=" + readTimeoutSeconds
            + '}';
    }

    static class Builder {

        private String accessKey;
        private String secretKey;
        private String region;
        private String iamRole;
        private String hostHeader;
        private String securityGroupName;
        private String tagKey;
        private String tagValue;
        private int connectionTimeoutSeconds;
        private int connectionRetries;
        private int readTimeoutSeconds;
        private PortRange hzPort;
        private String cluster;
        private String family;
        private String serviceName;

        Builder setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        Builder setSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        Builder setIamRole(String iamRole) {
            this.iamRole = iamRole;
            return this;
        }

        Builder setHostHeader(String hostHeader) {
            this.hostHeader = hostHeader;
            return this;
        }

        Builder setSecurityGroupName(String securityGroupName) {
            this.securityGroupName = securityGroupName;
            return this;
        }

        Builder setTagKey(String tagKey) {
            this.tagKey = tagKey;
            return this;
        }

        Builder setTagValue(String tagValue) {
            this.tagValue = tagValue;
            return this;
        }

        Builder setConnectionTimeoutSeconds(int connectionTimeoutSeconds) {
            this.connectionTimeoutSeconds = connectionTimeoutSeconds;
            return this;
        }

        Builder setConnectionRetries(int connectionRetries) {
            this.connectionRetries = connectionRetries;
            return this;
        }

        Builder setReadTimeoutSeconds(int readTimeoutSeconds) {
            this.readTimeoutSeconds = readTimeoutSeconds;
            return this;
        }

        Builder setHzPort(PortRange hzPort) {
            this.hzPort = hzPort;
            return this;
        }

        Builder setCluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        Builder setFamily(String family) {
            this.family = family;
            return this;
        }

        Builder setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        AwsConfig build() {
            return new AwsConfig(accessKey, secretKey, region, iamRole, hostHeader, securityGroupName, tagKey, tagValue,
                connectionTimeoutSeconds, connectionRetries, readTimeoutSeconds, hzPort, cluster, family, serviceName);
        }
    }
}
