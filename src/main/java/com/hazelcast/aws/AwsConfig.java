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

/**
 * AWS Discovery Strategy configuration that corresponds to the properties passed in the Hazelcast configuration and listed in
 * {@link AwsProperties}.
 */
public final class AwsConfig {
    private final String region;
    private final String hostHeader;
    private final String securityGroupName;
    private final String tagKey;
    private final String tagValue;
    private final int connectionTimeoutSeconds;
    private final int connectionRetries;
    private final int readTimeoutSeconds;
    private final PortRange hzPort;
    private String accessKey;
    private String secretKey;
    private String iamRole;

    @SuppressWarnings("checkstyle:parameternumber")
    // Constructor has a lot of parameters, but it's private.
    private AwsConfig(String accessKey, String secretKey, String region, String iamRole, String hostHeader,
                      String securityGroupName, String tagKey, String tagValue, int connectionTimeoutSeconds,
                      int connectionRetries, int readTimeoutSeconds, PortRange hzPort) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.iamRole = iamRole;
        this.hostHeader = hostHeader;
        this.securityGroupName = securityGroupName;
        this.tagKey = tagKey;
        this.tagValue = tagValue;
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        this.connectionRetries = connectionRetries;
        this.readTimeoutSeconds = readTimeoutSeconds;
        this.hzPort = hzPort;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Sets {@code accessKey}.
     *
     * @deprecated It violates the immutability of {@link AwsConfig}.
     */
    @Deprecated
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets {@code secretKey}.
     *
     * @deprecated It violates the immutability of {@link AwsConfig}.
     */
    @Deprecated
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getRegion() {
        return region;
    }

    public String getIamRole() {
        return iamRole;
    }

    /**
     * Sets {@code iamRole}.
     *
     * @deprecated It violates the immutability of {@link AwsConfig}.
     */
    @Deprecated
    public void setIamRole(String iamRole) {
        this.iamRole = iamRole;
    }

    public String getHostHeader() {
        return hostHeader;
    }

    public String getSecurityGroupName() {
        return securityGroupName;
    }

    public String getTagKey() {
        return tagKey;
    }

    public String getTagValue() {
        return tagValue;
    }

    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    public int getConnectionRetries() {
        return connectionRetries;
    }

    public int getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public PortRange getHzPort() {
        return hzPort;
    }

    @Override
    public String toString() {
        return "AwsConfig{" + "accessKey='***', secretKey='***', region='" + region + '\'' + ", iamRole='" + iamRole + '\''
                + ", hostHeader='" + hostHeader + '\'' + ", securityGroupName='" + securityGroupName + '\'' + ", tagKey='"
                + tagKey + '\'' + ", tagValue='" + tagValue + '\'' + ", connectionTimeoutSeconds=" + connectionTimeoutSeconds
                + ", readTimeoutSeconds=" + readTimeoutSeconds + ", connectionRetries=" + connectionRetries
                + ", hzPort=" + hzPort + '}';
    }

    public static class Builder {
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

        public Builder setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder setSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder setIamRole(String iamRole) {
            this.iamRole = iamRole;
            return this;
        }

        public Builder setHostHeader(String hostHeader) {
            this.hostHeader = hostHeader;
            return this;
        }

        public Builder setSecurityGroupName(String securityGroupName) {
            this.securityGroupName = securityGroupName;
            return this;
        }

        public Builder setTagKey(String tagKey) {
            this.tagKey = tagKey;
            return this;
        }

        public Builder setTagValue(String tagValue) {
            this.tagValue = tagValue;
            return this;
        }

        public Builder setConnectionTimeoutSeconds(int connectionTimeoutSeconds) {
            this.connectionTimeoutSeconds = connectionTimeoutSeconds;
            return this;
        }

        public Builder setConnectionRetries(int connectionRetries) {
            this.connectionRetries = connectionRetries;
            return this;
        }

        public Builder setReadTimeoutSeconds(int readTimeoutSeconds) {
            this.readTimeoutSeconds = readTimeoutSeconds;
            return this;
        }

        public Builder setHzPort(PortRange hzPort) {
            this.hzPort = hzPort;
            return this;
        }

        public AwsConfig build() {
            return new AwsConfig(accessKey, secretKey, region, iamRole, hostHeader, securityGroupName, tagKey, tagValue,
                    connectionTimeoutSeconds, connectionRetries, readTimeoutSeconds, hzPort);
        }
    }
}
