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

public class AwsConfig {
    private boolean enabled = false;

    private String accessKey;
    private String secretKey;
    private String region;
    private String securityGroupName;
    private String tagKey;
    private String tagValue;
    private String hostHeader = "ec2.amazonaws.com";
    private int connectionTimeoutSeconds = 5;

    public void setSecurityGroupName(String securityGroupName) {
        this.securityGroupName = securityGroupName;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public AwsConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public AwsConfig setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getHostHeader() {
        return hostHeader;
    }

    public void setHostHeader(String hostHeader) {
        this.hostHeader = hostHeader;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getSecurityGroupName() {
        return securityGroupName;
    }

    public void setTagKey(String tagKey) {
        this.tagKey = tagKey;
    }

    public void setTagValue(String tagValue) {
        this.tagValue = tagValue;
    }

    public String getTagKey() {
        return tagKey;
    }

    public String getTagValue() {
        return tagValue;
    }

    /**
     * @return the connectionTimeoutSeconds
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds to set
     */
    public AwsConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    @Override
    public String toString() {
        return "AwsConfig{" +
                "enabled=" + enabled +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", region='" + region + '\'' +
                ", hostHeader='" + hostHeader + '\'' +
                ", securityGroupName=" + securityGroupName +
                '}';
    }
}
