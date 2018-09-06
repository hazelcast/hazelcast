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
 * Configuration for the AWS Discovery Strategy.
 *
 * @deprecated Use {@link AliasedDiscoveryConfig} instead.
 */
@Deprecated
public class AwsConfig
        extends AliasedDiscoveryConfig {

    public String getAccessKey() {
        return getProperties().get("access-key");
    }

    public String getSecretKey() {
        return getProperties().get("secret-key");
    }

    public String getRegion() {
        return getProperties().get("region");
    }

    public String getHostHeader() {
        return getProperties().get("host-header");
    }

    public int getConnectionTimeoutSeconds() {
        return Integer.parseInt(getProperties().get("connection-timeout-seconds"));
    }

    public String getSecurityGroupName() {
        return getProperties().get("security-group-name");
    }

    public String getTagKey() {
        return getProperties().get("tag-key");
    }

    public String getTagValue() {
        return getProperties().get("tag-value");
    }

    public String getIamRole() {
        return getProperties().get("iam-role");
    }

    public String getHzPort() {
        return getProperties().get("hz-port");
    }

    public AwsConfig setAccessKey(String accessKey) {
        this.getProperties().put("access-key", checkHasText(accessKey, "accessKey must contain text"));
        return this;
    }

    public AwsConfig setSecretKey(String secretKey) {
        this.getProperties().put("secret-key", checkHasText(secretKey, "secretKey must contain text"));
        return this;
    }

    public AwsConfig setRegion(String region) {
        this.getProperties().put("region", checkHasText(region, "region must contain text"));
        return this;
    }

    public AwsConfig setHostHeader(String hostHeader) {
        this.getProperties().put("host-header", checkHasText(hostHeader, "hostHeader must contain text"));
        return this;
    }

    public AwsConfig setSecurityGroupName(String securityGroupName) {
        this.getProperties().put("security-group-name", securityGroupName);
        return this;
    }

    public AwsConfig setTagKey(String tagKey) {
        this.getProperties().put("tag-key", tagKey);
        return this;
    }

    public AwsConfig setTagValue(String tagValue) {
        this.getProperties().put("tag-value", tagValue);
        return this;
    }

    public AwsConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        if (connectionTimeoutSeconds < 0) {
            throw new IllegalArgumentException("connection timeout can't be smaller than 0");
        }
        this.getProperties().put("connection-timeout-seconds", String.valueOf(connectionTimeoutSeconds));
        return this;
    }

    public AwsConfig setIamRole(String iamRole) {
        this.getProperties().put("iam-role", iamRole);
        return this;
    }

    public AwsConfig setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        return this;
    }

    @Override
    public String getEnvironment() {
        return "aws";
    }
}
