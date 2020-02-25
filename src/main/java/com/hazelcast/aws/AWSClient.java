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

import com.hazelcast.aws.impl.DescribeInstances;
import com.hazelcast.aws.utility.MetadataUtil;
import com.hazelcast.config.InvalidConfigurationException;

import java.util.Collection;
import java.util.Map;

public class AWSClient {

    private final AwsConfig awsConfig;

    private String endpoint;

    public AWSClient(AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        this.awsConfig = awsConfig;
        this.endpoint = awsConfig.getHostHeader();
        if (awsConfig.getRegion() != null && awsConfig.getRegion().length() > 0) {
            if (!awsConfig.getHostHeader().startsWith("ec2.")) {
                throw new InvalidConfigurationException("HostHeader should start with \"ec2.\" prefix");
            }
            setEndpoint(awsConfig.getHostHeader().replace("ec2.", "ec2." + awsConfig.getRegion() + "."));
        }
    }

    public Collection<String> getPrivateIpAddresses()
            throws Exception {
        Map<String, String> result = new DescribeInstances(awsConfig, endpoint).execute();
        return result.keySet();
    }

    public Map<String, String> getAddresses()
            throws Exception {
        return new DescribeInstances(awsConfig, endpoint).execute();
    }

    public String getAvailabilityZone() {
        return MetadataUtil.getAvailabilityZone(
                awsConfig.getConnectionTimeoutSeconds(),
                awsConfig.getConnectionRetries(),
                awsConfig.getReadTimeoutSeconds()
        );
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public void setEndpoint(String s) {
        this.endpoint = s;
    }
}
