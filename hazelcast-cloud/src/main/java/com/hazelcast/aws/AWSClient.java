/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.aws.impl.DescribeInstances;
import com.hazelcast.config.AwsConfig;

import java.util.Collection;
import java.util.Map;

public class AWSClient {

    private String endpoint;
    private final AwsConfig awsConfig;

    public AWSClient(AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        if (awsConfig.getAccessKey() == null && awsConfig.getIamRole() == null) {
            throw new IllegalArgumentException("AWS access key or IAM Role is required!");
        }
        if (awsConfig.getSecretKey() == null && awsConfig.getIamRole() == null) {
            throw new IllegalArgumentException("AWS secret key or Iam Role is required!");
        }
        this.awsConfig = awsConfig;
        endpoint = awsConfig.getHostHeader();
        if (awsConfig.getRegion() != null && awsConfig.getRegion().length() > 0) {
            setEndpoint("ec2." + awsConfig.getRegion() + ".amazonaws.com");
        }
    }

    public Collection<String> getPrivateIpAddresses() throws Exception {
        final Map<String, String> result = new DescribeInstances(awsConfig, endpoint).execute();
        return result.keySet();
    }

    public Map<String, String> getAddresses() throws Exception {
        return new DescribeInstances(awsConfig, endpoint).execute();
    }

    public void setEndpoint(String s) {
        this.endpoint = s;
    }

    public String getEndpoint() {
        return this.endpoint;
    }
}
