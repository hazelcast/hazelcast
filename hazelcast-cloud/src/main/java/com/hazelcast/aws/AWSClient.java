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
        if (awsConfig.getAccessKey() == null) {
            throw new IllegalArgumentException("AWS access key is required!");
        }
        if (awsConfig.getSecretKey() == null) {
            throw new IllegalArgumentException("AWS secret key is required!");
        }
        this.awsConfig = awsConfig;
        endpoint = awsConfig.getHostHeader();
    }

    public Collection<String> getPrivateIpAddresses() throws Exception {
        final Map<String, String> result = new DescribeInstances(awsConfig).execute();
        return result.keySet();
    }

    public Map<String, String> getAddresses() throws Exception {
        return new DescribeInstances(awsConfig).execute();
    }

    public void setEndpoint(String s) {
        this.endpoint = s;
    }
}
