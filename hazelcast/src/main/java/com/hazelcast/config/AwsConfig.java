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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;

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

    public AwsConfig() {
        super("aws");
    }

    public AwsConfig(AwsConfig awsConfig) {
        super(awsConfig);
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.AWS_CONFIG;
    }
}
