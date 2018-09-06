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

package com.hazelcast.client.config;

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AwsConfig;

/**
 * The AWSConfig contains the configuration for client to connect to nodes in aws environment.
 *
 * @deprecated Use {@link AliasedDiscoveryConfig} instead.
 */
@Deprecated
public class ClientAwsConfig extends AwsConfig {
    private boolean insideAws;

    /**
     * If client is inside aws, it will use private ip addresses directly,
     * otherwise it will convert private ip addresses to public addresses
     * internally by calling AWS API.
     *
     * @return boolean true if client is inside aws environment.
     */
    public boolean isInsideAws() {
        return insideAws;
    }

    /**
     * Set to true if client is inside aws environment
     * Default value is false.
     *
     * @param insideAws isInsideAws
     */
    public ClientAwsConfig setInsideAws(boolean insideAws) {
        this.insideAws = insideAws;
        return this;
    }
}
