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

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains the 3 different join configurations; tcp-ip/multicast/cloud. Only
 * one of them should be enabled!
 */
public class JoinConfig {

    private MulticastConfig multicastConfig = new MulticastConfig();

    private TcpIpConfig tcpIpConfig = new TcpIpConfig();

    private CloudConfig cloudConfig = new CloudConfig();

    /**
     * @return the multicastConfig
     */
    public MulticastConfig getMulticastConfig() {
        return multicastConfig;
    }

    /**
     * @param multicastConfig the multicastConfig to set
     * @throws IllegalArgumentException if multicastConfig is null.
     */
    public JoinConfig setMulticastConfig(final MulticastConfig multicastConfig) {
        this.multicastConfig = isNotNull(multicastConfig, "multicastConfig");
        return this;
    }

    /**
     * @return the tcpIpConfig
     */
    public TcpIpConfig getTcpIpConfig() {
        return tcpIpConfig;
    }

    /**
     * @param tcpIpConfig the tcpIpConfig to set
     * @throws IllegalArgumentException if tcpIpConfig is null.
     */
    public JoinConfig setTcpIpConfig(final TcpIpConfig tcpIpConfig) {
        this.tcpIpConfig = isNotNull(tcpIpConfig,"tcpIpConfig");
        return this;
    }

    /**
     * @return the cloudConfig
     */
    public CloudConfig getCloudConfig() {
        return cloudConfig;
    }

    /**
     * @param cloudConfig the CloudConfig to set
     * @throws IllegalArgumentException if cloudConfig is null.
     */
    public JoinConfig setCloudConfig(final CloudConfig cloudConfig) {
        this.cloudConfig = isNotNull(cloudConfig, "cloudConfig");
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JoinConfig{");
        sb.append("multicastConfig=").append(multicastConfig);
        sb.append(", tcpIpConfig=").append(tcpIpConfig);
        sb.append(", cloudConfig=").append(cloudConfig);
        sb.append('}');
        return sb.toString();
    }
}
