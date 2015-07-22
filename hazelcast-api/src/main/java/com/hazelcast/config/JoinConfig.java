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

package com.hazelcast.config;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the 3 different join configurations; tcp-ip/multicast/aws. Only one of them should be enabled!
 */
public class JoinConfig {

    private MulticastConfig multicastConfig = new MulticastConfig();

    private TcpIpConfig tcpIpConfig = new TcpIpConfig();

    private AwsConfig awsConfig = new AwsConfig();

    /**
     * @return the multicastConfig join configuration
     */
    public MulticastConfig getMulticastConfig() {
        return multicastConfig;
    }

    /**
     * @param multicastConfig the multicastConfig join configuration to set
     * @throws IllegalArgumentException if multicastConfig is null
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
     * @param tcpIpConfig the tcpIpConfig join configuration to set
     * @throws IllegalArgumentException if tcpIpConfig is null
     */
    public JoinConfig setTcpIpConfig(final TcpIpConfig tcpIpConfig) {
        this.tcpIpConfig = isNotNull(tcpIpConfig, "tcpIpConfig");
        return this;
    }

    /**
     * @return the awsConfig join configuration
     */
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    /**
     * @param awsConfig the AwsConfig join configuration to set
     * @throws IllegalArgumentException if awsConfig is null
     */
    public JoinConfig setAwsConfig(final AwsConfig awsConfig) {
        this.awsConfig = isNotNull(awsConfig, "awsConfig");
        return this;
    }

    /**
     * Verifies this JoinConfig is valid. At most a single joiner should be active.
     *
     * @throws IllegalStateException when the join config is not valid.
     */
    public void verify() {
        if (getTcpIpConfig().isEnabled() && getMulticastConfig().isEnabled()) {
            throw new InvalidConfigurationException("TCP/IP and Multicast join can't be enabled at the same time");
        }

        if (getTcpIpConfig().isEnabled() && getAwsConfig().isEnabled()) {
            throw new InvalidConfigurationException("TCP/IP and AWS join can't be enabled at the same time");
        }

        if (getMulticastConfig().isEnabled() && getAwsConfig().isEnabled()) {
            throw new InvalidConfigurationException("Multicast and AWS join can't be enabled at the same time");
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JoinConfig{");
        sb.append("multicastConfig=").append(multicastConfig);
        sb.append(", tcpIpConfig=").append(tcpIpConfig);
        sb.append(", awsConfig=").append(awsConfig);
        sb.append('}');
        return sb.toString();
    }
}
