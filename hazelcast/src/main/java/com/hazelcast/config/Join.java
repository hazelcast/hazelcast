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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Join implements DataSerializable {

    private MulticastConfig multicastConfig = new MulticastConfig();

    private TcpIpConfig tcpIpConfig = new TcpIpConfig();

    private AwsConfig awsConfig = new AwsConfig();

    /**
     * @return the multicastConfig
     */
    public MulticastConfig getMulticastConfig() {
        return multicastConfig;
    }

    /**
     * @param multicastConfig the multicastConfig to set
     */
    public Join setMulticastConfig(final MulticastConfig multicastConfig) {
        this.multicastConfig = multicastConfig;
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
     */
    public Join setTcpIpConfig(final TcpIpConfig tcpIpConfig) {
        this.tcpIpConfig = tcpIpConfig;
        return this;
    }

    /**
     * @return the awsConfig
     */
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    /**
     * @param awsConfig the AwsConfig to set
     */
    public Join setAwsConfig(final AwsConfig awsConfig) {
        this.awsConfig = awsConfig;
        return this;
    }

    public void writeData(DataOutput out) throws IOException {
        multicastConfig.writeData(out);
        tcpIpConfig.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        multicastConfig = new MulticastConfig();
        multicastConfig.readData(in);
        tcpIpConfig = new TcpIpConfig();
        tcpIpConfig.readData(in);
    }

    @Override
    public String toString() {
        return "Join [multicastConfig=" + multicastConfig + ", tcpIpConfig=" + tcpIpConfig + ", awsConfig=" + awsConfig + "]";
    }
}
