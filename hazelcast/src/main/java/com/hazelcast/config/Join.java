/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

public class Join {

    private MulticastConfig multicastConfig = new MulticastConfig();

    private TcpIpConfig tcpIpConfig = new TcpIpConfig();

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
     * @return the joinMembers
     */
    public TcpIpConfig getJoinMembers() {
        return tcpIpConfig;
    }

    /**
     * @param tcpIpConfig the joinMembers to set
     */
    public Join setJoinMembers(final TcpIpConfig tcpIpConfig) {
        this.tcpIpConfig = tcpIpConfig;
        return this;
    }
}
