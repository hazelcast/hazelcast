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

public class MulticastConfig {

    public final static boolean DEFAULT_ENABLED = true;
    public final static String DEFAULT_MULTICAST_GROUP = "224.2.2.3";
    public final static int DEFAULT_MULTICAST_PORT = 54327;

    private boolean enabled = DEFAULT_ENABLED;

    private String multicastGroup = DEFAULT_MULTICAST_GROUP;

    private int multicastPort = DEFAULT_MULTICAST_PORT;

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return the multicastGroup
     */
    public String getMulticastGroup() {
        return multicastGroup;
    }

    /**
     * @param multicastGroup the multicastGroup to set
     */
    public void setMulticastGroup(String multicastGroup) {
        this.multicastGroup = multicastGroup;
    }

    /**
     * @return the multicastPort
     */
    public int getMulticastPort() {
        return multicastPort;
    }

    /**
     * @param multicastPort the multicastPort to set
     */
    public void setMulticastPort(int multicastPort) {
        this.multicastPort = multicastPort;
    }
}
