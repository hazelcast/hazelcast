/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;

public class MulticastConfig implements DataSerializable {

    public final static boolean DEFAULT_ENABLED = true;
    public final static String DEFAULT_MULTICAST_GROUP = "224.2.2.3";
    public final static int DEFAULT_MULTICAST_PORT = 54327;
    public final static int DEFAULT_MULTICAST_TIMEOUT_SECONDS = 2;

    private boolean enabled = DEFAULT_ENABLED;

    private String multicastGroup = DEFAULT_MULTICAST_GROUP;

    private int multicastPort = DEFAULT_MULTICAST_PORT;

    private int multicastTimeoutSeconds = DEFAULT_MULTICAST_TIMEOUT_SECONDS;

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public MulticastConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
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
    public MulticastConfig setMulticastGroup(String multicastGroup) {
        this.multicastGroup = multicastGroup;
        return this;
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
    public MulticastConfig setMulticastPort(int multicastPort) {
        this.multicastPort = multicastPort;
        return this;
    }

    /**
     * @return the multicastTimeoutSeconds
     */
    public int getMulticastTimeoutSeconds() {
        return multicastTimeoutSeconds;
    }

    /**
     * @param multicastTimeoutSeconds the multicastTimeoutSeconds to set
     */
    public MulticastConfig setMulticastTimeoutSeconds(int multicastTimeoutSeconds) {
        this.multicastTimeoutSeconds = multicastTimeoutSeconds;
        return this;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeUTF(multicastGroup);
        out.writeInt(multicastPort);
        out.writeInt(multicastTimeoutSeconds);
    }

    public void readData(DataInput in) throws IOException {
        enabled = in.readBoolean();
        multicastGroup = in.readUTF();
        multicastPort = in.readInt();
        multicastTimeoutSeconds = in.readInt();
    }
}
