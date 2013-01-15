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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MulticastConfig implements DataSerializable {

    public final static boolean DEFAULT_ENABLED = true;
    public final static String DEFAULT_MULTICAST_GROUP = "224.2.2.3";
    public final static int DEFAULT_MULTICAST_PORT = 54327;
    public final static int DEFAULT_MULTICAST_TIMEOUT_SECONDS = 2;
    public final static int DEFAULT_MULTICAST_TTL = 32;

    private boolean enabled = DEFAULT_ENABLED;

    private String multicastGroup = DEFAULT_MULTICAST_GROUP;

    private int multicastPort = DEFAULT_MULTICAST_PORT;

    private int multicastTimeoutSeconds = DEFAULT_MULTICAST_TIMEOUT_SECONDS;

    private int multicastTimeToLive = DEFAULT_MULTICAST_TTL;

    private final Set<String> trustedInterfaces = new HashSet<String>();

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

    public Set<String> getTrustedInterfaces() {
        return trustedInterfaces;
    }

    public MulticastConfig setTrustedInterfaces(Set<String> interfaces) {
        trustedInterfaces.clear();
        trustedInterfaces.addAll(interfaces);
        return this;
    }

    public MulticastConfig addTrustedInterface(final String ip) {
        trustedInterfaces.add(ip);
        return this;
    }

    public int getMulticastTimeToLive() {
        return multicastTimeToLive;
    }

    public MulticastConfig setMulticastTimeToLive(final int multicastTimeToLive) {
        this.multicastTimeToLive = multicastTimeToLive;
        return this;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeUTF(multicastGroup);
        out.writeInt(multicastPort);
        out.writeInt(multicastTimeoutSeconds);
        out.writeInt(multicastTimeToLive);
    }

    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        multicastGroup = in.readUTF();
        multicastPort = in.readInt();
        multicastTimeoutSeconds = in.readInt();
        multicastTimeToLive = in.readInt();
    }

    @Override
    public String toString() {
        return "MulticastConfig [enabled=" + enabled
                + ", multicastGroup=" + multicastGroup
                + ", multicastPort=" + multicastPort
                + ", multicastTimeoutSeconds=" + multicastTimeoutSeconds + "]";
    }
}
