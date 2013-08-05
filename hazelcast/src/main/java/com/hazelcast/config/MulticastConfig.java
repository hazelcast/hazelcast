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

import java.util.HashSet;
import java.util.Set;

/**
 * Contains the configuration for the multicast join mechanism.
 */
public class MulticastConfig {

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

    /**
     * Gets the trusted interfaces.
     *
     * @return the trusted interface.
     * @see #setTrustedInterfaces(java.util.Set)
     */
    public Set<String> getTrustedInterfaces() {
        return trustedInterfaces;
    }

    /**
     * Sets the trusted interfaces.
     *
     * By default, so when the set of trusted interfaces is empty, a Hazelcast member will accept join-requests
     * from every member. With a trusted interface you can control the members you want to receive join request
     * from.
     *
     * The interface is an ip address where the last octet can be a wildcard '*' or a range '10-20'.
     *
     * @param interfaces the new trusted interfaces.
     * @return the updated MulticastConfig.
     * @see IllegalArgumentException if interfaces is null.
     */
    public MulticastConfig setTrustedInterfaces(Set<String> interfaces) {
        if(interfaces == null){
            throw new IllegalArgumentException("interfaces is null");
        }
        trustedInterfaces.clear();
        trustedInterfaces.addAll(interfaces);
        return this;
    }

    /**
     * Adds a trusted interface.
     *
     * @param ip the ip of the trusted interface.
     * @return the updated MulticastConfig.
     * @throws IllegalArgumentException if ip is null.
     * @see #setTrustedInterfaces(java.util.Set)
     */
    public MulticastConfig addTrustedInterface(final String ip) {
        if(ip == null){
            throw new IllegalArgumentException("ip can't be null");
        }
        trustedInterfaces.add(ip);
        return this;
    }

    /**
     * Gets the time to live of the multicast package.
     *
     * @return the time to live
     * @see java.net.MulticastSocket#setTimeToLive(int)
     * @see #setMulticastTimeToLive(int)
     */
    public int getMulticastTimeToLive() {
        return multicastTimeToLive;
    }

    /**
     * Sets the time to live for the multicast package; a value between 0..255.
     *
     * See this <a href="http://www.tldp.org/HOWTO/Multicast-HOWTO-2.html">link</a> for more information.
     *
     * @param multicastTimeToLive the time to live.
     * @return the updated MulticastConfig.
     * @throws IllegalArgumentException if time to live is smaller than 0 or larger than 255.
     * @see #getMulticastTimeToLive()
     * @see java.net.MulticastSocket#setTimeToLive(int)
     */
    public MulticastConfig setMulticastTimeToLive(final int multicastTimeToLive) {
        if (multicastTimeToLive < 0 || multicastTimeToLive > 255) {
            throw new IllegalArgumentException("multicastTimeToLive out of range");
        }
        this.multicastTimeToLive = multicastTimeToLive;
        return this;
    }

    @Override
    public String toString() {
        return "MulticastConfig [enabled=" + enabled
                + ", multicastGroup=" + multicastGroup
                + ", multicastPort=" + multicastPort
                + ", multicastTimeToLive=" + multicastTimeToLive
                + ", multicastTimeoutSeconds=" + multicastTimeoutSeconds
                + ", trustedInterfaces=" + trustedInterfaces +
                "]";
    }
}
