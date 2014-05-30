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

import static com.hazelcast.util.ValidationUtil.hasText;
import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains the configuration for the multicast discovery mechanism.
 *
 * With the multicast discovery mechanism Hazelcast allows Hazelcast members to find each other using multicast. So
 * Hazelcast members do not need to know concrete addresses of members, they just multicast to everyone listening.
 *
 * It depends on your environment if multicast is possible or allowed; otherwise you need to have a look at the
 * tcp/ip cluster: {@link TcpIpConfig}.
 */
public class MulticastConfig {

    public final static boolean DEFAULT_ENABLED = true;
    public final static String DEFAULT_MULTICAST_GROUP = "224.2.2.3";
    public final static int DEFAULT_MULTICAST_PORT = 54327;
    public final static int DEFAULT_MULTICAST_TIMEOUT_SECONDS = 2;
    public final static int DEFAULT_MULTICAST_TTL = 32;
    public final static boolean DEFAULT_LOOPBACK_MODE_ENABLED = false;

    private boolean enabled = DEFAULT_ENABLED;

    private String multicastGroup = DEFAULT_MULTICAST_GROUP;

    private int multicastPort = DEFAULT_MULTICAST_PORT;

    private int multicastTimeoutSeconds = DEFAULT_MULTICAST_TIMEOUT_SECONDS;

    private int multicastTimeToLive = DEFAULT_MULTICAST_TTL;

    private final Set<String> trustedInterfaces = new HashSet<String>();

    private boolean loopbackModeEnabled = DEFAULT_LOOPBACK_MODE_ENABLED;

    /**
     * Check if the multicast discovery mechanism has been enabled.
     *
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables the multicast discovery mechanism
     *
     * @param enabled the enabled to set true when disabled, false when disabled.
     * @return the updated MulticastConfig
     */
    public MulticastConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Gets the multicast group.
     *
     * @return the multicastGroup
     */
    public String getMulticastGroup() {
        return multicastGroup;
    }

    /**
     * Sets the multicast-group.
     *
     * @param multicastGroup the multicastGroup to set
     * @return the updated MulticastConfig
     * @throws IllegalArgumentException if multicastGroup is null or empty.
     * @see #getMulticastGroup()
     * @see #setMulticastPort(int)
     */
    public MulticastConfig setMulticastGroup(String multicastGroup) {
        this.multicastGroup = hasText(multicastGroup, "multicastGroup");
        return this;
    }

    /**
     * Gets the multicast port.
     *
     * @return the multicastPort
     * @see #setMulticastPort(int)
     */
    public int getMulticastPort() {
        return multicastPort;
    }

    /**
     * Sets the multicast port.
     *
     * @param multicastPort the multicastPort to set
     * @return the updated MulticastConfig
     * @see #getMulticastPort()
     * @see #setMulticastGroup(String)
     * @throws IllegalArgumentException if multicastPort is smaller than 0.
     */
    public MulticastConfig setMulticastPort(int multicastPort) {
        if(multicastPort<0){
            throw new IllegalArgumentException("multicastPort can't be smaller than 0");
        }
        this.multicastPort = multicastPort;
        return this;
    }

    /**
     * Gets the multicast timeout in seconds.
     *
     * @return the multicastTimeoutSeconds
     * @see #setMulticastTimeoutSeconds(int)
     */
    public int getMulticastTimeoutSeconds() {
        return multicastTimeoutSeconds;
    }

    /**
     * Specifies the time in seconds that a node should wait for a valid multicast response from another node running
     * in the network before declaring itself as master node and creating its own cluster. This applies only to the startup
     * of nodes where no master has been assigned yet. If you specify a high value, e.g. 60 seconds, it means until a master
     * is selected, each node is going to wait 60 seconds before continuing, so be careful with providing a high value. If
     * the value is set too low, it might be that nodes are giving up too early and will create their own cluster.
     *
     * @param multicastTimeoutSeconds the multicastTimeoutSeconds to set
     * @returns the updated MulticastConfig
     * @see #getMulticastTimeoutSeconds()
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
        isNotNull(interfaces,"interfaces");

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
        trustedInterfaces.add(isNotNull(ip,"ip"));
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

    /**
     * Check if the loopback mode is enabled in the multicast discovery mechanism.
     *
     * @return the enabled
     */
    public boolean isLoopbackModeEnabled() {
        return loopbackModeEnabled;
    }

    /**
     * Enables or disables the loopback mode in the multicast discovery mechanism.
     *
     * @param enabled the enabled to set true when disabled, false when disabled.
     * @return the updated MulticastConfig
     */
    public MulticastConfig setLoopbackModeEnabled(boolean enabled) {
        this.loopbackModeEnabled = enabled;
        return this;
    }

    @Override
    public String toString() {
        return "MulticastConfig [enabled=" + enabled
                + ", multicastGroup=" + multicastGroup
                + ", multicastPort=" + multicastPort
                + ", multicastTimeToLive=" + multicastTimeToLive
                + ", multicastTimeoutSeconds=" + multicastTimeoutSeconds
                + ", trustedInterfaces=" + trustedInterfaces
                + ", loopbackModeEnabled=" + loopbackModeEnabled
                + "]";
    }
}
