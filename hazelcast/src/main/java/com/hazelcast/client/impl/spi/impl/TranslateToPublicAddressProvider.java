/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class TranslateToPublicAddressProvider {
    private static final int REACHABLE_ADDRESS_TIMEOUT_MILLIS = 1000;
    private static final int NON_REACHABLE_ADDRESS_TIMEOUT_MILLIS = 3000;
    private static final int REACHABLE_CHECK_NUMBER = 3;
    private static final EndpointQualifier CLIENT_PUBLIC_ENDPOINT_QUALIFIER =
            EndpointQualifier.resolve(ProtocolType.CLIENT, "public");
    private final ClientNetworkConfig config;
    private final HazelcastProperties properties;
    private final ILogger logger;

    private volatile boolean translateToPublicAddress;

    TranslateToPublicAddressProvider(ClientNetworkConfig config, HazelcastProperties properties, ILogger logger) {
        this.config = config;
        this.properties = properties;
        this.logger = logger;
    }

    void refresh(AddressProvider addressProvider, Collection<MemberInfo> members) {
        translateToPublicAddress = resolve(addressProvider, members);
    }

    private boolean resolve(AddressProvider addressProvider, Collection<MemberInfo> members) {
        if (!(addressProvider instanceof DefaultAddressProvider)) {
            return false;
        }

        // Default value of DISCOVERY_SPI_PUBLIC_IP_ENABLED is `null` intentionally.
        // If DISCOVERY_SPI_PUBLIC_IP_ENABLED is not set to true/false, we don't know the intention of the user,
        // we will try to decide if we should use private/public address automatically in that case.
        String publicIpEnabledProperty = properties.getString(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED);
        if (publicIpEnabledProperty == null) {
            SSLConfig sslConfig = config.getSSLConfig();
            if (sslConfig != null && sslConfig.isEnabled()) {
                if (logger.isFineEnabled()) {
                    logger.fine("SSL is configured. The client will use internal addresses to communicate with the cluster. If "
                            + "members are not reachable via private addresses, "
                            + "please set \"hazelcast.discovery.public.ip.enabled\" to true ");
                }
                return false;
            }

            if (memberInternalAddressAsDefinedInClientConfig(members)) {
                if (logger.isFineEnabled()) {
                    logger.fine("There are internal addresses of members used in the config."
                            + " The client will use internal addresses");
                }
                return false;
            }

            return membersReachableOnlyViaPublicAddress(members);
        }
        return properties.getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED);
    }

    /**
     * Checks if any member has its internal address as configured in ClientConfig.
     * <p>
     * If any member has its internal/private address the same as configured in ClientConfig, then it means that the client is
     * able to connect to members via configured address. No need to use make any address translation.
     */
    boolean memberInternalAddressAsDefinedInClientConfig(Collection<MemberInfo> members) {
        List<String> addresses = config.getAddresses();
        List<String> resolvedHosts = addresses.stream().map(s -> {
            try {
                return InetAddress.getByName(AddressUtil.getAddressHolder(s, -1).getAddress()).getHostAddress();
            } catch (UnknownHostException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        return members.stream()
                .map(memberInfo -> {
                    try {
                        return memberInfo.getAddress().getInetAddress().getHostAddress();
                    } catch (UnknownHostException e) {
                        return null;
                    }
                }).anyMatch(resolvedHosts::contains);
    }

    /**
     * Checks if members are reachable via public addresses, but not reachable via internal addresses.
     * <p>
     * We check only limited number of random members to reduce the slowdown of the startup.
     */
    private boolean membersReachableOnlyViaPublicAddress(Collection<MemberInfo> members) {
        List<MemberInfo> shuffledList = new ArrayList<>(members);
        Collections.shuffle(shuffledList);
        Iterator<MemberInfo> iter = shuffledList.iterator();
        for (int i = 0; i < REACHABLE_CHECK_NUMBER; i++) {
            if (!iter.hasNext()) {
                iter = shuffledList.iterator();
            }
            MemberInfo member = iter.next();
            Address publicAddress = member.getAddressMap().get(CLIENT_PUBLIC_ENDPOINT_QUALIFIER);
            Address internalAddress = member.getAddress();
            if (publicAddress == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("The public address is not available on the member. The client will use internal addresses");
                }
                return false;
            }
            if (isReachable(internalAddress, REACHABLE_ADDRESS_TIMEOUT_MILLIS)) {
                if (logger.isFineEnabled()) {
                    logger.fine("The internal address is reachable. The client will use the internal addresses");
                }
                return false;
            }
            if (!isReachable(publicAddress, NON_REACHABLE_ADDRESS_TIMEOUT_MILLIS)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Public address + " + publicAddress
                            + "  is not reachable. The client will use internal addresses");
                }
                return false;
            }
        }
        if (logger.isFineEnabled()) {
            logger.fine("Members are accessible via only public addresses. The client will use public addresses");
        }
        return true;
    }

    private boolean isReachable(Address address, int timeoutMs) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(address.getHost(), address.getPort()), timeoutMs);
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine("TranslateToPublicAddressProvider can not reach to address " + address
                        + " in " + timeoutMs + " millis", e);
            }
            return false;
        }
        return true;
    }

    boolean get() {
        return translateToPublicAddress;
    }
}
