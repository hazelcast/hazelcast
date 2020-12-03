/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

class TranslateToPublicAddressProvider {
    private static final int REACHABLE_ADDRESS_TIMEOUT_MILLIS = 1000;
    private static final int NON_REACHABLE_ADDRESS_TIMEOUT_MILLIS = 3000;
    private static final int REACHABLE_CHECK_NUMBER = 3;
    private static final EndpointQualifier CLIENT_PUBLIC_ENDPOINT_QUALIFIER =
            EndpointQualifier.resolve(ProtocolType.CLIENT, "public");
    private final ClientConfig config;
    private final HazelcastProperties properties;
    private final ILogger logger;

    private volatile boolean translateToPublicAddress;

    TranslateToPublicAddressProvider(ClientConfig config, HazelcastProperties properties, ILogger logger) {
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
            if (members.isEmpty() || memberInternalAddressAsDefinedInClientConfig(members)) {
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
        List<String> addresses = config.getNetworkConfig().getAddresses();
        return members.stream()
                .map(MemberInfo::getAddress)
                .anyMatch(a -> addresses.contains(a.getHost())
                        || addresses.contains(String.format("%s:%s", a.getHost(), a.getPort())));
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
            if (publicAddress == null) {
                return false;
            }
            Address internalAddress = member.getAddress();
            if (isReachable(internalAddress, REACHABLE_ADDRESS_TIMEOUT_MILLIS)) {
                return false;
            }
            if (!isReachable(publicAddress, NON_REACHABLE_ADDRESS_TIMEOUT_MILLIS)) {
                return false;
            }
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
