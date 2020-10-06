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

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Default address provider of Hazelcast.
 * <p>
 * Loads addresses from the Hazelcast configuration.
 */
public class DefaultAddressProvider implements AddressProvider, MembershipListener, InitialMembershipListener {
    private static final int REACHABLE_ADDRESS_TIMEOUT = 1000;
    private static final int NON_REACHABLE_ADDRESS_TIMEOUT = 3000;
    private static final int REACHABLE_CHECK_NUMBER = 3;

    private final ClientNetworkConfig networkConfig;
    private volatile Map<Address, Address> privateToPublic = new HashMap<>();
    private String usePublicIpProperty;
    private boolean usePublicAddress;
    private ILogger logger;
    private UUID membershipUuid;

    public DefaultAddressProvider(ClientNetworkConfig networkConfig, String usePublicIpProperty, ILogger logger) {
        this.networkConfig = networkConfig;
        this.usePublicIpProperty = usePublicIpProperty;
        this.logger = logger;
    }

    @Override
    public Addresses loadAddresses() {
        List<String> configuredAddresses = networkConfig.getAddresses();

        if (configuredAddresses.isEmpty()) {
            configuredAddresses.add("127.0.0.1");
        }

        Addresses addresses = new Addresses();
        for (String address : configuredAddresses) {
            addresses.addAll(AddressHelper.getSocketAddresses(address));
        }

        return addresses;
    }

    @Override
    public Address translate(Address address) throws Exception {
        if (address == null) {
            return null;
        }

        if (usePublicAddress) {
            Address publicAddress = privateToPublic.get(address);
            if (publicAddress != null) {
                return publicAddress;
            }
        }
        return address;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        refresh(membershipEvent.getMembers());
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        refresh(membershipEvent.getMembers());
    }

    @Override
    public void init(InitialMembershipEvent membershipEvent) {
        usePublicAddress = usePublicAddress(membershipEvent.getMembers());
        refresh(membershipEvent.getMembers());
    }

    private void refresh(Set<Member> members) {
        if (usePublicAddress) {
            Map<Address, Address> newPrivateToPublic = new HashMap<>();
            for (Member member : members) {
                Address publicAddress = member.getAddressMap().get(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"));
                if (publicAddress != null) {
                    newPrivateToPublic.put(member.getAddress(), publicAddress);
                }
            }

            this.privateToPublic = newPrivateToPublic;
        }
    }

    private boolean usePublicAddress(Set<Member> members) {
        if ("true".equalsIgnoreCase(usePublicIpProperty)) {
            return true;
        } else if ("false".equalsIgnoreCase(usePublicIpProperty)) {
            return false;
        }
        if (members.isEmpty() || memberAddressAsDefinedInClientConfig(members)) {
            return false;
        }

        try {
            return membersReachableOnlyViaPublicAddress(members);
        } catch (IOException e) {
            logger.finest(e);
            return false;
        }
    }

    /**
     * Checks if any member has its internal address as configured in ClientConfig.
     * <p>
     * If any member has its internal/private address the same as configured in ClientConfig, then it means that client is able
     * to connect to members via private address. No need to use member public address.
     */
    private boolean memberAddressAsDefinedInClientConfig(Set<Member> members) {
        List<String> addresses = networkConfig.getAddresses();
        return members.stream()
                .map(Member::getAddress)
                .anyMatch(a -> addresses.contains(a.getHost())
                        || addresses.contains(String.format("%s:%s", a.getHost(), a.getPort())));
    }

    /**
     * Checks if members are reachable via public addresses, but not reachable via private addresses.
     * <p>
     * We check only limited number of random members to not slow down the startup time.
     */
    private boolean membersReachableOnlyViaPublicAddress(Set<Member> members) throws IOException {
        Iterator<Member> iter = members.iterator();
        for (int i = 0; i < REACHABLE_CHECK_NUMBER; i++) {
            if (!iter.hasNext()) {
                iter = members.iterator();
            }
            Member member = iter.next();
            Address publicAddress = member.getAddressMap().get(EndpointQualifier.resolve(ProtocolType.CLIENT, "public"));
            if (publicAddress == null) {
                return false;
            }
            Address privateAddress = member.getAddress();
            if (isReachable(privateAddress, REACHABLE_ADDRESS_TIMEOUT)) {
                return false;
            }
            if (!isReachable(publicAddress, NON_REACHABLE_ADDRESS_TIMEOUT)) {
                return false;
            }
        }
        return true;
    }

    private boolean isReachable(Address address, int timeoutMs) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(address.getHost(), address.getPort()), timeoutMs);
        } catch (Exception e) {
            logger.fine(e);
            return false;
        }
        return true;
    }

    public void setMembershipUuid(UUID membershipUuid) {
        this.membershipUuid = membershipUuid;
    }


    public UUID getMembershipUuid() {
        return membershipUuid;
    }
}
