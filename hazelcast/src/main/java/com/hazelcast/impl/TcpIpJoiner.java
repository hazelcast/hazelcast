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

package com.hazelcast.impl;

import com.hazelcast.cluster.JoinInfo;
import com.hazelcast.config.Config;
import com.hazelcast.config.Interfaces;
import com.hazelcast.config.Join;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.AddressUtil;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class TcpIpJoiner extends AbstractJoiner {

    public TcpIpJoiner(Node node) {
        super(node);
    }

    private void joinViaRequiredMember(AtomicBoolean joined) {
        try {
            final Address requiredAddress = getAddressFor(config.getNetworkConfig().getJoin().getTcpIpConfig().getRequiredMember());
            logger.log(Level.FINEST, "Joining over required member " + requiredAddress);
            if (requiredAddress == null) {
                throw new RuntimeException("Invalid required member "
                        + config.getNetworkConfig().getJoin().getTcpIpConfig().getRequiredMember());
            }
            if (requiredAddress.equals(node.address)) {
                node.setAsMaster();
                return;
            }
            node.connectionManager.getOrConnect(requiredAddress);
            Connection conn = null;
            while (conn == null) {
                conn = node.connectionManager.getOrConnect(requiredAddress);
                Thread.sleep(2000L);
            }
            while (!joined.get()) {
                final Connection connection = node.connectionManager.getOrConnect(requiredAddress);
                if (connection == null) {
                    joinViaRequiredMember(joined);
                }
                logger.log(Level.FINEST, "Sending joinRequest " + requiredAddress);
                node.clusterManager.sendJoinRequest(requiredAddress);
                Thread.sleep(3000L);
            }
        } catch (final Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    private void joinViaPossibleMembers(AtomicBoolean joined) {
        try {
            node.getFailedConnections().clear();
            final Collection<Address> colPossibleAddresses = getPossibleMembers(config, node.address, logger);
            colPossibleAddresses.remove(node.address);
            for (final Address possibleAddress : colPossibleAddresses) {
                logger.log(Level.INFO, "connecting to " + possibleAddress);
                node.connectionManager.getOrConnect(possibleAddress);
            }
            boolean found = false;
            int numberOfSeconds = 0;
            final int connectionTimeoutSeconds = config.getNetworkConfig().getJoin().getTcpIpConfig().getConnectionTimeoutSeconds();
            while (!found && numberOfSeconds < connectionTimeoutSeconds) {
                colPossibleAddresses.removeAll(node.getFailedConnections());
                if (colPossibleAddresses.size() == 0) {
                    break;
                }
                Thread.sleep(1000L);
                numberOfSeconds++;
                int numberOfJoinReq = 0;
                logger.log(Level.FINEST, "we are going to try to connect to each address, but no more than five times");
                for (Address possibleAddress : colPossibleAddresses) {
                    logger.log(Level.FINEST, "connection attempt " + numberOfJoinReq + " to " + possibleAddress);
                    final Connection conn = node.connectionManager.getOrConnect(possibleAddress);
                    if (conn != null && numberOfJoinReq < 5) {
                        found = true;
                        logger.log(Level.FINEST, "found and sending join request for " + possibleAddress);
                        node.clusterManager.sendJoinRequest(possibleAddress);
                        numberOfJoinReq++;
                    } else {
                        logger.log(Level.FINEST, "number of join requests is greater than 5, no join request will be sent for " + possibleAddress);
                    }
                }
            }
            logger.log(Level.FINEST, "FOUND " + found);
            if (!found) {
                logger.log(Level.FINEST, "This node will assume master role since no possible member where connected to");
                node.setAsMaster();
            } else {
                while (!joined.get()) {
                    int maxTryCount = 3;
                    for (Address possibleAddress : colPossibleAddresses) {
                        if (node.address.hashCode() > possibleAddress.hashCode()) {
                            maxTryCount = 6;
                            break;
                        } else if (node.address.hashCode() == possibleAddress.hashCode()) {
                            maxTryCount = 3 + ((int) (Math.random() * 10));
                            break;
                        }
                    }
                    int tryCount = 0;
                    while (tryCount++ < maxTryCount && (node.getMasterAddress() == null)) {
                        connectAndSendJoinRequest(colPossibleAddresses);
                        Thread.sleep(1000L);
                    }
                    int requestCount = 0;
                    while (node.getMasterAddress() != null && !joined.get()) {
                        Thread.sleep(1000L);
                        node.clusterManager.sendJoinRequest(node.getMasterAddress());
                        if (requestCount++ > 22) {
                            failedJoiningToMaster(false, requestCount);
                        }
                    }
                    if (node.getMasterAddress() == null) { // no-one knows the master
                        boolean masterCandidate = true;
                        for (Address address : colPossibleAddresses) {
                            if (node.address.hashCode() > address.hashCode()) {
                                masterCandidate = false;
                            }
                        }
                        if (masterCandidate) {
                            logger.log(Level.FINEST, "I am the master candidate, setting as master");
                            node.setAsMaster();
                        }
                    }
                }
            }
            colPossibleAddresses.clear();
            node.getFailedConnections().clear();
        } catch (Throwable t) {
            logger.log(Level.SEVERE, t.getMessage(), t);
        }
    }

    private Address getAddressFor(String host) {
        int port = config.getPort();
        final int indexColon = host.indexOf(':');
        if (indexColon != -1) {
            port = Integer.parseInt(host.substring(indexColon + 1));
            host = host.substring(0, indexColon);
        }
        final boolean ip = isIP(host);
        try {
            if (ip) {
                return new Address(host, port, true);
            } else {
                final InetAddress[] allAddresses = InetAddress.getAllByName(host);
                for (final InetAddress inetAddress : allAddresses) {
                    boolean shouldCheck = true;
                    Address address;
                    Interfaces interfaces = config.getNetworkConfig().getInterfaces();
                    if (interfaces.isEnabled()) {
                        address = new Address(inetAddress.getAddress(), port);
                        shouldCheck = AddressPicker.matchAddress(address.getHost(), interfaces.getInterfaces());
                    }
                    if (shouldCheck) {
                        return new Address(inetAddress.getAddress(), port);
                    }
                }
            }
        } catch (final Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        return null;
    }

    public void doJoin(AtomicBoolean joined) {
        if (config.getNetworkConfig().getJoin().getTcpIpConfig().getRequiredMember() != null) {
            joinViaRequiredMember(joined);
        } else {
            joinViaPossibleMembers(joined);
        }
    }

    private List<Address> getPossibleIpAddresses(final String host, final int port, boolean portSet)
            throws UnknownHostException {
        final List<Address> list;
        if (portSet) {
            list = Collections.singletonList(new Address(host, port, true));
        } else {
            list = new ArrayList(6);
            for (int i = -2; i < 3; i++) {
                list.add(new Address(host, port + i, true));
            }
        }
        return list;
    }

    boolean isIP(final String address) {
        if (address.indexOf('.') == -1) {
            return false;
        } else {
            final StringTokenizer st = new StringTokenizer(address, ".");
            int tokenCount = 0;
            while (st.hasMoreTokens()) {
                final String token = st.nextToken();
                tokenCount++;
                try {
                    Integer.parseInt(token);
                } catch (final Exception e) {
                    return false;
                }
            }
            if (tokenCount != 4)
                return false;
        }
        return true;
    }

    Collection<Address> getPossibleMembers(Config config, Address thisAddress, ILogger logger) {
        final Set<String> lsJoinMembers = new HashSet<String>();
        List<String> members = getMembers(config);
        for (String member : members) {
            lsJoinMembers.addAll(AddressUtil.handleMember(member));
        }
        final Set<Address> setPossibleAddresses = new HashSet<Address>();
        for (final String lsJoinMember : lsJoinMembers) {
            try {
                String host = lsJoinMember;
                int port = config.getPort();
                final int indexColon = host.indexOf(':');
                if (indexColon >= 0) {
                    port = Integer.parseInt(host.substring(indexColon + 1));
                    host = host.substring(0, indexColon);
                }
                // check if host is hostname of ip address
                final boolean ip = isIP(host);
                if (ip) {
                    for (final Address addrs : getPossibleIpAddresses(host, port, indexColon >= 0)) {
                        if (!addrs.equals(thisAddress)) {
                            setPossibleAddresses.add(addrs);
                        }
                    }
                } else {
                    final InetAddress[] allAddresses = InetAddress.getAllByName(host);
                    for (final InetAddress inetAddress : allAddresses) {
                        // IPv6 can not be used atm.
                        if (inetAddress instanceof Inet6Address) {
                            logger.log(Level.FINEST, "This address [" + inetAddress + "] is not usable. " +
                                    "Hazelcast does not have support for IPv6 at the moment.");
                            continue;
                        }
                        boolean shouldCheck = true;
                        Address addrs;
                        Interfaces interfaces = config.getNetworkConfig().getInterfaces();
                        if (interfaces.isEnabled()) {
                            addrs = new Address(inetAddress.getAddress(), port);
                            shouldCheck = AddressPicker.matchAddress(addrs.getHost(), interfaces.getInterfaces());
                        }
                        if (indexColon < 0) {
                            // port is not set
                            if (shouldCheck) {
                                for (int i = -2; i < 3; i++) {
                                    final Address addressProper = new Address(inetAddress.getAddress(), port + i);
                                    if (!addressProper.equals(thisAddress)) {
                                        setPossibleAddresses.add(addressProper);
                                    }
                                }
                            }
                        } else {
                            final Address addressProper = new Address(inetAddress.getAddress(), port);
                            if (!addressProper.equals(thisAddress)) {
                                setPossibleAddresses.add(addressProper);
                            }
                        }
                    }
                }
            } catch (UnknownHostException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
        setPossibleAddresses.addAll(config.getNetworkConfig().getJoin().getTcpIpConfig().getAddresses());
        return setPossibleAddresses;
    }

    protected List<String> getMembers(Config config) {
        Join join = config.getNetworkConfig().getJoin();
        return join.getTcpIpConfig().getMembers();
    }

    public void searchForOtherClusters(SplitBrainHandler splitBrainHandler) {
        final Collection<Address> colPossibleAddresses;
        try {
            colPossibleAddresses = getPossibleMembers(node.getConfig(), node.getThisAddress(), logger);
        } catch (Throwable e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            return;
        }
        colPossibleAddresses.remove(node.getThisAddress());
        for (Member member : node.getClusterImpl().getMembers()) {
            colPossibleAddresses.remove(((MemberImpl) member).getAddress());
        }
        if (colPossibleAddresses.size() == 0) {
            return;
        }
        for (final Address possibleAddress : colPossibleAddresses) {
            logger.log(Level.FINEST, node.getThisAddress() + " is connecting to " + possibleAddress);
            node.connectionManager.getOrConnect(possibleAddress);
        }
        for (Address possibleAddress : colPossibleAddresses) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
            final Connection conn = node.connectionManager.getOrConnect(possibleAddress);
            if (conn != null) {
                JoinInfo response = node.clusterManager.checkJoin(conn);
                if (shouldMerge(response)) {
                    // we will join so delay the merge checks.
                    logger.log(Level.WARNING, node.address + " is merging [tcp/ip] to " + possibleAddress);
                    splitBrainHandler.restart();
                }
                // trying one live connection is good enough
                // no need to try other connections
                return;
            }
        }
    }
}
