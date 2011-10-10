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

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.cluster.JoinInfo;
import com.hazelcast.config.Config;
import com.hazelcast.config.Interfaces;
import com.hazelcast.config.Join;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.AddressUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
            while (node.isActive() && !joined.get()) {
                final Connection connection = node.connectionManager.getOrConnect(requiredAddress);
                if (connection == null) {
                    joinViaRequiredMember(joined);
                }
                logger.log(Level.FINEST, "Sending joinRequest " + requiredAddress);
                node.clusterManager.sendJoinRequest(requiredAddress, true);
                Thread.sleep(3000L);
            }
        } catch (final Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    public static class MasterQuestion extends AbstractRemotelyProcessable {
        public void process() {
            TcpIpJoiner tcpIpJoiner = (TcpIpJoiner) getNode().getJoiner();
            boolean shouldApprove = (tcpIpJoiner.askingForApproval || node.isMaster()) ? false : true;
            getNode().clusterManager.sendProcessableTo(new MasterAnswer(node.getThisAddress(), shouldApprove), getConnection());
        }
    }

    public static class MasterAnswer extends AbstractRemotelyProcessable {
        Address respondingAddress = null;
        boolean approved = false;

        public MasterAnswer(Address respondingAddress, boolean approved) {
            this.respondingAddress = respondingAddress;
            this.approved = approved;
        }

        public MasterAnswer() {
        }

        public void process() {
            TcpIpJoiner tcpIpJoiner = (TcpIpJoiner) getNode().getJoiner();
            if (!approved) {
                tcpIpJoiner.approved = false;
            }
            tcpIpJoiner.responseCounter.decrementAndGet();
        }

        @Override
        public void writeData(DataOutput out) throws IOException {
            super.writeData(out);
            out.writeBoolean(approved);
            respondingAddress.writeData(out);
        }

        @Override
        public void readData(DataInput in) throws IOException {
            super.readData(in);
            approved = in.readBoolean();
            respondingAddress = new Address();
            respondingAddress.readData(in);
        }
    }

    volatile boolean approved = true;
    final AtomicInteger responseCounter = new AtomicInteger();
    volatile boolean askingForApproval = false;

    private void joinViaPossibleMembers(AtomicBoolean joined) {
        try {
            node.getFailedConnections().clear();
            final Collection<Address> colPossibleAddresses = getPossibleMembers(config, node.address, logger);
            colPossibleAddresses.remove(node.address);
            for (final Address possibleAddress : colPossibleAddresses) {
                logger.log(Level.INFO, "connecting to " + possibleAddress);
                node.connectionManager.getOrConnect(possibleAddress);
            }
            boolean foundConnection = false;
            int numberOfSeconds = 0;
            final int connectionTimeoutSeconds = config.getNetworkConfig().getJoin().getTcpIpConfig().getConnectionTimeoutSeconds();
            while (!foundConnection && numberOfSeconds < connectionTimeoutSeconds) {
                colPossibleAddresses.removeAll(node.getFailedConnections());
                if (colPossibleAddresses.size() == 0) {
                    break;
                }
                Thread.sleep(1000L);
                numberOfSeconds++;
                logger.log(Level.FINEST, "we are going to try to connect to each address");
                for (Address possibleAddress : colPossibleAddresses) {
                    final Connection conn = node.connectionManager.getOrConnect(possibleAddress);
                    if (conn != null) {
                        foundConnection = true;
                        logger.log(Level.FINEST, "found and sending join request for " + possibleAddress);
                        node.clusterManager.sendJoinRequest(possibleAddress, true);
                    }
                }
            }
            logger.log(Level.FINEST, "FOUND " + foundConnection);
            if (!foundConnection) {
                logger.log(Level.FINEST, "This node will assume master role since no possible member where connected to");
                node.setAsMaster();
            } else {
                if (!node.joined()) {
                    if (connectionTimeoutSeconds - numberOfSeconds > 0) {
                        logger.log(Level.FINEST, "Sleeping for " + (connectionTimeoutSeconds - numberOfSeconds) + " seconds.");
                        Thread.sleep((connectionTimeoutSeconds - numberOfSeconds) * 1000L);
                    }
                    colPossibleAddresses.removeAll(node.getFailedConnections());
                    if (colPossibleAddresses.size() == 0) {
                        logger.log(Level.FINEST, "This node will assume master role since all possible members didn't accept join request");
                        node.setAsMaster();
                    } else {
                        boolean masterCandidate = true;
                        for (Address address : colPossibleAddresses) {
                            if (node.connectionManager.getConnection(address) != null) {
                                if (node.address.hashCode() > address.hashCode()) {
                                    masterCandidate = false;
                                }
                            }
                        }
                        if (masterCandidate) {
                            // ask others...
                            askingForApproval = true;
                            for (Address address : colPossibleAddresses) {
                                Connection conn = node.getConnectionManager().getConnection(address);
                                if (conn != null) {
                                    responseCounter.incrementAndGet();
                                    node.clusterManager.sendProcessableTo(new MasterQuestion(), conn);
                                }
                            }
                            int waitCount = 0;
                            while (node.isActive() && waitCount++ < 10) {
                                Thread.sleep(1000L);
                                if (responseCounter.get() == 0) {
                                    if (approved) {
                                        logger.log(Level.FINEST, node.getThisAddress() + " Setting myself as master! group " + node.getConfig().getGroupConfig().getName() + " possible addresses " + colPossibleAddresses.size() + "" + colPossibleAddresses);
                                        node.setAsMaster();
                                        return;
                                    } else {
                                        lookForMaster(colPossibleAddresses);
                                        break;
                                    }
                                }
                            }
                        } else {
                            lookForMaster(colPossibleAddresses);
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

    private void lookForMaster(Collection<Address> colPossibleAddresses) throws InterruptedException {
        int tryCount = 0;
        while (!node.joined() && tryCount++ < 20 && (node.getMasterAddress() == null)) {
            connectAndSendJoinRequest(colPossibleAddresses);
            Thread.sleep(1000L);
        }
        int requestCount = 0;
        colPossibleAddresses.removeAll(node.getFailedConnections());
        if (colPossibleAddresses.size() == 0) {
            node.setAsMaster();
            logger.log(Level.FINEST, node.getThisAddress() + " Setting myself as master! group " + node.getConfig().getGroupConfig().getName() + " no possible addresses without failed connection");
            return;
        }
        logger.log(Level.FINEST, node.getThisAddress() + " joining to master " + node.getMasterAddress() + ", group " + node.getConfig().getGroupConfig().getName());
        while (node.isActive() && !node.joined()) {
            Thread.sleep(1000L);
            final Address master = node.getMasterAddress();
            if (master != null) {
                node.clusterManager.sendJoinRequest(master, true);
                if (requestCount++ > node.getGroupProperties().MAX_WAIT_SECONDS_BEFORE_JOIN.getInteger() + 10) {
                    logger.log(Level.WARNING, "Couldn't join to the master : " + master);
                    return;
                }
            } else {
                logger.log(Level.FINEST, node.getThisAddress() + " couldn't find a master! but there was connections available: " + colPossibleAddresses);
                return;
            }
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
            list = new ArrayList<Address>(3);
            for (int i = 0; i < 3; i++) {
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
                    for (final Address addrs : getPossibleIpAddresses(host, port, indexColon >= 0 || !config.isPortAutoIncrement())) {
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
                                if (config.isPortAutoIncrement()) {
                                    for (int i = 0; i < 3; i++) {
                                        final Address addressProper = new Address(inetAddress.getAddress(), port + i);
                                        if (!addressProper.equals(thisAddress)) {
                                            setPossibleAddresses.add(addressProper);
                                        }
                                    }
                                } else {
                                    final Address addressProper = new Address(inetAddress.getAddress(), port);
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
