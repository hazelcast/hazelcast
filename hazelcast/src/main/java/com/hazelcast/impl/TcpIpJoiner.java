/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.cluster.JoinInfo;
import com.hazelcast.config.Config;
import com.hazelcast.config.Interfaces;
import com.hazelcast.config.Join;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.AddressUtil.AddressMatcher;
import com.hazelcast.util.AddressUtil.InvalidAddressException;
import com.hazelcast.util.Clock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.util.AddressUtil.AddressHolder;

public class TcpIpJoiner extends AbstractJoiner {

    private static final int MAX_ADDRESS_TRIES = 3;

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
                //noinspection BusyWait
                Thread.sleep(2000L);
            }
            long joinStartTime = Clock.currentTimeMillis();
            long maxJoinMillis = node.getGroupProperties().MAX_JOIN_SECONDS.getInteger() * 1000;
            while (node.isActive() && !joined.get() && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {
                final Connection connection = node.connectionManager.getOrConnect(requiredAddress);
                if (connection == null) {
                    joinViaRequiredMember(joined);
                }
                logger.log(Level.FINEST, "Sending joinRequest " + requiredAddress);
                node.clusterManager.sendJoinRequest(requiredAddress, true);
                //noinspection BusyWait
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
            getNode().clusterManager.sendProcessableTo(new MasterAnswer(node.getThisAddress(), shouldApprove),
                                                       getConnection());
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
            final Collection<Address> colPossibleAddresses = getPossibleAddresses();
            colPossibleAddresses.remove(node.address);
            for (final Address possibleAddress : colPossibleAddresses) {
                logger.log(Level.INFO, "connecting to " + possibleAddress);
                node.connectionManager.getOrConnect(possibleAddress);
            }
            boolean foundConnection = false;
            int numberOfSeconds = 0;
            final int connectionTimeoutSeconds = config.getNetworkConfig().getJoin().getTcpIpConfig().getConnectionTimeoutSeconds();
            while (!foundConnection && numberOfSeconds < connectionTimeoutSeconds) {
                logger.log(Level.FINEST, "Removing failedConnections: " + node.getFailedConnections());
                colPossibleAddresses.removeAll(node.getFailedConnections());
                if (colPossibleAddresses.size() == 0) {
                    break;
                }
                //noinspection BusyWait
                Thread.sleep(1000L);
                numberOfSeconds++;
                logger.log(Level.FINEST, "We are going to try to connect to each address" + colPossibleAddresses);
                for (Address possibleAddress : colPossibleAddresses) {
                    final Connection conn = node.connectionManager.getOrConnect(possibleAddress);
                    if (conn != null) {
                        foundConnection = true;
                        logger.log(Level.FINEST, "Found and sending join request for " + possibleAddress);
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
                                //noinspection BusyWait
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
            //noinspection BusyWait
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
            //noinspection BusyWait
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
        try {
            final AddressHolder addressHolder = AddressUtil.getAddressHolder(host, config.getPort());
            if (AddressUtil.isIpAddress(addressHolder.address)) {
                return new Address(addressHolder.address, addressHolder.port);
            } else {
                final InetAddress[] allAddresses = InetAddress.getAllByName(addressHolder.address);
                final Interfaces interfaces = config.getNetworkConfig().getInterfaces();
                for (final InetAddress inetAddress : allAddresses) {
                    boolean matchingAddress = true;
                    if (interfaces.isEnabled()) {
                        matchingAddress = AddressPicker.matchAddress(inetAddress.getHostAddress(), interfaces.getInterfaces());
                    }
                    if (matchingAddress) {
                        return new Address(inetAddress, addressHolder.port);
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

    private Collection<Address> getPossibleAddresses() {
        final Collection<String> possibleMembers = getMembers();
        final Set<Address> setPossibleAddresses = new HashSet<Address>();
        final Address thisAddress = node.address;
        for (String host : possibleMembers) {
            try {
                final AddressHolder addressHolder = AddressUtil.getAddressHolder(host);
                final boolean portIsDefined = addressHolder.port != -1 || !config.isPortAutoIncrement();
                final int maxAddressTries = portIsDefined ? 1 : MAX_ADDRESS_TRIES;
                final int port = addressHolder.port != -1 ? addressHolder.port : config.getPort();
                AddressMatcher addressMatcher = null;
                try {
                    addressMatcher = AddressUtil.getAddressMatcher(addressHolder.address);
                } catch (InvalidAddressException ignore) {
                }
                if (addressMatcher != null) {
                    final Collection<String> matchedAddresses;
                    if (addressMatcher.isIPv4()) {
                        matchedAddresses = AddressUtil.getMatchingIpv4Addresses(addressMatcher);
                    } else {
                        // for IPv6 we are not doing wildcard matching
                        matchedAddresses = Collections.singleton(addressHolder.address);
                    }
                    for (String matchedAddress : matchedAddresses) {
                        for (int i = 0; i < maxAddressTries; i++) {
                            final Address addressProper = new Address(matchedAddress, port + i);
                            if (!addressProper.equals(thisAddress)) {
                                setPossibleAddresses.add(addressProper);
                            }
                        }
                    }
                } else {
                    final InetAddress[] allAddresses = InetAddress.getAllByName(addressHolder.address);
                    for (final InetAddress inetAddress : allAddresses) {
                        boolean matchingAddress = true;
                        Interfaces interfaces = config.getNetworkConfig().getInterfaces();
                        if (interfaces.isEnabled()) {
                            matchingAddress = AddressPicker.matchAddress(inetAddress.getHostAddress(),
                                    interfaces.getInterfaces());
                        }
                        if (matchingAddress) {
                            for (int i = 0; i < maxAddressTries; i++) {
                                final Address addressProper = new Address(inetAddress, port + i);
                                if (!addressProper.equals(thisAddress)) {
                                    setPossibleAddresses.add(addressProper);
                                }
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

    protected Collection<String> getMembers() {
        return getConfigurationMembers(config);
    }

    public static Collection<String> getConfigurationMembers(Config config) {
        Join join = config.getNetworkConfig().getJoin();
        final Collection<String> configMembers = join.getTcpIpConfig().getMembers();
        final Set<String> possibleMembers = new HashSet<String>();
        for (String member : configMembers) {
            // split members defined in tcp-ip configuration by comma(,) semi-colon(;) space( ).
            String[] members = member.split("[,; ]");
            for (String address : members) {
                possibleMembers.add(address);
            }
        }
        return possibleMembers;
    }

    public void searchForOtherClusters(SplitBrainHandler splitBrainHandler) {
        final Collection<Address> colPossibleAddresses;
        try {
            colPossibleAddresses = getPossibleAddresses();
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
                //noinspection BusyWait
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
            final Connection conn = node.connectionManager.getConnection(possibleAddress);
            if (conn != null) {
                final JoinInfo response = node.clusterManager.checkJoin(conn);
                if (response != null && shouldMerge(response)) {
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
