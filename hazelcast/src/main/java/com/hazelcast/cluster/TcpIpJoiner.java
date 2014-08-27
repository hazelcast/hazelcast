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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.AddressUtil.AddressMatcher;
import com.hazelcast.util.AddressUtil.InvalidAddressException;
import com.hazelcast.util.Clock;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.AddressUtil.AddressHolder;

public class TcpIpJoiner extends AbstractJoiner {

    private static final int MAX_PORT_TRIES = 3;
    public static final long JOIN_RETRY_WAIT_TIME = 1000L;

    private volatile boolean claimingMaster = false;

    public TcpIpJoiner(Node node) {
        super(node);
    }

    public boolean isClaimingMaster() {
        return claimingMaster;
    }

    protected int getConnTimeoutSeconds() {
        return config.getNetworkConfig().getJoin().getTcpIpConfig().getConnectionTimeoutSeconds();
    }

    public void doJoin() {
        final Address targetAddress = getTargetAddress();
        if (targetAddress != null) {
            long maxJoinMergeTargetMillis = node.getGroupProperties().MAX_JOIN_MERGE_TARGET_SECONDS.getInteger() * 1000;
            joinViaTargetMember(targetAddress, maxJoinMergeTargetMillis);
            if (!node.joined()) {
                joinViaPossibleMembers();
            }
        } else if (config.getNetworkConfig().getJoin().getTcpIpConfig().getRequiredMember() != null) {
            Address requiredMember = getRequiredMemberAddress();
            long maxJoinMillis = getMaxJoinMillis();
            joinViaTargetMember(requiredMember, maxJoinMillis);
        } else {
            joinViaPossibleMembers();
        }
    }

    private void joinViaTargetMember(Address targetAddress, long maxJoinMillis) {
        try {
            if (targetAddress == null) {
                throw new IllegalArgumentException("Invalid target address -> NULL");
            }
            if (logger.isFinestEnabled()) {
                logger.finest("Joining over target member " + targetAddress);
            }
            if (targetAddress.equals(node.getThisAddress()) || isLocalAddress(targetAddress)) {
                node.setAsMaster();
                return;
            }
            long joinStartTime = Clock.currentTimeMillis();
            Connection connection;
            while (node.isActive() && !node.joined() && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {
                connection = node.connectionManager.getOrConnect(targetAddress);
                if (connection == null) {
                    //noinspection BusyWait
                    Thread.sleep(JOIN_RETRY_WAIT_TIME);
                    continue;
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("Sending joinRequest " + targetAddress);
                }
                node.clusterService.sendJoinRequest(targetAddress, true);
                //noinspection BusyWait
                Thread.sleep(JOIN_RETRY_WAIT_TIME);
            }
        } catch (final Exception e) {
            logger.warning(e);
        }
    }

    private void joinViaPossibleMembers() {
        try {
            blacklistedAddresses.clear();
            Collection<Address> possibleAddresses = getPossibleAddresses();

            boolean foundConnection = tryInitialConnection(possibleAddresses);
            removeBlacklistedAddresses(possibleAddresses);

            if (!foundConnection) {
                logger.finest("This node will assume master role since no possible member where connected to.");
                node.setAsMaster();
                return;
            }

            long maxJoinMillis = getMaxJoinMillis();
            long startTime = Clock.currentTimeMillis();

            while (node.isActive() && !node.joined() && (Clock.currentTimeMillis() - startTime < maxJoinMillis)) {

                tryToJoinPossibleAddresses(possibleAddresses);
                if (node.joined()) {
                    return;
                }

                removeBlacklistedAddresses(possibleAddresses);
                if (possibleAddresses.size() == 0) {
                    logger.finest(
                            "This node will assume master role since none of the possible members accepted join request.");
                    node.setAsMaster();
                    return;
                }

                boolean masterCandidate = isThisNodeMasterCandidate(possibleAddresses);
                if (masterCandidate) {
                    boolean consensus = claimMastership(possibleAddresses);
                    if (consensus) {
                        if (logger.isFinestEnabled()) {
                            logger.finest("Setting myself as master after consensus! " +
                                    "Voting endpoints: " + possibleAddresses);
                        }
                        node.setAsMaster();
                        claimingMaster = false;
                        return;
                    }
                }

                claimingMaster = false;
                lookForMaster(possibleAddresses);
            }
        } catch (Throwable t) {
            logger.severe(t);
        }
    }

    private boolean claimMastership(Collection<Address> possibleAddresses) {
        logger.finest("Claiming myself as master node!");
        claimingMaster = true;
        Collection<Future<Boolean>> responses = new LinkedList<Future<Boolean>>();
        for (Address address : possibleAddresses) {
            if (node.getConnectionManager().getConnection(address) != null) {
                Future future = node.nodeEngine.getOperationService()
                        .createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                                new MasterClaimOperation(), address).setTryCount(1).invoke();
                responses.add(future);
            }
        }

        final long maxWait = TimeUnit.SECONDS.toMillis(10);
        long waitTime = 0L;
        boolean consensus = true;
        for (Future<Boolean> response : responses) {
            long t = Clock.currentTimeMillis();
            try {
                consensus = response.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.finest(e);
                consensus = false;
            } finally {
                waitTime += (Clock.currentTimeMillis() - t);
            }

            if (!consensus) {
                break;
            }

            if (waitTime > maxWait) {
                consensus = false;
                break;
            }
        }
        return consensus;
    }

    private boolean isThisNodeMasterCandidate(Collection<Address> possibleAddresses) {
        int thisHashCode = node.getThisAddress().hashCode();
        for (Address address : possibleAddresses) {
            if (node.connectionManager.getConnection(address) != null) {
                if (thisHashCode > address.hashCode()) {
                    return false;
                }
            }
        }
        return true;
    }

    private void tryToJoinPossibleAddresses(Collection<Address> possibleAddresses) throws InterruptedException {
        long connectionTimeoutMillis = getConnTimeoutSeconds() * 1000L;
        long start = Clock.currentTimeMillis();

        while (!node.joined() && Clock.currentTimeMillis() - start < connectionTimeoutMillis) {

            Address masterAddress = node.getMasterAddress();
            if (possibleAddresses.isEmpty() && masterAddress == null) {
                return;
            }

            if (masterAddress != null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Sending join request to " + masterAddress);
                }
                node.clusterService.sendJoinRequest(masterAddress, true);
            } else {
                removeBlacklistedAddresses(possibleAddresses);
                sendMasterQuestion(possibleAddresses);
            }

            if (!node.joined()) {
                Thread.sleep(JOIN_RETRY_WAIT_TIME);
            }
        }
    }

    private void removeBlacklistedAddresses(Collection<Address> possibleAddresses) {
        if (!blacklistedAddresses.isEmpty()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Removing failedConnections: " + blacklistedAddresses);
            }
            possibleAddresses.removeAll(blacklistedAddresses);
        }
    }

    private boolean tryInitialConnection(Collection<Address> possibleAddresses) throws InterruptedException {
        long connectionTimeoutMillis = getConnTimeoutSeconds() * 1000L;
        long start = Clock.currentTimeMillis();
        while (Clock.currentTimeMillis() - start < connectionTimeoutMillis) {
            if (possibleAddresses.isEmpty()) {
                return false;
            }
            if (logger.isFinestEnabled()) {
                logger.finest("Will send master question to each address in: " + possibleAddresses);
            }
            if (sendMasterQuestion(possibleAddresses)) {
                return true;
            }
            Thread.sleep(JOIN_RETRY_WAIT_TIME);
            removeBlacklistedAddresses(possibleAddresses);
        }
        return false;
    }

    private void lookForMaster(Collection<Address> possibleAddresses) throws InterruptedException {
        int tryCount = 0;
        while (node.getMasterAddress() == null && tryCount++ < 20) {
            sendMasterQuestion(possibleAddresses);
            //noinspection BusyWait
            Thread.sleep(JOIN_RETRY_WAIT_TIME);
            possibleAddresses.removeAll(blacklistedAddresses);
            if (possibleAddresses.size() == 0) {
                break;
            }
        }

        if (node.joined()) {
            return;
        }

        if (possibleAddresses.size() == 0 && node.getMasterAddress() == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Setting myself as master! No possible addresses remaining to connect...");
            }
            node.setAsMaster();
            return;
        }

        // max join time to found master node,
        // this should be significantly greater than MAX_WAIT_SECONDS_BEFORE_JOIN property
        // hence we add 10 seconds more
        long maxMasterJoinTime = (node.getGroupProperties().MAX_WAIT_SECONDS_BEFORE_JOIN.getInteger() + 10) * 1000L;
        long start = Clock.currentTimeMillis();

        while (node.isActive() && !node.joined() && Clock.currentTimeMillis() - start < maxMasterJoinTime) {
            Address master = node.getMasterAddress();
            if (master != null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Joining to master " + master);
                }
                node.clusterService.sendJoinRequest(master, true);
            } else {
                break;
            }

            //noinspection BusyWait
            Thread.sleep(JOIN_RETRY_WAIT_TIME);
        }

        if (!node.joined()) {
            Address master = node.getMasterAddress();
            if (master != null) {
                logger.warning("Couldn't join to the master : " + master);
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Couldn't find a master! But there was connections available: " + possibleAddresses);
                }
            }
        }
    }

    private boolean sendMasterQuestion(Collection<Address> possibleAddresses) {
        boolean sent = false;
        for (Address address : possibleAddresses) {
            if (logger.isFinestEnabled()) {
                logger.finest("Sending master question to " + address);
            }
            if (node.clusterService.sendMasterQuestion(address)) {
                sent = true;
            }
        }
        return sent;
    }

    private Address getRequiredMemberAddress() {
        final TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
        final String host = tcpIpConfig.getRequiredMember();
        try {
            final AddressHolder addressHolder = AddressUtil.getAddressHolder(host, config.getNetworkConfig().getPort());
            if (AddressUtil.isIpAddress(addressHolder.getAddress())) {
                return new Address(addressHolder.getAddress(), addressHolder.getPort());
            } else {
                final InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
                if (interfaces.isEnabled()) {
                    final InetAddress[] inetAddresses = InetAddress.getAllByName(addressHolder.getAddress());
                    if (inetAddresses.length > 1) {
                        for (InetAddress inetAddress : inetAddresses) {
                            if (AddressUtil.matchAnyInterface(inetAddress.getHostAddress(),
                                    interfaces.getInterfaces())) {
                                return new Address(inetAddress, addressHolder.getPort());
                            }
                        }
                    } else {
                        final InetAddress inetAddress = inetAddresses[0];
                        if (AddressUtil.matchAnyInterface(inetAddress.getHostAddress(),
                                interfaces.getInterfaces())) {
                            return new Address(addressHolder.getAddress(), addressHolder.getPort());
                        }
                    }
                } else {
                    return new Address(addressHolder.getAddress(), addressHolder.getPort());
                }
            }
        } catch (final Exception e) {
            logger.warning(e);
        }
        return null;
    }

    private Collection<Address> getPossibleAddresses() {
        final Collection<String> possibleMembers = getMembers();
        final Set<Address> possibleAddresses = new HashSet<Address>();
        final NetworkConfig networkConfig = config.getNetworkConfig();
        for (String possibleMember : possibleMembers) {
            AddressHolder addressHolder = AddressUtil.getAddressHolder(possibleMember);
            try {
                boolean portIsDefined = addressHolder.getPort() != -1 || !networkConfig.isPortAutoIncrement();
                int count = portIsDefined ? 1 : MAX_PORT_TRIES;
                int port = addressHolder.getPort() != -1 ? addressHolder.getPort() : networkConfig.getPort();
                AddressMatcher addressMatcher = null;
                try {
                    addressMatcher = AddressUtil.getAddressMatcher(addressHolder.getAddress());
                } catch (InvalidAddressException ignore) {
                }
                if (addressMatcher != null) {
                    final Collection<String> matchedAddresses;
                    if (addressMatcher.isIPv4()) {
                        matchedAddresses = AddressUtil.getMatchingIpv4Addresses(addressMatcher);
                    } else {
                        // for IPv6 we are not doing wildcard matching
                        matchedAddresses = Collections.singleton(addressHolder.getAddress());
                    }
                    for (String matchedAddress : matchedAddresses) {
                        addPossibleAddresses(possibleAddresses, null, InetAddress.getByName(matchedAddress), port, count);
                    }
                } else {
                    final String host = addressHolder.getAddress();
                    final InterfacesConfig interfaces = networkConfig.getInterfaces();
                    if (interfaces.isEnabled()) {
                        final InetAddress[] inetAddresses = InetAddress.getAllByName(host);
                        if (inetAddresses.length > 1) {
                            for (InetAddress inetAddress : inetAddresses) {
                                if (AddressUtil.matchAnyInterface(inetAddress.getHostAddress(),
                                        interfaces.getInterfaces())) {
                                    addPossibleAddresses(possibleAddresses, null, inetAddress, port, count);
                                }
                            }
                        } else {
                            final InetAddress inetAddress = inetAddresses[0];
                            if (AddressUtil.matchAnyInterface(inetAddress.getHostAddress(),
                                    interfaces.getInterfaces())) {
                                addPossibleAddresses(possibleAddresses, host, null, port, count);
                            }
                        }
                    } else {
                        addPossibleAddresses(possibleAddresses, host, null, port, count);
                    }
                }
            } catch (UnknownHostException e) {
                logger.warning("Cannot resolve hostname '" + addressHolder.getAddress()
                        + "'. Please make sure host is valid and reachable.");
                if (logger.isFinestEnabled()) {
                    logger.finest("Error during resolving possible target!", e);
                }
            }
        }

        possibleAddresses.remove(node.getThisAddress());
        return possibleAddresses;
    }

    private void addPossibleAddresses(final Set<Address> possibleAddresses,
                                      final String host, final InetAddress inetAddress,
                                      final int port, final int count) throws UnknownHostException {
        for (int i = 0; i < count; i++) {
            int currentPort = port + i;
            Address address = host != null ? new Address(host, currentPort) : new Address(inetAddress, currentPort);
            if (!isLocalAddress(address)) {
                possibleAddresses.add(address);
            }
        }
    }

    private boolean isLocalAddress(final Address address) throws UnknownHostException {
        final Address thisAddress = node.getThisAddress();
        final boolean local = thisAddress.getInetSocketAddress().equals(address.getInetSocketAddress());
        if (logger.isFinestEnabled()) {
            logger.finest(address + " is local? " + local);
        }
        return local;
    }

    protected Collection<String> getMembers() {
        return getConfigurationMembers(config);
    }

    public static Collection<String> getConfigurationMembers(Config config) {
        final TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
        final Collection<String> configMembers = tcpIpConfig.getMembers();
        final Set<String> possibleMembers = new HashSet<String>();
        for (String member : configMembers) {
            // split members defined in tcp-ip configuration by comma(,) semi-colon(;) space( ).
            String[] members = member.split("[,; ]");
            Collections.addAll(possibleMembers, members);
        }
        return possibleMembers;
    }

    public void searchForOtherClusters() {
        final Collection<Address> colPossibleAddresses;
        try {
            colPossibleAddresses = getPossibleAddresses();
        } catch (Throwable e) {
            logger.severe(e);
            return;
        }
        colPossibleAddresses.remove(node.getThisAddress());
        for (Member member : node.getClusterService().getMembers()) {
            colPossibleAddresses.remove(((MemberImpl) member).getAddress());
        }
        if (colPossibleAddresses.isEmpty()) {
            return;
        }
        for (Address possibleAddress : colPossibleAddresses) {
            if (logger.isFinestEnabled()) {
                logger.finest(node.getThisAddress() + " is connecting to " + possibleAddress);
            }
            node.connectionManager.getOrConnect(possibleAddress, true);
            try {
                //noinspection BusyWait
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                return;
            }
            final Connection conn = node.connectionManager.getConnection(possibleAddress);
            if (conn != null) {
                final JoinRequest response = node.clusterService.checkJoinInfo(possibleAddress);
                if (response != null && shouldMerge(response)) {
                    logger.warning(node.getThisAddress() + " is merging [tcp/ip] to " + possibleAddress);
                    setTargetAddress(possibleAddress);
                    startClusterMerge(possibleAddress);
                    return;
                }
            }
        }
    }

    @Override
    public String getType() {
        return "tcp-ip";
    }
}
