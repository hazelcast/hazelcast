package com.hazelcast.cluster;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class HeartBeater {

    private final ClusterServiceImpl clusterService;
    private final long maxNoMasterConfirmationMillis;
    private final long maxNoHeartbeatMillis;
    private final boolean icmpEnabled;
    private final int icmpTtl;
    private final int icmpTimeout;
    private final Address thisAddress;
    private Node node;
    private final ConcurrentMap<MemberImpl, Long> masterConfirmationTimes;
    private NodeEngine nodeEngine;
    private ILogger logger;


    public HeartBeater(ClusterServiceImpl clusterService, Node node, ConcurrentMap<MemberImpl, Long> masterConfirmationTimes) {
        this.clusterService = clusterService;
        this.node = node;
        this.masterConfirmationTimes = masterConfirmationTimes;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(HeartBeater.class.getName());
        maxNoMasterConfirmationMillis = node.groupProperties.MAX_NO_MASTER_CONFIRMATION_SECONDS.getInteger() * 1000L;
        maxNoHeartbeatMillis = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger() * 1000L;
        icmpEnabled = node.groupProperties.ICMP_ENABLED.getBoolean();
        icmpTtl = node.groupProperties.ICMP_TTL.getInteger();
        icmpTimeout = node.groupProperties.ICMP_TIMEOUT.getInteger();
        thisAddress = node.getThisAddress();
    }

    public final void heartbeat() {
        if (!node.joined() || !node.isActive()) {
            return;
        }
        long now = Clock.currentTimeMillis();
        final Collection<MemberImpl> members = clusterService.getMemberList();
        if (node.isMaster()) {
            checkMemberHeartbeats(now, members);
        } else {
            sendHeartbeatToMaster(now);
            sendHeartbeatToMembers(members);
        }
    }

    private void checkMemberHeartbeats(long now, Collection<MemberImpl> members) {
        List<Address> deadAddresses = null;
        for (MemberImpl memberImpl : members) {
            final Address address = memberImpl.getAddress();
            if (!clusterService.getThisAddress().equals(address)) {
                try {
                    Connection conn = node.connectionManager.getOrConnect(address);
                    boolean connectionIsAlive = conn != null && conn.live();
                    long lastReadDiff = now - memberImpl.getLastRead();
                    boolean isHeartbeatTimedOut = lastReadDiff >= maxNoHeartbeatMillis;
                    boolean noPingForSomeTime = lastReadDiff >= 5000 && (now - memberImpl.getLastPing()) >= 5000;
                    boolean noWriteForSomeTime = (now - memberImpl.getLastWrite()) > 500;
                    Long lastConfirmation = masterConfirmationTimes.get(memberImpl);
                    boolean noMasterConfirmation = lastConfirmation == null
                            || (now - lastConfirmation > maxNoMasterConfirmationMillis);
                    boolean isConnectionMissing = conn == null && lastReadDiff > 5000;
                    if (connectionIsAlive) {
                        if (isHeartbeatTimedOut) {
                            deadAddresses = addToDeadAddresses(deadAddresses, address);
                            logger.warning("Added " + address
                                    + " to list of dead addresses because of timeout since last read");
                        } else if (noPingForSomeTime) {
                            ping(memberImpl);
                        }
                        if (noWriteForSomeTime) {
                            sendHeartbeat(address);
                        }
                        if (noMasterConfirmation) {
                            deadAddresses = addToDeadAddresses(deadAddresses, address);
                            logger.warning("Added " + address
                                    + " to list of dead addresses because it has not sent a master confirmation recently");
                        }
                    } else if (isConnectionMissing) {
                        handleMissingConnection(memberImpl, address);
                    }
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
        }
        if (deadAddresses != null) {
            removeDeadAddresses(deadAddresses);
        }
    }

    private void handleMissingConnection(MemberImpl memberImpl, Address address) {
        logMissingConnection(address);
        memberImpl.didRead();
    }

    private void removeDeadAddresses(List<Address> deadAddresses) {
        for (Address address : deadAddresses) {
            if (logger.isFinestEnabled()) {
                logger.finest("No heartbeat should remove " + address);
            }
            clusterService.removeAddress(address);
        }
    }

    private void sendHeartbeatToMembers(Collection<MemberImpl> members) {
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                Address address = member.getAddress();
                Connection conn = node.connectionManager.getOrConnect(address);
                if (conn != null) {
                    sendHeartbeat(address);
                } else {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Could not connect to " + address + " to send heartbeat");
                    }
                }
            }
        }
    }

    private void sendHeartbeatToMaster(long now) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress != null) {
            node.connectionManager.getOrConnect(masterAddress);
            MemberImpl masterMember = clusterService.getMember(masterAddress);
            boolean removed = false;
            if (masterMember != null) {
                if ((now - masterMember.getLastRead()) >= maxNoHeartbeatMillis) {
                    logger.warning("Master node has timed out its heartbeat and will be removed");
                    clusterService.removeAddress(masterAddress);
                    removed = true;
                } else if ((now - masterMember.getLastRead()) >= 5000 && (now - masterMember.getLastPing()) >= 5000) {
                    ping(masterMember);
                }
            }
            if (!removed) {
                sendHeartbeat(masterAddress);
            }
        }
    }

    private List<Address> addToDeadAddresses(List<Address> deadAddresses, Address address) {
        if (deadAddresses == null) {
            deadAddresses = new ArrayList<Address>();
        }
        deadAddresses.add(address);
        return deadAddresses;
    }


    private void ping(final MemberImpl memberImpl) {
        memberImpl.didPing();
        if (!icmpEnabled) {
            return;
        }
        nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
            public void run() {
                try {
                    final Address address = memberImpl.getAddress();
                    logger.warning(thisAddress + " will ping " + address);
                    for (int i = 0; i < 5; i++) {
                        try {
                            if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeout)) {
                                logger.info(thisAddress + " pings successfully. Target: " + address);
                                return;
                            }
                        } catch (ConnectException ignored) {
                            // no route to host
                            // means we cannot connect anymore
                        }
                    }
                    logger.warning(thisAddress + " couldn't ping " + address);
                    // not reachable.
                    clusterService.removeAddress(address);
                } catch (Throwable ignored) {
                }
            }
        });
    }

    private void sendHeartbeat(Address target) {
        if (target == null) return;
        try {
            node.nodeEngine.getOperationService().send(new HeartbeatOperation(), target);
        } catch (Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("Error while sending heartbeat -> "
                        + e.getClass().getName() + "[" + e.getMessage() + "]");
            }
        }
    }

    private void logMissingConnection(Address address) {
        String msg = node.getLocalMember() + " has no connection to " + address;
        logger.warning(msg);
    }
}
