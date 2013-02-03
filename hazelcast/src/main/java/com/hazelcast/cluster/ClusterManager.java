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

import com.hazelcast.core.Member;
import com.hazelcast.impl.*;
import com.hazelcast.impl.base.Call;
import com.hazelcast.impl.base.PacketProcessor;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.security.Credentials;
import com.hazelcast.util.Clock;
import com.hazelcast.util.Prioritized;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public final class ClusterManager extends BaseManager implements ConnectionListener {

    private final long WAIT_MILLIS_BEFORE_JOIN;

    private final long MAX_WAIT_SECONDS_BEFORE_JOIN;

    private final long MAX_NO_HEARTBEAT_MILLIS;

    private final long MAX_NO_MASTER_CONFIRMATION_MILLIS;

    private final boolean ICMP_ENABLED;

    private final int ICMP_TTL;

    private final int ICMP_TIMEOUT;

    private final Set<ScheduledAction> setScheduledActions = new LinkedHashSet<ScheduledAction>(1000);

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);

    private boolean joinInProgress = false;

    private long timeToStartJoin = 0;

    private long firstJoinRequest = 0;

    private final Map<MemberImpl, Long> memberMasterConfirmationTimes = new HashMap<MemberImpl, Long>();
    
    public ClusterManager(final Node node) {
        super(node);
        WAIT_MILLIS_BEFORE_JOIN = node.groupProperties.WAIT_SECONDS_BEFORE_JOIN.getInteger() * 1000L;
        MAX_WAIT_SECONDS_BEFORE_JOIN = node.groupProperties.MAX_WAIT_SECONDS_BEFORE_JOIN.getInteger();
        MAX_NO_HEARTBEAT_MILLIS = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger() * 1000L;
        long heartbeatIntervalMillis = node.groupProperties.HEARTBEAT_INTERVAL_SECONDS.getInteger() * 1000L;
        MAX_NO_MASTER_CONFIRMATION_MILLIS = node.groupProperties.MAX_NO_MASTER_CONFIRMATION_SECONDS.getInteger() * 1000L;
        long masterConfirmationIntervalMillis = node.groupProperties.MASTER_CONFIRMATION_INTERVAL_SECONDS.getInteger() * 1000L;
        long memberListPublishIntervalMillis = node.groupProperties.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getInteger() * 1000L;
        ICMP_ENABLED = node.groupProperties.ICMP_ENABLED.getBoolean();
        ICMP_TTL = node.groupProperties.ICMP_TTL.getInteger();
        ICMP_TIMEOUT = node.groupProperties.ICMP_TIMEOUT.getInteger();

        registerPeriodicProcessable(new Processable() {
            public void process() {
                heartBeater();
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis);

        registerPeriodicProcessable(new Processable() {
            public void process() {
                sendMasterConfirmation();
            }
        }, masterConfirmationIntervalMillis, masterConfirmationIntervalMillis);

        registerPeriodicProcessable(new Processable() {
            public void process() {
                sendMemberListToOthers();
            }
        }, memberListPublishIntervalMillis, memberListPublishIntervalMillis);

        registerPeriodicProcessable(new Processable() {
            public void process() {
                checkScheduledActions();
            }
        }, 0, 1000);

        final SplitBrainHandler splitBrainHandler = new SplitBrainHandler(node);
        registerPeriodicProcessable(splitBrainHandler,
                splitBrainHandler.getFirstRunDelayMillis(), splitBrainHandler.getNextRunDelayMillis());

        registerPeriodicProcessable(new Processable() {
            public void process() {
                node.clusterService.checkIdle();
            }
        }, 0, 1000);

        node.connectionManager.addConnectionListener(this);
        registerPacketProcessor(ClusterOperation.RESPONSE, new PacketProcessor() {
            public void process(Packet packet) {
                handleResponse(packet);
            }
        });
        registerPacketProcessor(ClusterOperation.HEARTBEAT, new PacketProcessor() {
            public void process(Packet packet) {
                releasePacket(packet);
            }
        });
        registerPacketProcessor(ClusterOperation.LOG, new PacketProcessor() {
            public void process(Packet packet) {
                logger.log(Level.parse(packet.name), toObject(packet.getValueData()).toString());
                releasePacket(packet);
            }
        });
        registerPacketProcessor(ClusterOperation.JOIN_CHECK, new PacketProcessor() {
            public void process(Packet packet) {
                Connection conn = packet.conn;
                Request request = Request.copyFromPacket(packet);
                Data value = request.value;
                request.clearForResponse();
                if (node.isMaster() && node.joined() && node.isActive()) {
                    JoinInfo joinInfo = (JoinInfo) toObject(value);
                    if (joinInfo != null) {
                        try {
                            node.validateJoinRequest(joinInfo);
                            request.response = toData(node.createJoinInfo());
                        } catch (Exception e) {
                            request.response = toData(e);
                        }
                    }
                }
                returnResponse(request, conn);
                releasePacket(packet);
            }
        });
        registerPacketProcessor(ClusterOperation.REMOTELY_PROCESS_AND_RESPOND,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Data data = packet.getValueData();
                        RemotelyProcessable rp = (RemotelyProcessable) toObject(data);
                        rp.setConnection(packet.conn);
                        rp.setNode(node);
                        rp.process();
                        sendResponse(packet);
                    }
                });
        registerPacketProcessor(ClusterOperation.REMOTELY_PROCESS,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Data data = packet.getValueData();
                        RemotelyProcessable rp = (RemotelyProcessable) toObject(data);
                        rp.setConnection(packet.conn);
                        rp.setNode(node);
                        rp.process();
                        releasePacket(packet);
                    }
                });
        registerPacketProcessor(ClusterOperation.REMOTELY_CALLABLE_BOOLEAN,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Boolean result;
                        AbstractRemotelyCallable<Boolean> callable = null;
                        try {
                            Data data = packet.getValueData();
                            callable = (AbstractRemotelyCallable<Boolean>) toObject(data);
                            callable.setConnection(packet.conn);
                            callable.setNode(node);
                            result = callable.call();
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Error processing " + callable, e);
                            result = Boolean.FALSE;
                        }
                        if (result == Boolean.TRUE) {
                            sendResponse(packet);
                        } else {
                            sendResponseFailure(packet);
                        }
                    }
                });
        registerPacketProcessor(ClusterOperation.REMOTELY_CALLABLE_OBJECT,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Object result;
                        AbstractRemotelyCallable<Boolean> callable = null;
                        try {
                            Data data = packet.getValueData();
                            callable = (AbstractRemotelyCallable) toObject(data);
                            callable.setConnection(packet.conn);
                            callable.setNode(node);
                            result = callable.call();
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Error processing " + callable, e);
                            result = null;
                        }
                        if (result != null) {
                            Data value;
                            if (result instanceof Data) {
                                value = (Data) result;
                            } else {
                                value = toData(result);
                            }
                            packet.setValue(value);
                        }
                        sendResponse(packet);
                    }
                });
    }

    private void registerPeriodicProcessable(final Processable p, long delay, long period) {
        delay = delay < 0 ? 0 : delay;
        period = period <= 0 ? 1: period;
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                node.clusterService.enqueuePriorityAndReturn(p);
            }
        }, delay, period, TimeUnit.MILLISECONDS);
    }

    public boolean shouldTryMerge() {
        return !joinInProgress && setJoins.size() == 0;
    }

    public JoinInfo checkJoin(Connection conn) {
        return new JoinCall(conn).checkJoin();
    }

    public void appendState(StringBuffer sbState) {
        sbState.append("\nClusterManager {");
        for (ScheduledAction sa : setScheduledActions) {
            sbState.append("\n\t" + sa + ", from:" + sa.getRequest().caller);
        }
        sbState.append("\n}");
    }

    class JoinCall extends ConnectionAwareOp {
        JoinCall(Connection target) {
            super(target);
        }

        JoinInfo checkJoin() {
            setLocal(ClusterOperation.JOIN_CHECK, "join", null, node.createJoinInfo(), -1, 0);
            doOp();
            return (JoinInfo) getResultAsObject();
        }
    }

    void logMissingConnection(Address address) {
        String msg = thisMember + " has no connection to " + address;
        logAtMaster(Level.WARNING, msg);
        logger.log(Level.WARNING, msg);
    }

    public void logAtMaster(Level level, String msg) {
        Address master = getMasterAddress();
        if (!isMaster() && master != null) {
            Connection connMaster = node.connectionManager.getOrConnect(getMasterAddress());
            if (connMaster != null) {
                Packet packet = obtainPacket(level.toString(), null, toData(msg), ClusterOperation.LOG, 0);
                sendOrReleasePacket(packet, connMaster);
            }
        } else {
            logger.log(level, msg);
        }
    }

    public final void heartBeater() {
        if (!node.joined() || !node.isActive()) return;
        long now = Clock.currentTimeMillis();
        if (isMaster()) {
            List<Address> lsDeadAddresses = null;
            for (MemberImpl memberImpl : lsMembers) {
                final Address address = memberImpl.getAddress();
                if (!thisAddress.equals(address)) {
                    try {
                        Connection conn = node.connectionManager.getOrConnect(address);
                        if (conn != null && conn.live()) {
                            if ((now - memberImpl.getLastRead()) >= (MAX_NO_HEARTBEAT_MILLIS)) {
                                conn = null;
                                if (lsDeadAddresses == null) {
                                    lsDeadAddresses = new ArrayList<Address>();
                                }
                                logger.log(Level.WARNING, "Added " + address
                                                          + " to list of dead addresses because of timeout since last read");
                                lsDeadAddresses.add(address);
                            } else if ((now - memberImpl.getLastRead()) >= 5000 && (now - memberImpl.getLastPing()) >= 5000) {
                                ping(memberImpl);
                            }
                            if ((now - memberImpl.getLastWrite()) > 500) {
                                sendHeartbeat(conn);
                            }
                            Long lastConfirmation = memberMasterConfirmationTimes.get(memberImpl);
                            if (lastConfirmation == null || (now - lastConfirmation > MAX_NO_MASTER_CONFIRMATION_MILLIS)) {
                                if (lsDeadAddresses == null) {
                                    lsDeadAddresses = new ArrayList<Address>();
                                }
                                logger.log(Level.WARNING, "Added " + address
                                                          + " to list of dead addresses because it has not sent a master confirmation recently");
                                lsDeadAddresses.add(address);
                            }
                        } else if (conn == null && (now - memberImpl.getLastRead()) > 5000) {
                            logMissingConnection(address);
                            memberImpl.didRead();
                        }
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, e.getMessage(), e);
                    }
                }
            }
            if (lsDeadAddresses != null) {
                for (Address address : lsDeadAddresses) {
                    logger.log(Level.FINEST, "No heartbeat should remove " + address);
                    doRemoveAddress(address);
                }
            }
        } else {
            // send heartbeat to master
            Address masterAddress = getMasterAddress();
            if (masterAddress != null) {
                final Connection connMaster = node.connectionManager.getOrConnect(masterAddress);
                MemberImpl masterMember = getMember(masterAddress);
                boolean removed = false;
                if (masterMember != null) {
                    if ((now - masterMember.getLastRead()) >= (MAX_NO_HEARTBEAT_MILLIS)) {
                        logger.log(Level.WARNING, "Master node has timed out its heartbeat and will be removed");
                        doRemoveAddress(masterAddress);
                        removed = true;
                    } else if ((now - masterMember.getLastRead()) >= 5000 && (now - masterMember.getLastPing()) >= 5000) {
                        ping(masterMember);
                    }
                }
                if (!removed) {
                    sendHeartbeat(connMaster);
                }
            }
            for (MemberImpl member : lsMembers) {
                if (!member.localMember()) {
                    Address address = member.getAddress();
                    Connection conn = node.connectionManager.getOrConnect(address);
                    if (conn != null) {
                        sendHeartbeat(conn);
                    } else {
                        logger.log(Level.FINEST, "Could not connect to " + address + " to send heartbeat");
                    }
                }
            }
        }
    }
    
    private void sendMasterConfirmation() {
        if (!node.joined() || !node.isActive() || isMaster()) {
            return;
        }
        
        final Address masterAddress = getMasterAddress();
        if (masterAddress == null) {
            logger.log(Level.FINEST, "Could not send MasterConfirmation, master is null!");
            return;
        }
        
        final MemberImpl masterMember = getMember(masterAddress);
        if (masterMember == null) {
            logger.log(Level.FINEST, "Could not send MasterConfirmation, master is null!");
            return;
        }
        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "Sending MasterConfirmation to " + masterMember);
        }
        sendProcessableTo(new MasterConfirmation(), masterAddress);
    }

    public static class MasterConfirmation extends AbstractRemotelyProcessable {

        public void process() {
            final Address endPoint = conn.getEndPoint();
            if (endPoint == null) {
                return;
            }
            final ILogger logger = node.getLogger(MasterConfirmation.class.getName());
            final ClusterManager clusterManager = node.clusterManager;
            final MemberImpl member = clusterManager.getMember(endPoint);
            if (member != null) {
                if (getNode().isMaster()) {
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.log(Level.FINEST, "MasterConfirmation has been received from " + member);
                    }
                    clusterManager.memberMasterConfirmationTimes.put(member, Clock.currentTimeMillis());
                } else {
                    logger.log(Level.WARNING, endPoint + " has sent MasterConfirmation, but this node is not master!");
                }
            } else {
                logger.log(Level.WARNING, "MasterConfirmation has been received from " + endPoint
                          + ", but it is not a member of this cluster!");
                clusterManager.sendProcessableTo(new MemberRemover(clusterManager.thisAddress), conn);
            }
        }
    }

    private void ping(final MemberImpl memberImpl) {
        memberImpl.didPing();
        if (!ICMP_ENABLED) return;
        node.executorManager.executeNow(new Runnable() {
            public void run() {
                try {
                    final Address address = memberImpl.getAddress();
                    logger.log(Level.WARNING, thisAddress + " will ping " + address);
                    for (int i = 0; i < 5; i++) {
                        try {
                            if (address.getInetAddress().isReachable(null, ICMP_TTL, ICMP_TIMEOUT)) {
                                logger.log(Level.INFO, thisAddress + " pings successfully. Target: " + address);
                                return;
                            }
                        } catch (ConnectException ignored) {
                            // no route to host
                            // means we cannot connect anymore
                        }
                            }
                    logger.log(Level.WARNING, thisAddress + " couldn't ping " + address);
                            // not reachable.
                            enqueueAndReturn(new Processable() {
                                public void process() {
                                    doRemoveAddress(address);
                                }
                            });
                } catch (Throwable ignored) {
                }
            }
        });
    }

    void sendHeartbeat(Connection conn) {
        if (conn == null) return;
        Packet packet = obtainPacket("heartbeat", null, null, ClusterOperation.HEARTBEAT, 0);
        sendOrReleasePacket(packet, conn);
    }
    
    private void sendRemoveMemberToOthers(final Address deadAddress) {
        for (MemberImpl member : lsMembers) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadAddress)) {
                sendProcessableTo(new MemberRemover(deadAddress), address);
            }
        }
    }
    
    // Will be called just before this node becomes the master
    private void resetMemberMasterConfirmations() {
        checkServiceThread();
        for (MemberImpl member : lsMembers) {
            memberMasterConfirmationTimes.put(member, Clock.currentTimeMillis());
        }
    }
    
    private void sendMemberListToOthers() {
        checkServiceThread();
        if (!isMaster()) {
            return;
        }
        sendProcessableToAll(new MembersUpdateCall(lsMembers, node.getClusterImpl().getClusterTime()), false);
    }
    
    public void sendClusterMergeToOthers(final Address newTargetAddress) {
    	sendProcessableToAll(new MergeClusters(newTargetAddress), false);
    }

    public void handleMaster(Master master) {
        if (!node.joined() && !thisAddress.equals(master.address)) {
            logger.log(Level.FINEST, "Handling master response: " + master);
            final Address currentMaster = node.getMasterAddress();
            if (currentMaster != null && !currentMaster.equals(master.address)) {
                final Connection conn = node.connectionManager.getConnection(currentMaster);
                if (conn != null && conn.live()) {
                    logger.log(Level.FINEST, "Ignoring master response " + master +
                              " since node has an active master: " + currentMaster);
                    return;
                }
            }
            node.setMasterAddress(master.address);
            node.connectionManager.getOrConnect(master.address);
            if (!sendJoinRequest(master.address, true)) {
                logger.log(Level.WARNING, "Could not create connection to possible master " + master.address);
            }
        }
    }

    public void handleAddRemoveConnection(final AddOrRemoveConnection connection) {
        if (connection.add) { // Just connect to the new address if not connected already.
            if (!connection.address.equals(thisAddress)) {
                node.connectionManager.getOrConnect(connection.address);
            }
        } else { // Remove dead member
            if (connection.address != null) {
                logger.log(Level.FINEST, "Disconnected from " + connection.address + "... will be removed!");
                doRemoveAddress(connection.address);
            }
        } // end of REMOVE CONNECTION
    }

    void doRemoveAddress(Address deadAddress) {
        doRemoveAddress(deadAddress, true);
    }

    void doRemoveAddress(Address deadAddress, boolean destroyConnection) {
        checkServiceThread();
        logger.log(Level.INFO, "Removing Address " + deadAddress);
        if (!node.joined()) {
            node.failedConnection(deadAddress);
            return;
        }
        if (deadAddress.equals(thisAddress))
            return;
        if (deadAddress.equals(getMasterAddress())) {
            if (node.joined()) {
                MemberImpl newMaster = getNextMemberAfter(deadAddress, false, 1);
                if (newMaster != null)
                    node.setMasterAddress(newMaster.getAddress());
                else
                    node.setMasterAddress(null);
            } else {
                node.setMasterAddress(null);
            }
            logger.log(Level.FINEST, "Now Master " + node.getMasterAddress());
        }
        if (isMaster()) {
            setJoins.remove(new MemberInfo(deadAddress));
            resetMemberMasterConfirmations();
        }
        final Connection conn = node.connectionManager.getConnection(deadAddress);
        if (destroyConnection && conn != null) {
            node.connectionManager.destroyConnection(conn);
        }
        MemberImpl deadMember = getMember(deadAddress);
        if (deadMember != null) {
            removeMember(deadMember);
            node.getClusterImpl().setMembers(lsMembers);
            node.concurrentMapManager.syncForDead(deadMember);
            node.blockingQueueManager.syncForDead(deadAddress);
            node.listenerManager.syncForDead(deadAddress);
            node.topicManager.syncForDead(deadAddress);
            // node.getClusterImpl().setMembers(lsMembers); // shifted up to get members in syncForDead methods
            disconnectExistingCalls(deadAddress);
            if (isMaster()) {
                logger.log(Level.FINEST, deadAddress + " is dead. Sending remove to all other members.");
                sendRemoveMemberToOthers(deadAddress);
            }
            logger.log(Level.INFO, this.toString());
        }
    }

    public void disconnectExistingCalls(Address deadAddress) {
        Object[] calls = mapCalls.values().toArray();
        for (Object call : calls) {
            ((Call) call).onDisconnect(deadAddress);
        }
    }

    void handleJoinRequest(JoinRequest joinRequest) {
        final long now = Clock.currentTimeMillis();
        String msg = "Handling join from " + joinRequest.address + ", inProgress: " + joinInProgress
                     + (timeToStartJoin > 0 ? ", timeToStart: " + (timeToStartJoin - now) : "");
        logger.log(Level.FINEST, msg);
        boolean validJoinRequest;
        try {
            validJoinRequest = node.validateJoinRequest(joinRequest);
        } catch (Exception e) {
            validJoinRequest = false;
        }
        final Connection conn = joinRequest.getConnection();
        if (validJoinRequest) {
            final MemberImpl member = getMember(joinRequest.address);
            if (member != null) {
                if (joinRequest.getUuid().equals(member.getUuid())) {
                    String message = "Ignoring join request, member already exists.. => " + joinRequest;
                    logger.log(Level.FINEST, message);

                    // send members update back to node trying to join again...
                    final long clusterTime = node.getClusterImpl().getClusterTime();
                    sendProcessableTo(new MembersUpdateCall(lsMembers, clusterTime), conn);
                    sendProcessableTo(new SyncProcess(), conn);
                    return;
                }
                // If this node is master then remove old member and process join request.
                // If requesting address is equal to master node's address, that means master node
                // somehow disconnected and wants to join back.
                // So drop old member and process join request if this node becomes master.
                if (isMaster() || member.getAddress().equals(getMasterAddress())) {
                    logger.log(Level.WARNING, "New join request has been received from an existing endpoint! => " + member
                                              + " Removing old member and processing join request...");
                    // If existing connection of endpoint is different from current connection
                    // destroy it, otherwise keep it.
//                    final Connection existingConnection = node.connectionManager.getConnection(joinRequest.address);
//                    final boolean destroyExistingConnection = existingConnection != conn;
                    doRemoveAddress(member.getAddress(), false);
                }
            }
            if (!node.getConfig().getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
                if (node.isActive() && node.joined() && node.getMasterAddress() != null && !isMaster()) {
                    sendProcessableTo(new Master(node.getMasterAddress()), conn);
                }
            }
            if (isMaster() && node.joined() && node.isActive()) {
                final MemberInfo newMemberInfo = new MemberInfo(joinRequest.address, joinRequest.nodeType, joinRequest.getUuid());
                if (node.securityContext != null && !setJoins.contains(newMemberInfo)) {
                    final ILogger securityLogger = node.loggingService.getLogger("com.hazelcast.security");
                    final Credentials cr = joinRequest.getCredentials();
                    if (cr == null) {
                        securityLogger.log(Level.SEVERE, "Expecting security credentials " +
                                "but credentials could not be found in JoinRequest!");
                        sendAuthFail(conn);
                        return;
                    } else {
                        try {
                            LoginContext lc = node.securityContext.createMemberLoginContext(cr);
                            lc.login();
                        } catch (LoginException e) {
                            securityLogger.log(Level.SEVERE, "Authentication has failed for " + cr.getPrincipal()
                                    + '@' + cr.getEndpoint() + " => (" + e.getMessage() + ")");
                            securityLogger.log(Level.FINEST, e.getMessage(), e);
                            sendAuthFail(conn);
                            return;
                        }
                    }
                }
                if (joinRequest.to != null && !joinRequest.to.equals(thisAddress)) {
                    sendProcessableTo(new Master(node.getMasterAddress()), conn);
                    return;
                }
                if (!joinInProgress) {
                    if (firstJoinRequest != 0 && now - firstJoinRequest >= MAX_WAIT_SECONDS_BEFORE_JOIN * 1000) {
                        startJoin();
                    } else {
                        if (setJoins.add(newMemberInfo)) {
                            sendProcessableTo(new Master(node.getMasterAddress()), conn);
                            if (firstJoinRequest == 0) {
                                firstJoinRequest = now;
                            }
                            if (now - firstJoinRequest < MAX_WAIT_SECONDS_BEFORE_JOIN * 1000) {
                                timeToStartJoin = now + WAIT_MILLIS_BEFORE_JOIN;
                            }
                        }
                        if (now > timeToStartJoin) {
                            startJoin();
                        }
                    }
                }
            }
        } else {
            conn.close();
        }
    }

    public static class AuthenticationFailureProcessable extends AbstractRemotelyProcessable implements RemotelyProcessable {
        public void process() {
            node.executorManager.executeNow(new Runnable() {
                public void run() {
                    final ILogger logger = node.loggingService.getLogger("com.hazelcast.security");
                    logger.log(Level.SEVERE, "Authentication failed on master node! Node is going to shutdown now!");
                    node.shutdown(true, true);
                }
            });
        }
    }

    private void sendAuthFail(Connection conn) {
        sendProcessableTo(new AuthenticationFailureProcessable(), conn);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        sb.append(lsMembers.size());
        sb.append("] {");
        for (MemberImpl member : lsMembers) {
            sb.append("\n\t").append(member);
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    void joinReset() {
        joinInProgress = false;
        setJoins.clear();
        timeToStartJoin = Clock.currentTimeMillis() + WAIT_MILLIS_BEFORE_JOIN;
        firstJoinRequest = 0;
    }

    public void onRestart() {
        enqueueAndWait(new Processable() {
            public void process() {
                joinReset();
                lsMembers.clear();
                mapMembers.clear();
                dataMemberCount.reset();
                memberMasterConfirmationTimes.clear();
            }
        }, 5);
    }

    public boolean checkAuthorization(String groupName, String groupPassword, Address target) {
        AbstractRemotelyCallable<Boolean> authorizationCall = new AuthorizationCall(groupName, groupPassword);
        AsyncRemotelyBooleanOp op = new NoneMemberAsyncRemotelyBooleanOp(authorizationCall, target, true);
        op.execute();
        return op.getResultAsBoolean();
    }

    public class NoneMemberAsyncRemotelyBooleanOp extends AsyncRemotelyBooleanOp {

        public NoneMemberAsyncRemotelyBooleanOp(final AbstractRemotelyCallable<Boolean> arp,
                                                final Address target, final boolean canTimeout) {
            super(arp, target, canTimeout);
        }

        @Override
        protected boolean memberOnly() {
            return false;
        }
    }

    public class AsyncRemotelyBooleanOp extends TargetAwareOp {
        private final AbstractRemotelyCallable<Boolean> arp;
        private final boolean canTimeout;

        public AsyncRemotelyBooleanOp(final AbstractRemotelyCallable<Boolean> arp,
                                      final Address target, final boolean canTimeout) {
            this.arp = arp;
            this.target = target;
            this.canTimeout = canTimeout;
        }

        public void execute() {
            arp.setNode(node);
            setLocal(ClusterOperation.REMOTELY_CALLABLE_BOOLEAN, "call", null, arp, 0, -1);
            request.setBooleanRequest();
            doOp();
        }

        @Override
        public Address getTarget() {
            return target;
        }

        @Override
        public void onDisconnect(final Address dead) {
            if (dead.equals(target)) {
                removeRemoteCall(getCallId());
                setResult(Boolean.FALSE);
            }
        }

        @Override
        public void doLocalOp() {
            Boolean result;
            try {
                result = arp.call();
                setResult(result);
            } catch (Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
            }
        }

        @Override
        public void setTarget() {
        }

        @Override
        public void redo(int redoTypeCode) {
            removeRemoteCall(getCallId());
            setResult(Boolean.FALSE);
        }

        @Override
        protected void memberDoesNotExist() {
            setResult(Boolean.FALSE);
        }

        @Override
        protected void packetNotSent() {
            setResult(Boolean.FALSE);
        }

        @Override
        protected final boolean canTimeout() {
            return canTimeout;
        }
    }

    public void finalizeJoin() {
        Set<Member> members = node.getClusterImpl().getMembers();
        List<AsyncRemotelyBooleanOp> calls = new ArrayList<AsyncRemotelyBooleanOp>();
        for (Member m : members) {
            MemberImpl member = (MemberImpl) m;
            if (!member.localMember()) {
                AsyncRemotelyBooleanOp op = new AsyncRemotelyBooleanOp(
                        new FinalizeJoin(), member.getAddress(), false);
                op.execute();
                calls.add(op);
            }
        }
        for (AsyncRemotelyBooleanOp call : calls) {
            call.getResultAsBoolean();
        }
    }

    class JoinRunnable extends FallThroughRunnable implements Prioritized {

        final MembersUpdateCall membersUpdate;

        JoinRunnable(MembersUpdateCall membersUpdate) {
            this.membersUpdate = membersUpdate;
        }

        @Override
        public void doRun() {
            Collection<MemberInfo> lsMemberInfos = membersUpdate.getMemberInfos();
            List<Address> newMemberList = new ArrayList<Address>(lsMemberInfos.size());
            for (final MemberInfo memberInfo : lsMemberInfos) {
                newMemberList.add(memberInfo.address);
            }
            doCall(membersUpdate, newMemberList, true);
            systemLogService.logJoin("JoinRunnable update members done.");
            doCall(new SyncProcess(), newMemberList, false);
            systemLogService.logJoin("JoinRunnable sync done.");
            doCall(new ConnectionCheckCall(), newMemberList, false);
            systemLogService.logJoin("JoinRunnable connection check done.");
        }

        void doCall(AbstractRemotelyCallable callable, List<Address> targets, boolean ignoreThis) {
            List<AsyncRemotelyBooleanOp> calls = new ArrayList<AsyncRemotelyBooleanOp>(targets.size());
            for (final Address target : targets) {
                boolean skip = ignoreThis && thisAddress.equals(target);
                if (!skip) {
                    AsyncRemotelyBooleanOp op = new AsyncRemotelyBooleanOp(callable, target, false);
                    op.execute();
                    calls.add(op);
                }
            }
            for (AsyncRemotelyBooleanOp call : calls) {
                if (!call.getResultAsBoolean(5)) {
                    targets.remove(call.getTarget());
                }
            }
        }
    }

    void startJoin() {
        logger.log(Level.FINEST, "Starting Join.");
        joinInProgress = true;
        final MembersUpdateCall membersUpdate = new MembersUpdateCall(lsMembers, node.getClusterImpl().getClusterTime());
        if (setJoins != null && setJoins.size() > 0) {
            for (MemberInfo memberJoined : setJoins) {
                membersUpdate.addMemberInfo(memberJoined);
            }
        }
        membersUpdate.setNode(node);
        membersUpdate.call();
        node.executorManager.executeNow(new JoinRunnable(membersUpdate));
    }

    void updateMembers(Collection<MemberInfo> lsMemberInfos) {
        checkServiceThread();
        final Map<Address, MemberImpl> mapOldMembers = new HashMap<Address, MemberImpl>();
        for (MemberImpl member : lsMembers) {
            mapOldMembers.put(member.getAddress(), member);
        }
        if (mapOldMembers.size() == lsMemberInfos.size()) {
            boolean same = true;
            for (MemberInfo memberInfo : lsMemberInfos) {
                MemberImpl member = mapOldMembers.get(memberInfo.getAddress());
                if (member == null || !member.getUuid().equals(memberInfo.uuid)) {
                    same = false;
                    break;
                }
            }
            if (same) {
                logger.log(Level.FINEST, "No need to process member update...");
                return;
            }
        }

        logger.log(Level.FINEST, "Updating Members");
        lsMembers.clear();
        dataMemberCount.reset();
        mapMembers.clear();
        memberMasterConfirmationTimes.clear();
        for (MemberInfo memberInfo : lsMemberInfos) {
            MemberImpl member = mapOldMembers.get(memberInfo.address);
            if (member == null) {
                member = addMember(memberInfo.address, memberInfo.nodeType, memberInfo.uuid);
            } else {
                addMember(member);
            }
            member.didRead();
        }
        if (!lsMembers.contains(thisMember)) {
            throw new RuntimeException("Member list doesn't contain local member!");
        }
        heartBeater();
        node.getClusterImpl().setMembers(lsMembers);
        node.setJoined();
        logger.log(Level.INFO, this.toString());
    }

    public boolean sendJoinRequest(Address toAddress, boolean withCredentials) {
        if (toAddress == null) {
            toAddress = node.getMasterAddress();
        }
        logger.log(Level.FINEST, "Sending join request to " + toAddress);
        final boolean send = sendProcessableTo(node.createJoinInfo(withCredentials), toAddress);
        if (!send) {
            logger.log(Level.WARNING, "Could not send join request to " + toAddress);
        }
        return send;
    }

    public void registerScheduledAction(ScheduledAction scheduledAction) {
        setScheduledActions.add(scheduledAction);
    }

    public void deregisterScheduledAction(ScheduledAction scheduledAction) {
        setScheduledActions.remove(scheduledAction);
    }

    public void checkScheduledActions() {
        if (!node.joined() || !node.isActive()) return;
        if (setScheduledActions.size() > 0) {
            Iterator<ScheduledAction> it = setScheduledActions.iterator();
            while (it.hasNext()) {
                ScheduledAction sa = it.next();
                if (sa.expired() && sa.isValid()) {
                    sa.onExpire();
                    it.remove();
                } else if (!sa.isValid()) {
                    it.remove();
                }
            }
        }
    }

    public void invalidateScheduledActionsFor(Address endpoint, Set<Integer> threadIds) {
        if (!node.joined() || !node.isActive()) return;
        if (setScheduledActions.size() > 0) {
            Iterator<ScheduledAction> it = setScheduledActions.iterator();
            while (it.hasNext()) {
                ScheduledAction sa = it.next();
                Request request = sa.getRequest();
                if (endpoint.equals(request.caller) && threadIds.contains(request.lockThreadId)) {
                    sa.setValid(false);
                    it.remove();
                }
            }
        }
    }

    public void connectionAdded(final Connection connection) {
        enqueueAndReturn(new Processable() {
            public void process() {
                MemberImpl member = getMember(connection.getEndPoint());
                if (member != null) {
                    member.didRead();
                }
            }
        });
    }

    public void connectionRemoved(Connection connection) {
        logger.log(Level.FINEST, "Connection is removed " + connection.getEndPoint());
        if (!node.joined()) {
            if (getMasterAddress() != null) {
                if (getMasterAddress().equals(connection.getEndPoint())) {
                    node.setMasterAddress(null);
                }
            }
        }
    }

    public Member addMember(MemberImpl member) {
        return addMember(true, member);
    }

    public Member addMember(boolean checkServiceThread, MemberImpl member) {
        if (checkServiceThread) {
            checkServiceThread();
        }
        logger.log(Level.FINEST, "ClusterManager adding " + member);
        if (lsMembers.contains(member)) {
            for (MemberImpl m : lsMembers) {
                if (m.equals(member)) {
                    member = m;
                }
            }
            mapMembers.put(member.getAddress(), member);
        } else {
            lsMembers.add(member);
            mapMembers.put(member.getAddress(), member);
            if (!member.isLiteMember()) {
                dataMemberCount.increment();
            }
        }
       
        memberMasterConfirmationTimes.put(member, Clock.currentTimeMillis());
        
        return member;
    }

    public void removeMember(MemberImpl member) {
        checkServiceThread();
        logger.log(Level.FINEST, "ClusterManager removing  " + member);
        mapMembers.remove(member.getAddress());
        lsMembers.remove(member);
        memberMasterConfirmationTimes.remove(member);
        if (!member.isLiteMember()) {
            dataMemberCount.decrement();
        }
    }

    protected MemberImpl createMember(Address address, NodeType nodeType, String nodeUuid, String ipV6ScopeId) {
        address.setScopeId(ipV6ScopeId);
        return new MemberImpl(address, thisAddress.equals(address), nodeType, nodeUuid);
    }

    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        return mapMembers.get(address);
    }

    final public MemberImpl addMember(Address address, NodeType nodeType, String nodeUuid) {
        checkServiceThread();
        if (address == null) {
            logger.log(Level.FINEST, "Address cannot be null");
            return null;
        }
        MemberImpl member = getMember(address);
        if (member == null) {
            member = createMember(address, nodeType, nodeUuid, thisAddress.getScopeId());
        }
        addMember(member);
        return member;
    }

    public void stop() {
        if (setJoins != null) {
            setJoins.clear();
        }
        timeToStartJoin = 0;
        if (lsMembers != null) {
            lsMembers.clear();
        }
        dataMemberCount.reset();
        if (mapMembers != null) {
            mapMembers.clear();
        }
        if (mapCalls != null) {
            mapCalls.clear();
        }
        if (memberMasterConfirmationTimes != null) {
            memberMasterConfirmationTimes.clear();
        }
    }
}
