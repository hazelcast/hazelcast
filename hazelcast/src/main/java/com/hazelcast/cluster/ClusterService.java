/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.cluster.client.ClientAuthenticateHandler;
import com.hazelcast.cluster.client.GetMembersHandler;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.ExecutedBy;
import com.hazelcast.spi.annotation.ThreadType;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;


public final class ClusterService implements CoreService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener>, ClientProtocolService {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    private final Node node;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    protected final Address thisAddress;

    protected final MemberImpl thisMember;

    private final long waitMillisBeforeJoin;

    private final long maxWaitSecondsBeforeJoin;

    private final long maxNoHeartbeatMillis;

    private final long maxNoMasterConfirmationMillis;

    private final boolean icmpEnabled;

    private final int icmpTtl;

    private final int icmpTimeout;

    private final Lock lock = new ReentrantLock();

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);

    private final AtomicReference<Map<Address, MemberImpl>> membersRef = new AtomicReference<Map<Address, MemberImpl>>();

    private final AtomicInteger dataMemberCount = new AtomicInteger(); // excluding lite members

    private boolean joinInProgress = false;

    private long timeToStartJoin = 0;

    private long firstJoinRequest = 0;

    private final ConcurrentMap<MemberImpl, Long> masterConfirmationTimes = new ConcurrentHashMap<MemberImpl, Long>();

    private volatile long clusterTimeDiff = Long.MAX_VALUE;

    private final Map<Command, ClientCommandHandler> commandHandlers = new HashMap<Command, ClientCommandHandler>();

    public ClusterService(final Node node) {
        this.node = node;
        nodeEngine = node.nodeEngine;
        logger = node.getLogger(getClass().getName());
        thisAddress = node.getThisAddress();
        thisMember = node.getLocalMember();
        waitMillisBeforeJoin = node.groupProperties.WAIT_SECONDS_BEFORE_JOIN.getInteger() * 1000L;
        maxWaitSecondsBeforeJoin = node.groupProperties.MAX_WAIT_SECONDS_BEFORE_JOIN.getInteger();
        maxNoHeartbeatMillis = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger() * 1000L;
        maxNoMasterConfirmationMillis = node.groupProperties.MAX_NO_MASTER_CONFIRMATION_SECONDS.getInteger() * 1000L;
        icmpEnabled = node.groupProperties.ICMP_ENABLED.getBoolean();
        icmpTtl = node.groupProperties.ICMP_TTL.getInteger();
        icmpTimeout = node.groupProperties.ICMP_TIMEOUT.getInteger();
        node.connectionManager.addConnectionListener(this);
    }

    public void init(final NodeEngine nodeEngine, Properties properties) {
        final long mergeFirstRunDelay = node.getGroupProperties().MERGE_FIRST_RUN_DELAY_SECONDS.getLong();
        final long mergeNextRunDelay = node.getGroupProperties().MERGE_NEXT_RUN_DELAY_SECONDS.getLong();
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new SplitBrainHandler(node),
                mergeFirstRunDelay, mergeNextRunDelay, TimeUnit.SECONDS);
        final long heartbeatInterval = node.groupProperties.HEARTBEAT_INTERVAL_SECONDS.getInteger();
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new Runnable() {
            public void run() {
//                heartBeater();
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);
        final long masterConfirmationInterval = node.groupProperties.MASTER_CONFIRMATION_INTERVAL_SECONDS.getInteger();
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new Runnable() {
            public void run() {
                sendMasterConfirmation();
            }
        }, masterConfirmationInterval, masterConfirmationInterval, TimeUnit.SECONDS);
        final long memberListPublishInterval = node.groupProperties.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getInteger();
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new Runnable() {
            public void run() {
                sendMemberListToOthers();
            }
        }, memberListPublishInterval, memberListPublishInterval, TimeUnit.SECONDS);
        registerClientOperationHandlers();
    }

    private void registerClientOperationHandlers() {
        registerHandler(Command.AUTH, new ClientAuthenticateHandler(nodeEngine));
        registerHandler(Command.MEMBERS, new GetMembersHandler(nodeEngine));
    }

    void registerHandler(Command command, ClientCommandHandler handler) {
        commandHandlers.put(command, handler);
    }

    public boolean isJoinInProgress() {
        lock.lock();
        try {
            return joinInProgress || !setJoins.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    public JoinInfo checkJoinInfo(Address target) {
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                new JoinCheckOperation(node.createJoinInfo()), target)
                .setTryCount(1).build();
        try {
            return (JoinInfo) nodeEngine.toObject(inv.invoke().get());
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error during join check!", e);
        }
        return null;
    }

    public boolean validateJoinRequest(JoinRequest joinRequest) throws Exception {
        boolean valid = Packet.PACKET_VERSION == joinRequest.packetVersion;
        if (valid) {
            try {
                valid = node.getConfig().isCompatible(joinRequest.config);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Invalid join request, reason:" + e.getMessage());
                node.getSystemLogService().logJoin("Invalid join request, reason:" + e.getMessage());
                throw e;
            }
        }
        return valid;
    }

    private void logMissingConnection(Address address) {
        String msg = node.getLocalMember() + " has no connection to " + address;
        logAtMaster(Level.WARNING, msg);
        logger.log(Level.WARNING, msg);
    }

    private void logAtMaster(Level level, String msg) {
//        Address master = node.getMasterAddress();
//        if (!node.isMaster() && master != null) {
//            Connection connMaster = node.connectionManager.getOrConnect(node.getMasterAddress());
//            if (connMaster != null) {
//                Packet packet = new Packet();
//                packet.set(level.toString(), null, toData(msg), ClusterOperation.LOG);
//                packet.timeout = 0;
//                send(packet, connMaster);
//            }
//        } else {
//            logger.log(level, msg);
//        }
    }

    public final void heartBeater() {
        if (!node.joined() || !node.isActive()) return;
        long now = Clock.currentTimeMillis();
        final Collection<MemberImpl> members = getMemberList();
        if (node.isMaster()) {
            List<Address> deadAddresses = null;
            for (MemberImpl memberImpl : members) {
                final Address address = memberImpl.getAddress();
                if (!thisAddress.equals(address)) {
                    try {
                        Connection conn = node.connectionManager.getOrConnect(address);
                        if (conn != null && conn.live()) {
                            if ((now - memberImpl.getLastRead()) >= (maxNoHeartbeatMillis)) {
                                if (deadAddresses == null) {
                                    deadAddresses = new ArrayList<Address>();
                                }
                                logger.log(Level.WARNING, "Added " + address + " to list of dead addresses because of timeout since last read");
                                deadAddresses.add(address);
                            } else if ((now - memberImpl.getLastRead()) >= 5000 && (now - memberImpl.getLastPing()) >= 5000) {
                                ping(memberImpl);
                            }
                            if ((now - memberImpl.getLastWrite()) > 500) {
                                sendHeartbeat(address);
                            }
                            Long lastConfirmation = masterConfirmationTimes.get(memberImpl);
                            if (lastConfirmation == null ||
                                    (now - lastConfirmation > maxNoMasterConfirmationMillis)) {
                                if (deadAddresses == null) {
                                    deadAddresses = new ArrayList<Address>();
                                }
                                logger.log(Level.WARNING, "Added " + address +
                                        " to list of dead addresses because it has not sent a master confirmation recently");
                                deadAddresses.add(address);
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
            if (deadAddresses != null) {
                for (Address address : deadAddresses) {
                    logger.log(Level.FINEST, "No heartbeat should remove " + address);
                    removeAddress(address);
                }
            }
        } else {
            // send heartbeat to master
            Address masterAddress = node.getMasterAddress();
            if (masterAddress != null) {
                node.connectionManager.getOrConnect(masterAddress);
                MemberImpl masterMember = getMember(masterAddress);
                boolean removed = false;
                if (masterMember != null) {
                    if ((now - masterMember.getLastRead()) >= (maxNoHeartbeatMillis)) {
                        logger.log(Level.WARNING, "Master node has timed out its heartbeat and will be removed");
                        removeAddress(masterAddress);
                        removed = true;
                    } else if ((now - masterMember.getLastRead()) >= 5000 && (now - masterMember.getLastPing()) >= 5000) {
                        ping(masterMember);
                    }
                }
                if (!removed) {
                    sendHeartbeat(masterAddress);
                }
            }
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    Address address = member.getAddress();
                    Connection conn = node.connectionManager.getOrConnect(address);
                    if (conn != null) {
                        sendHeartbeat(address);
                    } else {
                        logger.log(Level.FINEST, "Could not connect to " + address + " to send heartbeat");
                    }
                }
            }
        }
    }

    private void ping(final MemberImpl memberImpl) {
        memberImpl.didPing();
        if (!icmpEnabled) return;
        nodeEngine.getExecutionService().execute("hz:system", new Runnable() {
            public void run() {
                try {
                    final Address address = memberImpl.getAddress();
                    logger.log(Level.WARNING, thisAddress + " will ping " + address);
                    for (int i = 0; i < 5; i++) {
                        try {
                            if (address.getInetAddress().isReachable(null, icmpTtl, icmpTimeout)) {
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
                    removeAddress(address);
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
            logger.log(Level.FINEST, "Error while sending heartbeat -> "
                    + e.getClass().getName() + "[" + e.getMessage() + "]");
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
        invokeClusterOperation(new MasterConfirmationOperation(), masterAddress);
    }

    // Will be called just before this node becomes the master
    private void resetMemberMasterConfirmations() {
        final Collection<MemberImpl> memberList = getMemberList();
        for (MemberImpl member : memberList) {
            masterConfirmationTimes.put(member, Clock.currentTimeMillis());
        }
    }

    private void sendMemberListToOthers() {
        if (!isMaster()) {
            return;
        }
        final Collection<MemberImpl> members = getMemberList();
        MemberInfoUpdateOperation op = new MemberInfoUpdateOperation(createMemberInfos(members), getClusterTime());
        for (MemberImpl member : members) {
            if (member.equals(thisMember)) {
                continue;
            }
            invokeClusterOperation(op, member.getAddress());
        }
    }

    public void removeAddress(Address deadAddress) {
        doRemoveAddress(deadAddress, true);
    }

    private void doRemoveAddress(Address deadAddress, boolean destroyConnection) {
        logger.log(Level.INFO, "Removing Address " + deadAddress);
        if (!node.joined()) {
            node.failedConnection(deadAddress);
            return;
        }
        if (deadAddress.equals(thisAddress)) {
            return;
        }
        lock.lock();
        try {
            if (deadAddress.equals(node.getMasterAddress())) {
                assignNewMaster();
            }
            if (node.isMaster()) {
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
                nodeEngine.onMemberLeft(deadMember);
                logger.log(Level.INFO, membersString());
            }
        } finally {
            lock.unlock();
        }
    }

    private void assignNewMaster() {
        final Address oldMasterAddress = node.getMasterAddress();
        logger.log(Level.FINEST, "Master " + oldMasterAddress + " is dead...");
        if (node.joined()) {
            final Collection<MemberImpl> members = getMemberList();
            MemberImpl newMaster = null;
            final int size = members.size();
            if (size > 1) {
                final Iterator<MemberImpl> iter = members.iterator();
                final MemberImpl member = iter.next();
                if (member.getAddress().equals(oldMasterAddress)) {
                    newMaster = iter.next();
                } else {
                    logger.log(Level.SEVERE, "Old master " + oldMasterAddress
                                             + " is dead but the first of member list is a different member " +
                                             member + "!");
                    newMaster = member;
                }
            } else {
                logger.log(Level.WARNING, "Old master is dead and this node is not master " +
                                          "but member list contains only " + size + " members! -> " + members);
            }
            if (newMaster != null) {
                node.setMasterAddress(newMaster.getAddress());
            } else {
                node.setMasterAddress(null);
            }
        } else {
            node.setMasterAddress(null);
        }
        logger.log(Level.FINEST, "Now Master " + node.getMasterAddress());
    }

    @ExecutedBy(ThreadType.EXECUTOR_THREAD)
    void handleJoinRequest(JoinRequest joinRequest) {
        lock.lock();
        try {
            final long now = Clock.currentTimeMillis();
            String msg = "Handling join from " + joinRequest.address + ", inProgress: " + joinInProgress
                         + (timeToStartJoin > 0 ? ", timeToStart: " + (timeToStartJoin - now) : "");
            logger.log(Level.FINEST, msg);
            boolean validJoinRequest;
            try {
                validJoinRequest = validateJoinRequest(joinRequest);
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
                        invokeClusterOperation(new FinalizeJoinOperation(createMemberInfos(getMemberList()), getClusterTime()),
                                member.getAddress());
                        return;
                    }
                    // If this node is master then remove old member and process join request.
                    // If requesting address is equal to master node's address, that means master node
                    // somehow disconnected and wants to join back.
                    // So drop old member and process join request if this node becomes master.
                    if (node.isMaster() || member.getAddress().equals(node.getMasterAddress())) {
                        logger.log(Level.WARNING, "New join request has been received from an existing endpoint! => " + member
                                                  + " Removing old member and processing join request...");
                        // If existing connection of endpoint is different from current connection
                        // destroy it, otherwise keep it.
//                    final Connection existingConnection = node.connectionManager.getConnection(joinRequest.address);
//                    final boolean destroyExistingConnection = existingConnection != conn;
                        doRemoveAddress(member.getAddress(), false);
                    }
                }
                final boolean multicastEnabled = node.getConfig().getNetworkConfig().getJoin().getMulticastConfig().isEnabled();
                if (!multicastEnabled && node.isActive() && node.joined() && node.getMasterAddress() != null && !node.isMaster()) {
                    sendMasterAnswer(joinRequest);
                }
                if (node.isMaster() && node.joined() && node.isActive()) {
                    final MemberInfo newMemberInfo = new MemberInfo(joinRequest.address, joinRequest.nodeType,
                            joinRequest.getUuid());
                    if (node.securityContext != null && !setJoins.contains(newMemberInfo)) {
                        final Credentials cr = joinRequest.getCredentials();
                        ILogger securityLogger = node.loggingService.getLogger("com.hazelcast.security");
                        if (cr == null) {
                            securityLogger.log(Level.SEVERE, "Expecting security credentials " +
                                    "but credentials could not be found in JoinRequest!");
                            invokeClusterOperation(new AuthenticationFailureOperation(), joinRequest.address);
                            return;
                        } else {
                            try {
                                LoginContext lc = node.securityContext.createMemberLoginContext(cr);
                                lc.login();
                            } catch (LoginException e) {
                                securityLogger.log(Level.SEVERE, "Authentication has failed for " + cr.getPrincipal()
                                        + '@' + cr.getEndpoint() + " => (" + e.getMessage() +
                                        ")");
                                securityLogger.log(Level.FINEST, e.getMessage(), e);
                                invokeClusterOperation(new AuthenticationFailureOperation(), joinRequest.address);
                                return;
                            }
                        }
                    }
                    if (joinRequest.to != null && !joinRequest.to.equals(thisAddress)) {
                        sendMasterAnswer(joinRequest);
                        return;
                    }
                    if (!joinInProgress) {
                        if (firstJoinRequest != 0 && now - firstJoinRequest >= maxWaitSecondsBeforeJoin * 1000) {
                            startJoin();
                        } else {
                            if (setJoins.add(newMemberInfo)) {
                                sendMasterAnswer(joinRequest);
                                if (firstJoinRequest == 0) {
                                    firstJoinRequest = now;
                                }
                                if (now - firstJoinRequest < maxWaitSecondsBeforeJoin * 1000) {
                                    timeToStartJoin = now + waitMillisBeforeJoin;
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
        } finally {
            lock.unlock();
        }
    }

    private void sendMasterAnswer(final JoinRequest joinRequest) {
        invokeClusterOperation(new SetMasterOperation(node.getMasterAddress()), joinRequest.address);
    }

    void acceptMasterConfirmation(MemberImpl member) {
        if (member != null) {
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "MasterConfirmation has been received from " + member);
            }
            masterConfirmationTimes.put(member, Clock.currentTimeMillis());
        }
    }

    private void joinReset() {
        lock.lock();
        try {
            joinInProgress = false;
            setJoins.clear();
            timeToStartJoin = Clock.currentTimeMillis() + waitMillisBeforeJoin;
            firstJoinRequest = 0;
        } finally {
            lock.unlock();
        }
    }

    public void onRestart() {
        joinReset();
        membersRef.set(null);
        dataMemberCount.set(0);
        masterConfirmationTimes.clear();
    }

    void startJoin() {
        logger.log(Level.FINEST, "Starting Join.");
        lock.lock();
        try {
            joinInProgress = true;
            final Collection<MemberImpl> members = getMemberList();
            final Collection<MemberInfo> memberInfos = createMemberInfos(members);
            for (MemberImpl member : members) {
                memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
            }
            for (MemberInfo memberJoining : setJoins) {
                memberInfos.add(memberJoining);
            }
            final long time = getClusterTime();
            final MemberInfoUpdateOperation memberInfoUpdateOp = new MemberInfoUpdateOperation(memberInfos, time, true);
            // Post join operations must be lock free; means no locks at all;
            // no partition locks, no key-based locks, no service level locks!
            final Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
            final PostJoinOperation postJoinOp = postJoinOps != null && postJoinOps.length > 0
                    ? new PostJoinOperation(postJoinOps) : null;
            final FinalizeJoinOperation finalizeJoinOp = new FinalizeJoinOperation(memberInfos, postJoinOp, time);
            final List<Future> calls = new ArrayList<Future>(members.size());
            for (MemberInfo member : setJoins) {
                calls.add(invokeClusterOperation(finalizeJoinOp, member.getAddress()));
            }
            for (MemberImpl member : members) {
                if (!member.getAddress().equals(thisAddress)) {
                    calls.add(invokeClusterOperation(memberInfoUpdateOp, member.getAddress()));
                }
            }
            updateMembers(memberInfos);
            for (Future future : calls) {
                try {
                    future.get(3, TimeUnit.SECONDS);
                } catch (TimeoutException ignored) {
                    logger.log(Level.FINEST, "Finalize join call timed-out: " + future);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "While waiting finalize join calls...", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private static Collection<MemberInfo> createMemberInfos(Collection<MemberImpl> members) {
        final Collection<MemberInfo> memberInfos = new LinkedList<MemberInfo>();
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
        }
        return memberInfos;
    }

    void updateMembers(Collection<MemberInfo> members) {
        lock.lock();
        try {
            Map<Address, MemberImpl> mapOldMembers = new HashMap<Address, MemberImpl>();
            for (MemberImpl member : getMemberList()) {
                mapOldMembers.put(member.getAddress(), member);
            }
            if (mapOldMembers.size() == members.size()) {
                boolean same = true;
                for (MemberInfo memberInfo : members) {
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
            MemberImpl[] newMembers = new MemberImpl[members.size()];
            int k = 0;
            for (MemberInfo memberInfo : members) {
                MemberImpl member = mapOldMembers.get(memberInfo.address);
                if (member == null) {
                    member = createMember(memberInfo.address, memberInfo.nodeType, memberInfo.uuid,
                            thisAddress.getScopeId());
                }
                newMembers[k++] = member;
                member.didRead();
            }
            addMembers(newMembers);
            if (!getMemberList().contains(thisMember)) {
                throw new RuntimeException("Member list doesn't contain local member!");
            }
            joinReset();
            heartBeater();
            node.setJoined();
            logger.log(Level.INFO, membersString());
        } finally {
            lock.unlock();
        }
    }

    public boolean sendJoinRequest(Address toAddress, boolean withCredentials) {
        if (toAddress == null) {
            toAddress = node.getMasterAddress();
        }
        JoinRequest joinRequest = node.createJoinInfo(withCredentials);
        invokeClusterOperation(joinRequest, toAddress);
        return true;
    }

    public void connectionAdded(final Connection connection) {
        MemberImpl member = getMember(connection.getEndPoint());
        if (member != null) {
            member.didRead();
        }
    }

    public void connectionRemoved(Connection connection) {
        logger.log(Level.FINEST, "Connection is removed " + connection.getEndPoint());
        final Address masterAddress = node.getMasterAddress();
        if (!node.joined()) {
            if (masterAddress != null && masterAddress.equals(connection.getEndPoint())) {
                node.setMasterAddress(null);
            }
        }
    }

    Future invokeClusterOperation(Operation op, Address target) {
        return nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(5).build().invoke();
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public void addMember(MemberImpl member) {
        addMembers(member);
    }

    public void addMembers(MemberImpl... members) {
        if (members == null || members.length == 0) return;
        logger.log(Level.FINEST, "Adding members -> " + Arrays.toString(members));
        lock.lock();
        try {
            Collection<MemberImpl> newMembers = new LinkedList<MemberImpl>();
            Map<Address, MemberImpl> memberMap = membersRef.get();
            if (memberMap == null) {
                memberMap = new LinkedHashMap<Address, MemberImpl>();  // ! ORDERED !
            } else {
                memberMap = new LinkedHashMap<Address, MemberImpl>(memberMap);  // ! ORDERED !
            }
            for (MemberImpl member : members) {
                MemberImpl oldMember = memberMap.remove(member.getAddress());
                if (oldMember == null) {
                    newMembers.add(member);
                    masterConfirmationTimes.put(member, Clock.currentTimeMillis());
                } else if (!oldMember.isLiteMember()) {
                    dataMemberCount.decrementAndGet();
                }
                memberMap.put(member.getAddress(), member);
                if (!member.isLiteMember()) {
                    dataMemberCount.incrementAndGet();
                }
            }
            setMembers(memberMap);
            for (MemberImpl member : newMembers) {
                node.getPartitionService().memberAdded(member); // sync call
                sendMembershipEventNotifications(member, true);
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeMember(MemberImpl deadMember) {
        logger.log(Level.FINEST, "Removing  " + deadMember);
        lock.lock();
        try {
            final Map<Address, MemberImpl> members = membersRef.get();
            if (members != null && members.containsKey(deadMember.getAddress())) {
                Map<Address, MemberImpl> newMembers = new LinkedHashMap<Address, MemberImpl>(members);  // ! ORDERED !
                newMembers.remove(deadMember.getAddress());
                masterConfirmationTimes.remove(deadMember);
                if (!deadMember.isLiteMember()) {
                    dataMemberCount.decrementAndGet();
                }
                setMembers(newMembers);
                node.getPartitionService().memberRemoved(deadMember); // sync call
                if (node.isMaster()) {
                    logger.log(Level.FINEST, deadMember + " is dead. Sending remove to all other members.");
                    invokeMemberRemoveOperation(deadMember);
                }
                sendMembershipEventNotifications(deadMember, false); // async events
            }
        } finally {
            lock.unlock();
        }
    }

    private void invokeMemberRemoveOperation(final MemberImpl deadMember) {
        final Address deadAddress = deadMember.getAddress();
        final Collection<Future> responses = new LinkedList<Future>();
        for (MemberImpl member : getMemberList()) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadAddress)) {
                Future f = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                        new MemberRemoveOperation(deadAddress), address)
                        .setTryCount(10).setTryPauseMillis(100).build().invoke();
                responses.add(f);
            }
        }
        for (Future response : responses) {
            try {
                response.get(1, TimeUnit.SECONDS);
            } catch (Throwable e) {
                logger.log(Level.FINEST, e.getMessage(), e);
            }
        }
    }

    private void sendMembershipEventNotifications(final MemberImpl member, final boolean added) {
        final int eventType = added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED;
        final MembershipEvent membershipEvent = new MembershipEvent(member, eventType);
        final EventService eventService = nodeEngine.getEventService();
        final Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            eventService.executeEvent(new Runnable() {
                final MembershipServiceEvent event = new MembershipServiceEvent(member, eventType);

                public void run() {
                    for (MembershipAwareService service : membershipAwareServices) {
                        if (added) {
                            service.memberAdded(event);
                        } else {
                            service.memberRemoved(event);
                        }
                    }
                }
            });
        }
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        eventService.publishEvent(SERVICE_NAME, registrations, membershipEvent);
    }

    protected MemberImpl createMember(Address address, NodeType nodeType, String nodeUuid, String ipV6ScopeId) {
        address.setScopeId(ipV6ScopeId);
        return new MemberImpl(address, thisAddress.equals(address), nodeType, nodeUuid);
    }

    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        return membersRef.get().get(address);
    }

    private void setMembers(final Map<Address, MemberImpl> memberMap) {
        final Map<Address, MemberImpl> members = Collections.unmodifiableMap(memberMap);
        // make values(), keySet() and entrySet() to be cached
        members.values();
        members.keySet();
        members.entrySet();
        membersRef.set(members);
    }

    public Collection<MemberImpl> getMemberList() {
        final Map<Address, MemberImpl> map = membersRef.get();
        return map != null ? Collections.unmodifiableCollection(map.values()) : null;
    }

    public void reset() {
        lock.lock();
        try {
            setJoins.clear();
            timeToStartJoin = 0;
            membersRef.set(null);
            dataMemberCount.set(0);
            masterConfirmationTimes.clear();
        } finally {
            lock.unlock();
        }
    }

    public void destroy() {
        reset();
    }

    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    public boolean isMaster() {
        return node.isMaster();
    }

    public Address getThisAddress() {
        return thisAddress;
    }

    public Member getLocalMember() {
        return node.getLocalMember();
    }

    public Set<Member> getMembers() {
        final Collection<MemberImpl> members = getMemberList();
        return members != null ? new LinkedHashSet<Member>(members) : new HashSet<Member>(0);
    }

    public int getSize() {
        final Collection<MemberImpl> members = getMemberList();
        return members != null ? members.size() : 0;
    }

    public long getClusterTime() {
        return Clock.currentTimeMillis() + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public void setMasterTime(long masterTime) {
        long diff = masterTime - Clock.currentTimeMillis();
        if (Math.abs(diff) < Math.abs(clusterTimeDiff)) {
            this.clusterTimeDiff = diff;
        }
    }

    public long getClusterTimeFor(long localTime) {
        return localTime + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public void addMembershipListener(MembershipListener listener) {
        nodeEngine.getEventService().registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
    }

    public void removeMembershipListener(MembershipListener listener) {
        //listeners.remove(listener);
    }

    public void dispatchEvent(MembershipEvent event, MembershipListener listener) {
        if (event.getEventType() == MembershipEvent.MEMBER_ADDED) {
            listener.memberAdded(event);
        } else {
            listener.memberRemoved(event);
        }
    }

    public Cluster getClusterProxy() {
        return new ClusterProxy(this);
    }

    public String membersString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        final Collection<MemberImpl> members = getMemberList();
        sb.append(members != null ? members.size() : 0);
        sb.append("] {");
        if (members != null) {
            for (Member member : members) {
                sb.append("\n\t").append(member);
            }
        }
        sb.append("\n}\n");
        return sb.toString();
    }



    public Map<Command, ClientCommandHandler> getCommandMap() {
        return commandHandlers;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ClusterService");
        sb.append("{address=").append(thisAddress);
        sb.append('}');
        return sb.toString();
    }
}
