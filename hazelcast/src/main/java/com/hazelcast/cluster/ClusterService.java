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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.ExecutedBy;
import com.hazelcast.spi.annotation.ThreadType;
import com.hazelcast.spi.impl.NodeServiceImpl;
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

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public final class ClusterService implements ConnectionListener, MembershipAwareService, CoreService, ManagedService {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    private final Node node;

    private final NodeServiceImpl nodeService;

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

    private final Data heartbeatOperationData;

    private final List<MembershipListener> listeners = new CopyOnWriteArrayList<MembershipListener>();

    private boolean joinInProgress = false;

    private long timeToStartJoin = 0;

    private long firstJoinRequest = 0;

    private final ConcurrentMap<MemberImpl, Long> masterConfirmationTimes = new ConcurrentHashMap<MemberImpl, Long>();

    private volatile long clusterTimeDiff = Long.MAX_VALUE;

    public ClusterService(final Node node) {
        this.node = node;
        nodeService = node.nodeService;
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
        heartbeatOperationData = toData(new HeartbeatOperation());
        node.connectionManager.addConnectionListener(this);
    }

    public void init(final NodeService nodeService, Properties properties) {
        final long mergeFirstRunDelay = node.getGroupProperties().MERGE_FIRST_RUN_DELAY_SECONDS.getLong();
        final long mergeNextRunDelay = node.getGroupProperties().MERGE_NEXT_RUN_DELAY_SECONDS.getLong();
        nodeService.scheduleWithFixedDelay(new SplitBrainHandler(node),
                mergeFirstRunDelay, mergeNextRunDelay, TimeUnit.SECONDS);

        final long heartbeatInterval = node.groupProperties.HEARTBEAT_INTERVAL_SECONDS.getInteger();
        nodeService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                heartBeater();
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);

        final long masterConfirmationInterval = node.groupProperties.MASTER_CONFIRMATION_INTERVAL_SECONDS.getInteger();
        nodeService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                sendMasterConfirmation();
            }
        }, masterConfirmationInterval, masterConfirmationInterval, TimeUnit.SECONDS);

        final long memberListPublishInterval = node.groupProperties.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getInteger();
        nodeService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                sendMemberListToOthers();
            }
        }, memberListPublishInterval, memberListPublishInterval, TimeUnit.SECONDS);
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
        Invocation inv = nodeService.createInvocationBuilder(SERVICE_NAME,
                new JoinCheckOperation(node.createJoinInfo()), target)
                .setTryCount(1).build();
        try {
            return (JoinInfo) toObject(inv.invoke().get());
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
        node.nodeService.execute(new Runnable() {
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
        send(new Packet(heartbeatOperationData), target);
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
        final Collection<MemberImpl> memberList = getMemberList();
        MemberInfoUpdateOperation op = new MemberInfoUpdateOperation(memberList, getClusterTime());
        for (MemberImpl member : memberList) {
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
                nodeService.onMemberLeft(deadMember);
                logger.log(Level.INFO, this.toString());
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
                        invokeClusterOperation(new FinalizeJoinOperation(getMemberList(), getClusterTime()),
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
                if (!node.getConfig().getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
                    if (node.isActive() && node.joined() && node.getMasterAddress() != null && !node.isMaster()) {
                        sendMasterAnswer(joinRequest);
                    }
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
            final FinalizeJoinOperation finalizeJoinOp = new FinalizeJoinOperation(getMemberList(), getClusterTime());
            if (setJoins.size() > 0) {
                for (MemberInfo memberJoined : setJoins) {
                    finalizeJoinOp.addMemberInfo(memberJoined);
                }
            }
            Collection<MemberInfo> members = finalizeJoinOp.getMemberInfos();
            List<Future> calls = new ArrayList<Future>(members.size());
            for (final MemberInfo member : members) {
                if (!member.getAddress().equals(thisAddress)) {
                    calls.add(invokeClusterOperation(finalizeJoinOp, member.getAddress()));
                }
            }
            updateMembers(members);
            for (Future future : calls) {
                try {
                    future.get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
            }
        } finally {
            lock.unlock();
        }
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
            logger.log(Level.INFO, toString());
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
        return nodeService.createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(5).build().invoke();
    }

    public NodeServiceImpl getNodeService() {
        return nodeService;
    }

    public void memberAdded(final MemberImpl member) {

    }

    public void memberRemoved(final MemberImpl member) {

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

            Collection<MembershipAwareService> services = nodeService.getServices(MembershipAwareService.class);
            for (MemberImpl member : newMembers) {
                for (MembershipAwareService service : services) {
                    service.memberAdded(member);
                }
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

                Collection<MembershipAwareService> services = nodeService.getServices(MembershipAwareService.class);
                for (MembershipAwareService service : services) {
                    service.memberRemoved(deadMember);
                }
                if (node.isMaster()) {
                    logger.log(Level.FINEST, deadMember + " is dead. Sending remove to all other members.");
                    invokeMemberRemoveOperation(deadMember);
                }
                sendMembershipEventNotifications(deadMember, false);
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
                Future f = nodeService.createInvocationBuilder(SERVICE_NAME, new MemberRemoveOperation(deadAddress), address)
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
        if (listeners.size() > 0) {
            nodeService.getEventExecutor().execute(new Runnable() {
                public void run() {
                    MembershipEvent membershipEvent = new MembershipEvent(getClusterProxy(), member,
                            (added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED));
                    for (MembershipListener listener : listeners) {
                        if (added) {
                            listener.memberAdded(membershipEvent);
                        } else {
                            listener.memberRemoved(membershipEvent);
                        }
                    }
                }
            });
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

    public int getDataMemberCount() {
        return dataMemberCount.get();
    }

    public final boolean send(SocketWritable packet, Address address) {
        if (address == null) return false;
        final Connection conn = node.connectionManager.getOrConnect(address);
        return send(packet, conn);
    }

    public final boolean send(SocketWritable packet, Connection conn) {
        return conn != null && conn.live() && writePacket(conn, packet);
    }

    private boolean writePacket(Connection conn, SocketWritable packet) {
        final MemberImpl memberImpl = getMember(conn.getEndPoint());
        if (memberImpl != null) {
            memberImpl.didWrite();
        }
        // TODO
//        if (packet.lockAddress != null) {
//            if (thisAddress.equals(packet.lockAddress)) {
//                packet.lockAddress = null;
//            }
//        }
        conn.getWriteHandler().enqueueSocketWritable(packet);
        return true;
    }

    public void reset() {
        destroy();
    }

    public void destroy() {
        lock.lock();
        try {
            if (setJoins != null) {
                setJoins.clear();
            }
            timeToStartJoin = 0;
            membersRef.set(null);
            dataMemberCount.set(0);
            masterConfirmationTimes.clear();
        } finally {
            lock.unlock();
        }
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
        listeners.add(listener);
    }

    public void removeMembershipListener(MembershipListener listener) {
        listeners.remove(listener);
    }

    public Cluster getClusterProxy() {
        return new ClusterProxy(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        final Collection<MemberImpl> members = getMemberList();
        sb.append(members.size());
        sb.append("] {");
        for (Member member : members) {
            sb.append("\n\t").append(member);
        }
        sb.append("\n}\n");
        return sb.toString();
    }
}
