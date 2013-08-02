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

import com.hazelcast.core.*;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;

public final class ClusterServiceImpl implements ClusterService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener> {

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

    private final AtomicBoolean preparingToMerge = new AtomicBoolean(false);

    private boolean joinInProgress = false;

    private long timeToStartJoin = 0;

    private long firstJoinRequest = 0;

    private final ConcurrentMap<MemberImpl, Long> masterConfirmationTimes = new ConcurrentHashMap<MemberImpl, Long>();

    private volatile long clusterTimeDiff = Long.MAX_VALUE;

    public ClusterServiceImpl(final Node node) {
        this.node = node;
        nodeEngine = node.nodeEngine;
        logger = node.getLogger(ClusterService.class.getName());
        thisAddress = node.getThisAddress();
        thisMember = node.getLocalMember();
        setMembers(thisMember);
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
        long mergeFirstRunDelay = node.getGroupProperties().MERGE_FIRST_RUN_DELAY_SECONDS.getLong() * 1000;
        mergeFirstRunDelay = mergeFirstRunDelay <= 0 ? 100 : mergeFirstRunDelay; // milliseconds

        long mergeNextRunDelay = node.getGroupProperties().MERGE_NEXT_RUN_DELAY_SECONDS.getLong() * 1000;
        mergeNextRunDelay = mergeNextRunDelay <= 0 ? 100 : mergeNextRunDelay; // milliseconds
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new SplitBrainHandler(node),
                mergeFirstRunDelay, mergeNextRunDelay, TimeUnit.MILLISECONDS);

        long heartbeatInterval = node.groupProperties.HEARTBEAT_INTERVAL_SECONDS.getInteger();
        heartbeatInterval = heartbeatInterval <= 0 ? 1 : heartbeatInterval;
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new Runnable() {
            public void run() {
                heartBeater();
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);

        long masterConfirmationInterval = node.groupProperties.MASTER_CONFIRMATION_INTERVAL_SECONDS.getInteger();
        masterConfirmationInterval = masterConfirmationInterval <= 0 ? 1 : masterConfirmationInterval;
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new Runnable() {
            public void run() {
                sendMasterConfirmation();
            }
        }, masterConfirmationInterval, masterConfirmationInterval, TimeUnit.SECONDS);

        long memberListPublishInterval = node.groupProperties.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getInteger();
        memberListPublishInterval = memberListPublishInterval <= 0 ? 1 : memberListPublishInterval;
        nodeEngine.getExecutionService().scheduleWithFixedDelay(new Runnable() {
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

    public JoinRequest checkJoinInfo(Address target) {
        Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME,
                new JoinCheckOperation(node.createJoinRequest()), target)
                .setTryCount(1).build();
        try {
            return (JoinRequest) nodeEngine.toObject(inv.invoke().get());
        } catch (Exception e) {
            logger.warning("Error during join check!", e);
        }
        return null;
    }

    public boolean validateJoinMessage(JoinMessage joinMessage) throws Exception {
        boolean valid = Packet.VERSION == joinMessage.getPacketVersion();
        if (valid) {
            try {
                valid = node.createConfigCheck().isCompatible(joinMessage.getConfigCheck());
            } catch (Exception e) {
                final String message = "Invalid join request from: " + joinMessage.getAddress() + ", reason:" + e.getMessage();
                logger.warning(message);
                node.getSystemLogService().logJoin(message);
                throw e;
            }
        }
        return valid;
    }

    private void logMissingConnection(Address address) {
        String msg = node.getLocalMember() + " has no connection to " + address;
        logger.warning(msg);
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
                                logger.warning("Added " + address + " to list of dead addresses because of timeout since last read");
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
                                logger.warning("Added " + address +
                                        " to list of dead addresses because it has not sent a master confirmation recently");
                                deadAddresses.add(address);
                            }
                        } else if (conn == null && (now - memberImpl.getLastRead()) > 5000) {
                            logMissingConnection(address);
                            memberImpl.didRead();
                        }
                    } catch (Exception e) {
                        logger.severe(e);
                    }
                }
            }
            if (deadAddresses != null) {
                for (Address address : deadAddresses) {
                    logger.finest( "No heartbeat should remove " + address);
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
                        logger.warning("Master node has timed out its heartbeat and will be removed");
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
                        logger.finest( "Could not connect to " + address + " to send heartbeat");
                    }
                }
            }
        }
    }

    private void ping(final MemberImpl memberImpl) {
        memberImpl.didPing();
        if (!icmpEnabled) return;
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
            logger.finest( "Error while sending heartbeat -> "
                    + e.getClass().getName() + "[" + e.getMessage() + "]");
        }
    }

    private void sendMasterConfirmation() {
        if (!node.joined() || !node.isActive() || isMaster()) {
            return;
        }
        final Address masterAddress = getMasterAddress();
        if (masterAddress == null) {
            logger.finest( "Could not send MasterConfirmation, master is null!");
            return;
        }
        final MemberImpl masterMember = getMember(masterAddress);
        if (masterMember == null) {
            logger.finest( "Could not send MasterConfirmation, master is null!");
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest( "Sending MasterConfirmation to " + masterMember);
        }
        nodeEngine.getOperationService().send(new MasterConfirmationOperation(), masterAddress);
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
        MemberInfoUpdateOperation op = new MemberInfoUpdateOperation(createMemberInfos(members), getClusterTime(), false);
        for (MemberImpl member : members) {
            if (member.equals(thisMember)) {
                continue;
            }
            nodeEngine.getOperationService().send(op, member.getAddress());
        }
    }

    public void removeAddress(Address deadAddress) {
        doRemoveAddress(deadAddress, true);
    }

    private void doRemoveAddress(Address deadAddress, boolean destroyConnection) {
        if (preparingToMerge.get()) {
            logger.warning("Cluster-merge process is ongoing, won't process member removal: " + deadAddress);
            return;
        }
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
                logger.info(membersString());
            }
        } finally {
            lock.unlock();
        }
    }

    private void assignNewMaster() {
        final Address oldMasterAddress = node.getMasterAddress();
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
                    logger.severe("Old master " + oldMasterAddress
                            + " is dead but the first of member list is a different member " +
                            member + "!");
                    newMaster = member;
                }
            } else {
                logger.warning("Old master is dead and this node is not master " +
                        "but member list contains only " + size + " members! -> " + members);
            }
            logger.info("Master " + oldMasterAddress + " left the cluster. Assigning new master " + newMaster);
            if (newMaster != null) {
                node.setMasterAddress(newMaster.getAddress());
            } else {
                node.setMasterAddress(null);
            }
        } else {
            node.setMasterAddress(null);
        }
        logger.finest( "Now Master " + node.getMasterAddress());
    }

    void handleJoinRequest(JoinRequestOperation joinRequest) {
        lock.lock();
        try {
            final JoinRequest joinMessage = joinRequest.getMessage();
            final long now = Clock.currentTimeMillis();
            String msg = "Handling join from " + joinMessage.getAddress() + ", inProgress: " + joinInProgress
                    + (timeToStartJoin > 0 ? ", timeToStart: " + (timeToStartJoin - now) : "");
            logger.finest( msg);
            boolean validJoinRequest;
            try {
                validJoinRequest = validateJoinMessage(joinMessage);
            } catch (Exception e) {
                validJoinRequest = false;
            }
            final Connection conn = joinRequest.getConnection();
            if (validJoinRequest) {
                final MemberImpl member = getMember(joinMessage.getAddress());
                if (member != null) {
                    if (joinMessage.getUuid().equals(member.getUuid())) {
                        String message = "Ignoring join request, member already exists.. => " + joinMessage;
                        logger.finest( message);
                        // send members update back to node trying to join again...
                        nodeEngine.getOperationService().send(new MemberInfoUpdateOperation(createMemberInfos(getMemberList()), getClusterTime(), false),
                                member.getAddress());
                        return;
                    }
                    // If this node is master then remove old member and process join request.
                    // If requesting address is equal to master node's address, that means master node
                    // somehow disconnected and wants to join back.
                    // So drop old member and process join request if this node becomes master.
                    if (node.isMaster() || member.getAddress().equals(node.getMasterAddress())) {
                        logger.warning("New join request has been received from an existing endpoint! => " + member
                                + " Removing old member and processing join request...");
                        // If existing connection of endpoint is different from current connection
                        // destroy it, otherwise keep it.
//                    final Connection existingConnection = node.connectionManager.getConnection(joinMessage.address);
//                    final boolean destroyExistingConnection = existingConnection != conn;
                        doRemoveAddress(member.getAddress(), false);
                    }
                }
                final boolean multicastEnabled = node.getConfig().getNetworkConfig().getJoin().getMulticastConfig().isEnabled();
                if (!multicastEnabled && node.isActive() && node.joined() && node.getMasterAddress() != null && !node.isMaster()) {
                    sendMasterAnswer(joinMessage);
                }
                if (node.isMaster() && node.joined() && node.isActive()) {
                    final MemberInfo newMemberInfo = new MemberInfo(joinMessage.getAddress(), joinMessage.getUuid());
                    if (node.securityContext != null && !setJoins.contains(newMemberInfo)) {
                        final Credentials cr = joinMessage.getCredentials();
                        ILogger securityLogger = node.loggingService.getLogger("com.hazelcast.security");
                        if (cr == null) {
                            securityLogger.severe("Expecting security credentials " +
                                    "but credentials could not be found in JoinRequest!");
                            nodeEngine.getOperationService().send(new AuthenticationFailureOperation(), joinMessage.getAddress());
                            return;
                        } else {
                            try {
                                LoginContext lc = node.securityContext.createMemberLoginContext(cr);
                                lc.login();
                            } catch (LoginException e) {
                                securityLogger.severe("Authentication has failed for " + cr.getPrincipal()
                                        + '@' + cr.getEndpoint() + " => (" + e.getMessage() +
                                        ")");
                                securityLogger.finest(e);
                                nodeEngine.getOperationService().send(new AuthenticationFailureOperation(), joinMessage.getAddress());
                                return;
                            }
                        }
                    }
                    if (!joinInProgress) {
                        if (firstJoinRequest != 0 && now - firstJoinRequest >= maxWaitSecondsBeforeJoin * 1000) {
                            startJoin();
                        } else {
                            if (setJoins.add(newMemberInfo)) {
                                sendMasterAnswer(joinMessage);
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
        nodeEngine.getOperationService().send(new SetMasterOperation(node.getMasterAddress()), joinRequest.getAddress());
    }

    void handleMaster(Address masterAddress) {
        lock.lock();
        try {
            if (!node.joined() && !node.getThisAddress().equals(masterAddress)) {
                logger.finest( "Handling master response: " + this);
                final Address currentMaster = node.getMasterAddress();
                if (currentMaster != null && !currentMaster.equals(masterAddress)) {
                    final Connection conn = node.connectionManager.getConnection(currentMaster);
                    if (conn != null && conn.live()) {
                        logger.warning("Ignoring master response from " + masterAddress +
                                ", since this node has an active master: " + currentMaster);
                        return;
                    }
                }
                node.setMasterAddress(masterAddress);
                node.connectionManager.getOrConnect(masterAddress);
                if (!sendJoinRequest(masterAddress, true)) {
                    logger.warning("Could not create connection to possible master " + masterAddress);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    void acceptMasterConfirmation(MemberImpl member) {
        if (member != null) {
            if (logger.isFinestEnabled()) {
                logger.finest( "MasterConfirmation has been received from " + member);
            }
            masterConfirmationTimes.put(member, Clock.currentTimeMillis());
        }
    }

    void prepareToMerge(final Address newTargetAddress) {
        preparingToMerge.set(true);
        node.getJoiner().setTargetAddress(newTargetAddress);
        nodeEngine.getExecutionService().schedule(new Runnable() {
            public void run() {
                merge(newTargetAddress);
            }
        }, 10, TimeUnit.SECONDS);
    }

    void merge(Address newTargetAddress) {
        if (preparingToMerge.compareAndSet(true, false)) {
            node.getJoiner().setTargetAddress(newTargetAddress);
            final LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
            lifecycleService.runUnderLifecycleLock(new Runnable() {
                public void run() {
                    lifecycleService.fireLifecycleEvent(MERGING);
                    final NodeEngineImpl nodeEngine = node.nodeEngine;
                    final Collection<SplitBrainHandlerService> services = nodeEngine.getServices(SplitBrainHandlerService.class);
                    final Collection<Runnable> tasks = new LinkedList<Runnable>();
                    for (SplitBrainHandlerService service : services) {
                        final Runnable runnable = service.prepareMergeRunnable();
                        if (runnable != null) {
                            tasks.add(runnable);
                        }
                    }
                    final Collection<ManagedService> managedServices = nodeEngine.getServices(ManagedService.class);
                    for (ManagedService service : managedServices) {
                        service.reset();
                    }
                    node.onRestart();
                    node.connectionManager.restart();
                    node.rejoin();
                    final Collection<Future> futures = new LinkedList<Future>();
                    for (Runnable task : tasks) {
                        Future f = nodeEngine.getExecutionService().submit("hz:system", task);
                        futures.add(f);
                    }
                    for (Future f : futures) {
                        try {
                            f.get();
                        } catch (Exception e) {
                            logger.severe("While merging...", e);
                        }
                    }
                    lifecycleService.fireLifecycleEvent(MERGED);
                }
            });
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

    public void reset() {
        lock.lock();
        try {
            joinInProgress = false;
            setJoins.clear();
            timeToStartJoin = 0;
            membersRef.set(null);
            masterConfirmationTimes.clear();
        } finally {
            lock.unlock();
        }
    }

    void startJoin() {
        logger.finest( "Starting Join.");
        lock.lock();
        try {
            joinInProgress = true;
            final Collection<MemberImpl> members = getMemberList();
            final Collection<MemberInfo> memberInfos = createMemberInfos(members);
            for (MemberInfo memberJoining : setJoins) {
                memberInfos.add(memberJoining);
            }
            final long time = getClusterTime();
            // Post join operations must be lock free; means no locks at all;
            // no partition locks, no key-based locks, no service level locks!
            final Operation[] postJoinOps = nodeEngine.getPostJoinOperations();
            final PostJoinOperation postJoinOp = postJoinOps != null && postJoinOps.length > 0
                    ? new PostJoinOperation(postJoinOps) : null;
            final List<Future> calls = new ArrayList<Future>(members.size());
            final int count = members.size() - 1 + setJoins.size();
            for (MemberInfo member : setJoins) {
                calls.add(invokeClusterOperation(new FinalizeJoinOperation(memberInfos, postJoinOp, time), member.getAddress()));
            }
            for (MemberImpl member : members) {
                if (!member.getAddress().equals(thisAddress)) {
                    calls.add(invokeClusterOperation(new MemberInfoUpdateOperation(memberInfos, time, true), member.getAddress()));
                }
            }
            updateMembers(memberInfos);
            for (Future future : calls) {
                try {
                    future.get(10, TimeUnit.SECONDS);
                } catch (TimeoutException ignored) {
                    logger.finest( "Finalize join call timed-out: " + future);
                } catch (Exception e) {
                    logger.warning("While waiting finalize join calls...", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private static Collection<MemberInfo> createMemberInfos(Collection<MemberImpl> members) {
        final Collection<MemberInfo> memberInfos = new LinkedList<MemberInfo>();
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getUuid()));
        }
        return memberInfos;
    }

    void updateMembers(Collection<MemberInfo> members) {
        lock.lock();
        try {
            Map<Address, MemberImpl> oldMemberMap = membersRef.get();
            if (oldMemberMap == null) {
                oldMemberMap = Collections.emptyMap();
            }
            if (oldMemberMap.size() == members.size()) {
                boolean same = true;
                for (MemberInfo memberInfo : members) {
                    MemberImpl member = oldMemberMap.get(memberInfo.getAddress());
                    if (member == null || !member.getUuid().equals(memberInfo.uuid)) {
                        same = false;
                        break;
                    }
                }
                if (same) {
                    logger.finest( "No need to process member update...");
                    return;
                }
            }
            MemberImpl[] newMembers = new MemberImpl[members.size()];
            int k = 0;
            for (MemberInfo memberInfo : members) {
                MemberImpl member = oldMemberMap.get(memberInfo.address);
                if (member == null) {
                    member = createMember(memberInfo.address, memberInfo.uuid, thisAddress.getScopeId());
                }
                newMembers[k++] = member;
                member.didRead();
            }
            setMembers(newMembers);
            if (!getMemberList().contains(thisMember)) {
                throw new HazelcastException("Member list doesn't contain local member!");
            }
            joinReset();
            heartBeater();
            node.setJoined();
            logger.info(membersString());
        } finally {
            lock.unlock();
        }
    }

    public boolean sendJoinRequest(Address toAddress, boolean withCredentials) {
        if (toAddress == null) {
            toAddress = node.getMasterAddress();
        }
        JoinRequestOperation joinRequest = new JoinRequestOperation(node.createJoinRequest(withCredentials));
        nodeEngine.getOperationService().send(joinRequest, toAddress);
        return true;
    }

    public void connectionAdded(final Connection connection) {
        MemberImpl member = getMember(connection.getEndPoint());
        if (member != null) {
            member.didRead();
        }
    }

    public void connectionRemoved(Connection connection) {
        logger.finest( "Connection is removed " + connection.getEndPoint());
        if (!node.joined()) {
            final Address masterAddress = node.getMasterAddress();
            if (masterAddress != null && masterAddress.equals(connection.getEndPoint())) {
                node.setMasterAddress(null);
            }
        }
    }

    private Future invokeClusterOperation(Operation op, Address target) {
       return nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(50).build().invoke();
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    private void setMembers(MemberImpl... members) {
        if (members == null || members.length == 0) return;
        logger.finest( "Updating members -> " + Arrays.toString(members));
        lock.lock();
        try {
            Map<Address, MemberImpl> oldMemberMap = membersRef.get();
            if (oldMemberMap == null) {
                oldMemberMap = Collections.emptyMap();
            }
            final Map<Address, MemberImpl> memberMap = new LinkedHashMap<Address, MemberImpl>();  // ! ORDERED !
            final Collection<MemberImpl> newMembers = new LinkedList<MemberImpl>();
            for (MemberImpl member : members) {
                MemberImpl currentMember = oldMemberMap.get(member.getAddress());
                if (currentMember == null) {
                    newMembers.add(member);
                    masterConfirmationTimes.put(member, Clock.currentTimeMillis());
                }
                memberMap.put(member.getAddress(), member);
            }
            setMembersRef(memberMap);

            if (!newMembers.isEmpty()) {
                Set<Member> eventMembers = new LinkedHashSet<Member>(oldMemberMap.values());
                if (newMembers.size() == 1) {
                    MemberImpl newMember = newMembers.iterator().next();
                    node.getPartitionService().memberAdded(newMember); // sync call
                    eventMembers.add(newMember);
                    sendMembershipEventNotifications(newMember, Collections.unmodifiableSet(eventMembers), true); // async events
                } else {
                    for (MemberImpl newMember : newMembers) {
                        node.getPartitionService().memberAdded(newMember); // sync call
                        eventMembers.add(newMember);
                        sendMembershipEventNotifications(newMember, Collections.unmodifiableSet(new LinkedHashSet<Member>(eventMembers)), true); // async events
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeMember(MemberImpl deadMember) {
        logger.info("Removing " + deadMember);
        lock.lock();
        try {
            final Map<Address, MemberImpl> members = membersRef.get();
            if (members != null && members.containsKey(deadMember.getAddress())) {
                Map<Address, MemberImpl> newMembers = new LinkedHashMap<Address, MemberImpl>(members);  // ! ORDERED !
                newMembers.remove(deadMember.getAddress());
                masterConfirmationTimes.remove(deadMember);
                setMembersRef(newMembers);
                node.getPartitionService().memberRemoved(deadMember); // sync call
                nodeEngine.onMemberLeft(deadMember);                  // sync call
                sendMembershipEventNotifications(deadMember, Collections.unmodifiableSet(new LinkedHashSet<Member>(newMembers.values())), false); // async events
                if (node.isMaster()) {
                    logger.finest( deadMember + " is dead. Sending remove to all other members.");
                    invokeMemberRemoveOperation(deadMember.getAddress());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void invokeMemberRemoveOperation(final Address deadAddress) {
        for (MemberImpl member : getMemberList()) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadAddress)) {
                nodeEngine.getOperationService().send(new MemberRemoveOperation(deadAddress), address);
            }
        }
    }

    public void sendShutdownMessage() {
        invokeMemberRemoveOperation(thisAddress);
    }

    private void sendMembershipEventNotifications(final MemberImpl member, Set<Member> members, final boolean added) {
        final int eventType = added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED;
        final MembershipEvent membershipEvent = new MembershipEvent(getClusterProxy(), member, eventType, members);
        final Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            final MembershipServiceEvent event = new MembershipServiceEvent(membershipEvent);
            for (final MembershipAwareService service : membershipAwareServices) {
                // service events should not block each other
                nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                    public void run() {
                        if (added) {
                            service.memberAdded(event);
                        } else {
                            service.memberRemoved(event);
                        }
                    }
                });
            }
        }
        final EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        for (EventRegistration reg : registrations) {
            eventService.publishEvent(SERVICE_NAME, reg, membershipEvent, reg.getId().hashCode());
        }
    }

    protected MemberImpl createMember(Address address, String nodeUuid, String ipV6ScopeId) {
        address.setScopeId(ipV6ScopeId);
        return new MemberImpl(address, thisAddress.equals(address), nodeUuid);
    }

    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        final Map<Address, MemberImpl> memberMap = membersRef.get();
        return memberMap != null ? memberMap.get(address) : null;
    }

    public MemberImpl getMember(String uuid) {
        if (uuid == null) {
            return null;
        }
        final Map<Address, MemberImpl> memberMap = membersRef.get();
        if (memberMap == null) {
            return null;
        }
        for (MemberImpl member : memberMap.values()) {
            if (uuid.equals(member.getUuid())) {
                return member;
            }
        }
        return null;
    }

    private void setMembersRef(final Map<Address, MemberImpl> memberMap) {
        final Map<Address, MemberImpl> members = Collections.unmodifiableMap(memberMap);
        // make values(), keySet() and entrySet() to be cached
        members.values();
        members.keySet();
        members.entrySet();
        membersRef.set(members);
    }

    public Collection<MemberImpl> getMemberList() {
        final Map<Address, MemberImpl> map = membersRef.get();
        return map != null ? Collections.unmodifiableCollection(map.values()) : Collections.<MemberImpl>emptySet();
    }

    public void shutdown() {
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

    public String addMembershipListener(MembershipListener listener) {
        if (listener instanceof InitialMembershipListener) {
            lock.lock();
            try {
                ((InitialMembershipListener) listener).init(new InitialMembershipEvent(getClusterProxy(), getMembers()));
                final EventRegistration registration = nodeEngine.getEventService().registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
                return registration.getId();
            } finally {
                lock.unlock();
            }
        }  else {
            final EventRegistration registration = nodeEngine.getEventService().registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
            return registration.getId();
        }
    }

    public boolean removeMembershipListener(final String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ClusterService");
        sb.append("{address=").append(thisAddress);
        sb.append('}');
        return sb.toString();
    }
}
