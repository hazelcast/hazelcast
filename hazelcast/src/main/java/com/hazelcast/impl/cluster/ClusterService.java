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

package com.hazelcast.impl.cluster;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.NodeType;
import com.hazelcast.impl.spi.*;
import com.hazelcast.impl.spi.annotation.ExecutedBy;
import com.hazelcast.impl.spi.annotation.ThreadType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.security.Credentials;
import com.hazelcast.util.Clock;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public final class ClusterService implements ConnectionListener, MembershipAwareService, CoreService {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    private final Node node;

    private final NodeServiceImpl nodeService;

    private final ILogger logger;

    protected final Address thisAddress;

    protected final MemberImpl thisMember;

    private final long waitMillisBeforeJoin;

    private final long maxWaitSecondsBeforeJoin;

    private final long maxNoHeartbeatMillis;

    private final boolean icmpEnabled;

    private final int icmpTtl;

    private final int icmpTimeout;

    private final Lock lock = new ReentrantLock();

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);

    private final AtomicReference<Map<Address, MemberImpl>> membersRef = new AtomicReference<Map<Address, MemberImpl>>();

    private final AtomicInteger dataMemberCount = new AtomicInteger(); // excluding lite members

    private final ILogger securityLogger;

    private final Data heartbeatOperationData;

    private final CopyOnWriteArraySet<MembershipListener> listeners = new CopyOnWriteArraySet<MembershipListener>();

    private boolean joinInProgress = false;

    private long timeToStartJoin = 0;

    private long firstJoinRequest = 0;

    private volatile long clusterTimeDiff = Long.MAX_VALUE;

    public ClusterService(final Node node) {
        this.node = node;
        nodeService = node.nodeService;
        logger = node.getLogger(getClass().getName());
        thisAddress = node.getThisAddress();
        thisMember = node.getLocalMember();
        securityLogger = node.loggingService.getLogger("com.hazelcast.security");
        waitMillisBeforeJoin = node.groupProperties.WAIT_SECONDS_BEFORE_JOIN.getInteger() * 1000L;
        maxWaitSecondsBeforeJoin = node.groupProperties.MAX_WAIT_SECONDS_BEFORE_JOIN.getInteger();
        maxNoHeartbeatMillis = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger() * 1000L;
        icmpEnabled = node.groupProperties.ICMP_ENABLED.getBoolean();
        icmpTtl = node.groupProperties.ICMP_TTL.getInteger();
        icmpTimeout = node.groupProperties.ICMP_TIMEOUT.getInteger();
        heartbeatOperationData = toData(new HeartbeatOperation());
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

        node.connectionManager.addConnectionListener(this);

        nodeService.registerService(SERVICE_NAME, this);
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
        Invocation inv = nodeService.createSingleInvocation(SERVICE_NAME,
                new JoinCheckOperation(node.createJoinInfo()), -1)
                .setTarget(target).setTryCount(1).build();
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
        Address master = node.getMasterAddress();
        if (!node.isMaster() && master != null) {
            Connection connMaster = node.connectionManager.getOrConnect(node.getMasterAddress());
            if (connMaster != null) {
                Packet packet = new Packet();
                packet.set(level.toString(), null, toData(msg), ClusterOperation.LOG);
                packet.timeout = 0;
                send(packet, connMaster);
            }
        } else {
            logger.log(level, msg);
        }
    }

    public final void heartBeater() {
        if (!node.joined() || !node.isActive()) return;
        long now = Clock.currentTimeMillis();
        final Collection<MemberImpl> members = getMemberList();
        if (node.isMaster()) {
            List<Address> lsDeadAddresses = null;
            for (MemberImpl memberImpl : members) {
                final Address address = memberImpl.getAddress();
                if (!thisAddress.equals(address)) {
                    try {
                        Connection conn = node.connectionManager.getOrConnect(address);
                        if (conn != null && conn.live()) {
                            if ((now - memberImpl.getLastRead()) >= (maxNoHeartbeatMillis)) {
                                if (lsDeadAddresses == null) {
                                    lsDeadAddresses = new ArrayList<Address>();
                                }
                                logger.log(Level.WARNING, "Added " + address + " to list of dead addresses because of timeout since last read");
                                lsDeadAddresses.add(address);
                            } else if ((now - memberImpl.getLastRead()) >= 5000 && (now - memberImpl.getLastPing()) >= 5000) {
                                ping(memberImpl);
                            }
                            if ((now - memberImpl.getLastWrite()) > 500) {
                                sendHeartbeat(address);
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
                        logger.log(Level.FINEST, "could not connect to " + address + " to send heartbeat");
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

    void sendHeartbeat(Address target) {
        if (target == null) return;
        send(new SimpleSocketWritable(heartbeatOperationData, -1, -1, 0, null), target);
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
            }
            final Connection conn = node.connectionManager.getConnection(deadAddress);
            if (destroyConnection && conn != null) {
                node.connectionManager.destroyConnection(conn);
            }
            MemberImpl deadMember = getMember(deadAddress);
            if (deadMember != null) {
                removeMember(deadMember);
                disconnectExistingCalls(deadAddress);
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
                    logger.log(Level.SEVERE, "Old master is dead but the first of member list is a different member!");
                    newMaster = member;
                }
            } else {
                logger.log(Level.WARNING, "Old master is dead, this node is not master " +
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

    public void disconnectExistingCalls(Address deadAddress) {
        lock.lock();
        try {
            nodeService.disconnectExistingCalls(deadAddress);
        } finally {
            lock.unlock();
        }
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
                        if (cr == null) {
                            securityLogger.log(Level.SEVERE, "Expecting security credentials " +
                                    "but credentials could not be found in JoinRequest!");
                            sendAuthFail(joinRequest.address);
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
                                sendAuthFail(joinRequest.address);
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
        node.nodeService.createSingleInvocation(SERVICE_NAME,
                new SetMasterOperation(node.getMasterAddress()), -1).setTarget(joinRequest.address)
                .setTryCount(1).build().invoke();
    }

    private void sendAuthFail(Address target) {
        node.nodeService.createSingleInvocation(SERVICE_NAME, new AuthenticationFailureOperation(),
                NodeService.EXECUTOR_THREAD_ID).setTarget(target).setTryCount(1).build().invoke();
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
    }

    void startJoin() {
        logger.log(Level.FINEST, "Starting Join.");
        lock.lock();
        try {
            joinInProgress = true;
            final FinalizeJoinOperation finalizeJoinOp = new FinalizeJoinOperation(getMemberList(), getClusterTime());
            if (setJoins != null && setJoins.size() > 0) {
                for (MemberInfo memberJoined : setJoins) {
                    finalizeJoinOp.addMemberInfo(memberJoined);
                }
            }
            Collection<MemberInfo> members = finalizeJoinOp.getMemberInfos();
            List<Future> calls = new ArrayList<Future>(members.size());
            for (final MemberInfo member : members) {
                if (!member.getAddress().equals(thisAddress)) {
                    Invocation inv = nodeService.createSingleInvocation(SERVICE_NAME, finalizeJoinOp, -1)
                            .setTarget(member.getAddress()).setTryCount(1).build();
                    calls.add(inv.invoke());
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
        logger.log(Level.FINEST, "Updating Members");
        lock.lock();
        try {
            Map<Address, MemberImpl> mapOldMembers = new HashMap<Address, MemberImpl>();
            for (MemberImpl member : getMemberList()) {
                mapOldMembers.put(member.getAddress(), member);
            }
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
        Invocation inv = node.nodeService.createSingleInvocation(SERVICE_NAME, joinRequest, NodeService.EXECUTOR_THREAD_ID)
                .setTarget(toAddress).setTryCount(1).build();
        inv.invoke();
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
                } else if (!oldMember.isLiteMember()) {
                    dataMemberCount.decrementAndGet();
                }
                memberMap.put(member.getAddress(), member);
                if (!member.isLiteMember()) {
                    dataMemberCount.incrementAndGet();
                }
            }
            setMembers(memberMap);

            final Collection<MembershipAwareService> services = getMembershipAwareServices();
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
                if (!deadMember.isLiteMember()) {
                    dataMemberCount.decrementAndGet();
                }
                setMembers(newMembers);

                Collection<MembershipAwareService> services = getMembershipAwareServices();
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

    private Collection<MembershipAwareService> getMembershipAwareServices() {
        final Collection allServices = nodeService.getServices();
        final LinkedList<MembershipAwareService> services = new LinkedList<MembershipAwareService>();
        for (Object service : allServices) {
            if (service instanceof MembershipAwareService) {
                if (service instanceof CoreService) {
                    services.addFirst((MembershipAwareService) service);
                } else {
                    services.addLast((MembershipAwareService) service);
                }
            }
        }
        return services;
    }

    private void invokeMemberRemoveOperation(final MemberImpl deadMember) {
        final Address deadAddress = deadMember.getAddress();
        final Collection<Future> responses = new LinkedList<Future>();
        for (MemberImpl member : getMemberList()) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadAddress)) {
                Future f = node.nodeService.createSingleInvocation(SERVICE_NAME, new MemberRemoveOperation(deadAddress), -1)
                        .setTarget(address).setTryCount(10).setTryPauseMillis(100).build().invoke();
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
            nodeService.getEventService().execute(new Runnable() {
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
        stop();
    }

    public void stop() {
        lock.lock();
        try {
            if (setJoins != null) {
                setJoins.clear();
            }
            timeToStartJoin = 0;
            membersRef.set(null);
            dataMemberCount.set(0);
        } finally {
            lock.unlock();
        }
    }

    public Address getMasterAddress() {
        return node.getMasterAddress();
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
