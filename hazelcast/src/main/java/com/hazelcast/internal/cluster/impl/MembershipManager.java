/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.FetchMembersViewOp;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.executor.ExecutorType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.cluster.impl.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.CLUSTER_EXECUTOR_NAME;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.MEMBERSHIP_EVENT_EXECUTOR_NAME;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.SERVICE_NAME;
import static com.hazelcast.spi.properties.GroupProperty.MASTERSHIP_CLAIM_TIMEOUT_SECONDS;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

/**
 * MembershipManager maintains member list and version, manages member update, suspicion and removal mechanisms.
 * Also initiates and manages mastership claim process.
 *
 * @since 3.9
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class MembershipManager {

    private static final long FETCH_MEMBER_LIST_MILLIS = 5000;
    private static final String MASTERSHIP_CLAIM_EXECUTOR_NAME = "hz:cluster:mastership";

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final Lock clusterServiceLock;
    private final ILogger logger;

    private final AtomicReference<MemberMap> memberMapRef = new AtomicReference<>(MemberMap.empty());

    /**
     * Members removed from active cluster members list while cluster state
     * doesn't allow new members to join, such as FROZEN or PASSIVE.
     * <p>
     * Missing members are associated with either their {@code UUID} or their {@code Address}
     * depending on Hot Restart is enabled or not.
     */
    private final AtomicReference<Map<Object, MemberImpl>> missingMembersRef = new AtomicReference<>(Collections.emptyMap());

    private final Set<Address> suspectedMembers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final int mastershipClaimTimeoutSeconds;

    MembershipManager(Node node, ClusterServiceImpl clusterService, Lock clusterServiceLock) {
        this.node = node;
        this.clusterService = clusterService;
        this.clusterServiceLock = clusterServiceLock;
        this.nodeEngine = node.getNodeEngine();
        this.logger = node.getLogger(getClass());

        mastershipClaimTimeoutSeconds = node.getProperties().getInteger(MASTERSHIP_CLAIM_TIMEOUT_SECONDS);
        registerThisMember();
    }

    /**
     * Initializes the {@link MembershipManager}.
     * It will schedule the member list publication to the {@link GroupProperty#MEMBER_LIST_PUBLISH_INTERVAL_SECONDS} interval.
     */
    void init() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        HazelcastProperties hazelcastProperties = node.getProperties();

        executionService.register(MASTERSHIP_CLAIM_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);

        long memberListPublishInterval = hazelcastProperties.getSeconds(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS);
        memberListPublishInterval = (memberListPublishInterval > 0 ? memberListPublishInterval : 1);
        executionService.scheduleWithRepetition(CLUSTER_EXECUTOR_NAME, this::publishMemberList,
                memberListPublishInterval, memberListPublishInterval, TimeUnit.SECONDS);
    }

    private void registerThisMember() {
        MemberImpl thisMember = clusterService.getLocalMember();
        memberMapRef.set(MemberMap.singleton(thisMember));
    }

    public MemberImpl getMember(Address address) {
        assert address != null : "Address required!";
        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(address);
    }

    public MemberImpl getMember(UUID uuid) {
        assert uuid != null : "UUID required!";

        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(uuid);
    }

    public MemberImpl getMember(Address address, UUID uuid) {
        assert address != null : "Address required!";
        assert uuid != null : "UUID required!";

        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(address, uuid);
    }

    public Collection<MemberImpl> getMembers() {
        return memberMapRef.get().getMembers();
    }

    @SuppressWarnings("unchecked")
    public Set<Member> getMemberSet() {
        return (Set) memberMapRef.get().getMembers();
    }

    MemberMap getMemberMap() {
        return memberMapRef.get();
    }

    // used in Jet, must be public
    public MembersView getMembersView() {
        return memberMapRef.get().toMembersView();
    }

    public int getMemberListVersion() {
        return memberMapRef.get().getVersion();
    }

    /**
     * Sends the current member list to the {@code target}. Called on the master node.
     *
     * @param target the destination for the member update operation
     */
    public void sendMemberListToMember(Address target) {
        clusterServiceLock.lock();
        try {
            if (!clusterService.isMaster() || !clusterService.isJoined()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot publish member list to " + target + ". Is-master: "
                            + clusterService.isMaster() + ", joined: " + clusterService.isJoined());
                }

                return;
            }
            if (clusterService.getThisAddress().equals(target)) {
                return;
            }

            MemberMap memberMap = memberMapRef.get();
            MemberImpl member = memberMap.getMember(target);
            if (member == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Not member: " + target + ", cannot send member list.");
                }

                return;
            }

            if (logger.isFineEnabled()) {
                logger.fine("Sending member list to member: " + target + " " + memberListString());
            }

            MembersUpdateOp op = new MembersUpdateOp(member.getUuid(), memberMap.toMembersView(),
                    clusterService.getClusterTime(), null, false);
            op.setCallerUuid(clusterService.getThisUuid());
            nodeEngine.getOperationService().send(op, target);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void publishMemberList() {
        clusterServiceLock.lock();
        try {
            sendMemberListToOthers();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    /** Invoked on the master to send the member list (see {@link MembersUpdateOp}) to non-master nodes. */
    private void sendMemberListToOthers() {
        if (!clusterService.isMaster() || !clusterService.isJoined()
                || clusterService.getClusterJoinManager().isMastershipClaimInProgress()) {
            if (logger.isFineEnabled()) {
                logger.fine("Cannot publish member list to cluster. Is-master: "
                        + clusterService.isMaster() + ", joined: " + clusterService.isJoined()
                        + " , mastership claim in progress: " + clusterService.getClusterJoinManager()
                                                                              .isMastershipClaimInProgress());
            }

            return;
        }

        MemberMap memberMap = getMemberMap();
        MembersView membersView = memberMap.toMembersView();

        if (logger.isFineEnabled()) {
            logger.fine("Sending member list to the non-master nodes: " + memberListString());
        }

        for (MemberImpl member : memberMap.getMembers()) {
            if (member.localMember()) {
                continue;
            }

            MembersUpdateOp op = new MembersUpdateOp(member.getUuid(), membersView,
                    clusterService.getClusterTime(), null, false);
            op.setCallerUuid(clusterService.getThisUuid());
            nodeEngine.getOperationService().send(op, member.getAddress());
        }
    }

    String memberListString() {
        MemberMap memberMap = getMemberMap();
        Collection<MemberImpl> members = memberMap.getMembers();
        StringBuilder sb = new StringBuilder("\n\nMembers {")
                .append("size:").append(members.size()).append(", ")
                .append("ver:").append(memberMap.getVersion())
                .append("} [");

        for (Member member : members) {
            sb.append("\n\t").append(member);
        }
        sb.append("\n]\n");
        return sb.toString();
    }

    // handles both new and left members
    void updateMembers(MembersView membersView) {
        MemberMap currentMemberMap = memberMapRef.get();

        Collection<MemberImpl> addedMembers = new LinkedList<>();
        Collection<MemberImpl> removedMembers = new LinkedList<>();
        ClusterHeartbeatManager clusterHeartbeatManager = clusterService.getClusterHeartbeatManager();

        MemberImpl[] members = new MemberImpl[membersView.size()];
        int memberIndex = 0;
        for (MemberInfo memberInfo : membersView.getMembers()) {
            Address address = memberInfo.getAddress();
            MemberImpl member = currentMemberMap.getMember(address);

            if (member != null && member.getUuid().equals(memberInfo.getUuid())) {
                member = createNewMemberImplIfChanged(memberInfo, member);
                members[memberIndex++] = member;
                continue;
            }

            if (member != null) {
                assert !(member.localMember() && member.equals(clusterService.getLocalMember()))
                        : "Local " + member + " cannot be replaced with " + memberInfo;

                // UUID changed: means member has gone and come back with a new uuid
                removedMembers.add(member);
            }

            member = createMember(memberInfo, memberInfo.getAttributes());
            addedMembers.add(member);

            long now = clusterService.getClusterTime();
            clusterHeartbeatManager.onHeartbeat(member, now);

            repairPartitionTableIfReturningMember(member);
            members[memberIndex++] = member;
        }

        MemberMap newMemberMap = membersView.toMemberMap();
        for (MemberImpl member : currentMemberMap.getMembers()) {
            if (!newMemberMap.contains(member.getAddress())) {
                removedMembers.add(member);
            }
        }

        setMembers(MemberMap.createNew(membersView.getVersion(), members));

        for (MemberImpl member : removedMembers) {
            closeConnection(member.getAddress(), "Member left event received from master");
            handleMemberRemove(memberMapRef.get(), member);
        }

        clusterService.getClusterJoinManager().insertIntoRecentlyJoinedMemberSet(addedMembers);
        sendMembershipEvents(currentMemberMap.getMembers(), addedMembers);

        removeFromMissingMembers(members);

        clusterHeartbeatManager.heartbeat();
        clusterService.printMemberList();

        //async call
        node.getNodeExtension().scheduleClusterVersionAutoUpgrade();
    }

    private MemberImpl createNewMemberImplIfChanged(MemberInfo newMemberInfo, MemberImpl member) {
        if (member.isLiteMember() && !newMemberInfo.isLiteMember()) {
            // lite member promoted
            logger.info(member + " is promoted to normal member.");
            if (member.localMember()) {
                member = clusterService.promoteAndGetLocalMember();
            } else {
                member = createMember(newMemberInfo, member.getAttributes());
            }
        } else if (member.getMemberListJoinVersion() != newMemberInfo.getMemberListJoinVersion()) {
            if (member.getMemberListJoinVersion() != NA_MEMBER_LIST_JOIN_VERSION) {
                if (logger.isFineEnabled()) {
                    logger.fine("Member list join version of " + member + " is changed to "
                            + newMemberInfo.getMemberListJoinVersion() + " from " + member.getMemberListJoinVersion());
                }
            }
            if (member.localMember()) {
                setLocalMemberListJoinVersion(newMemberInfo.getMemberListJoinVersion());
                member = clusterService.getLocalMember();
            } else {
                member = createMember(newMemberInfo, member.getAttributes());
            }
        }

        return member;
    }

    private MemberImpl createMember(MemberInfo memberInfo, Map<String, String> attributes) {
        Address address = memberInfo.getAddress();
        Address thisAddress = node.getThisAddress();
        String ipV6ScopeId = thisAddress.getScopeId();
        address.setScopeId(ipV6ScopeId);
        boolean localMember = thisAddress.equals(address);

        MemberImpl.Builder builder;
        if (memberInfo.getAddressMap() != null && memberInfo.getAddressMap().containsKey(MEMBER)) {
            builder = new MemberImpl.Builder(memberInfo.getAddressMap());
        } else {
            builder = new MemberImpl.Builder(memberInfo.getAddress());
        }

        return builder.version(memberInfo.getVersion())
                      .localMember(localMember)
                      .uuid(memberInfo.getUuid())
                      .attributes(attributes)
                      .liteMember(memberInfo.isLiteMember())
                      .memberListJoinVersion(memberInfo.getMemberListJoinVersion())
                      .instance(node.hazelcastInstance)
                      .build();
    }

    private void repairPartitionTableIfReturningMember(MemberImpl member) {
        if (!clusterService.isMaster()) {
            return;
        }

        if (clusterService.getClusterState().isMigrationAllowed()) {
            return;
        }

        if (!node.getNodeExtension().isStartCompleted()) {
            return;
        }

        MemberImpl missingMember = getMissingMember(member.getAddress(), member.getUuid());
        if (missingMember != null) {
            boolean repair;
            Level level;
            if (isHotRestartEnabled()) {
                repair = !missingMember.getAddress().equals(member.getAddress());
                level = Level.INFO;
            } else {
                repair = !missingMember.getUuid().equals(member.getUuid());
                level = Level.FINE;
            }
            if (repair) {
                logger.log(level, member + " is returning with a new identity. Old one was: " + missingMember
                        + ". Will update partition table with the new identity.");
                InternalPartitionServiceImpl partitionService = node.partitionService;
                partitionService.replaceMember(missingMember, member);
            }
        }
    }

    void setLocalMemberListJoinVersion(int memberListJoinVersion) {
        MemberImpl localMember = clusterService.getLocalMember();
        if (memberListJoinVersion != NA_MEMBER_LIST_JOIN_VERSION) {
            localMember.setMemberListJoinVersion(memberListJoinVersion);
            if (logger.isFineEnabled()) {
                logger.fine("Local member list join version is set to " + memberListJoinVersion);
            }
        } else if (logger.isFineEnabled()) {
            logger.fine("No member list join version is available during join. Local member list join version: "
                    + localMember.getMemberListJoinVersion());
        }
    }

    void setMembers(MemberMap memberMap) {
        if (logger.isFineEnabled()) {
            logger.fine("Setting members " + memberMap.getMembers() + ", version: " + memberMap.getVersion());
        }
        clusterServiceLock.lock();
        try {
            memberMapRef.set(memberMap);
            retainSuspectedMembers(memberMap);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    // called under cluster service lock
    private void retainSuspectedMembers(MemberMap memberMap) {
        Iterator<Address> it = suspectedMembers.iterator();
        while (it.hasNext()) {
            Address suspectedAddress = it.next();
            if (!memberMap.contains(suspectedAddress)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Removing suspected address " + suspectedAddress + ", it's no longer a member.");
                }

                it.remove();
            }
        }
    }

    boolean isMemberSuspected(Address address) {
        return suspectedMembers.contains(address);
    }

    boolean clearMemberSuspicion(Address address, String reason) {
        clusterServiceLock.lock();
        try {
            if (!suspectedMembers.contains(address)) {
                 return true;
            }

            MemberMap memberMap = getMemberMap();
            Address masterAddress = clusterService.getMasterAddress();
            if (memberMap.isBeforeThan(address, masterAddress)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Not removing suspicion of " + address + " since it is before than current master "
                            + masterAddress + " in member list.");
                }

                return false;
            }

            boolean removed = suspectedMembers.remove(address);
            if (removed && logger.isInfoEnabled()) {
                logger.info("Removed suspicion from " + address + ". Reason: " + reason);
            }

        } finally {
            clusterServiceLock.unlock();
        }
        return true;
    }

    void handleExplicitSuspicionTrigger(Address caller, int callerMemberListVersion,
            MembersViewMetadata suspectedMembersViewMetadata) {
        clusterServiceLock.lock();
        try {
            Address masterAddress = clusterService.getMasterAddress();
            int memberListVersion = getMemberListVersion();

            if (!(masterAddress.equals(caller) && memberListVersion == callerMemberListVersion)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Ignoring explicit suspicion trigger for " + suspectedMembersViewMetadata
                            + ". Caller: " + caller + ", caller member list version: " + callerMemberListVersion
                            + ", known master: " + masterAddress + ", local member list version: " + memberListVersion);
                }

                return;
            }

            clusterService.sendExplicitSuspicion(suspectedMembersViewMetadata);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void handleExplicitSuspicion(MembersViewMetadata expectedMembersViewMetadata, Address suspectedAddress) {
        clusterServiceLock.lock();
        try {
            MembersViewMetadata localMembersViewMetadata = createLocalMembersViewMetadata();
            if (!localMembersViewMetadata.equals(expectedMembersViewMetadata)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Ignoring explicit suspicion of " + suspectedAddress
                            + ". Expected: " + expectedMembersViewMetadata + ", Local: " + localMembersViewMetadata);
                }

                return;
            }

            MemberImpl suspectedMember = getMember(suspectedAddress);
            if (suspectedMember == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("No need for explicit suspicion, " + suspectedAddress + " is not a member.");
                }

                return;
            }

            suspectMember(suspectedMember, "explicit suspicion", true);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    MembersViewMetadata createLocalMembersViewMetadata() {
        return new MembersViewMetadata(node.getThisAddress(), clusterService.getThisUuid(),
                clusterService.getMasterAddress(), getMemberListVersion());
    }

    boolean validateMembersViewMetadata(MembersViewMetadata membersViewMetadata) {
        MemberImpl sender = getMember(membersViewMetadata.getMemberAddress(), membersViewMetadata.getMemberUuid());
        return sender != null && node.getThisAddress().equals(membersViewMetadata.getMasterAddress());
    }

    void suspectMember(MemberImpl suspectedMember, String reason, boolean shouldCloseConn) {
        assert !suspectedMember.equals(clusterService.getLocalMember()) : "Cannot suspect from myself!";
        assert !suspectedMember.localMember() : "Cannot be local member";

        final MemberMap localMemberMap;
        final Set<Member> membersToAsk;

        clusterServiceLock.lock();
        try {
            if (!clusterService.isJoined()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot handle suspect of " + suspectedMember + " because this node is not joined...");
                }

                return;
            }

            ClusterJoinManager clusterJoinManager = clusterService.getClusterJoinManager();
            if (clusterService.isMaster() && !clusterJoinManager.isMastershipClaimInProgress()) {
                removeMember(suspectedMember, reason, shouldCloseConn);
                return;
            }

            if (!addSuspectedMember(suspectedMember, reason, shouldCloseConn)) {
                return;
            }

            if (!tryStartMastershipClaim()) {
                return;
            }

            localMemberMap = getMemberMap();
            membersToAsk = collectMembersToAsk(localMemberMap);
            logger.info("Local " + localMemberMap.toMembersView() + " with suspected members: "  + suspectedMembers
                    + " and initial addresses to ask: " + membersToAsk);
        } finally {
            clusterServiceLock.unlock();
        }

        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(MASTERSHIP_CLAIM_EXECUTOR_NAME);
        executor.submit(new DecideNewMembersViewTask(localMemberMap, membersToAsk));
    }

    private Set<Member> collectMembersToAsk(MemberMap localMemberMap) {
        Set<Member> membersToAsk = new HashSet<>();
        for (MemberImpl member : localMemberMap.getMembers()) {
            if (member.localMember() || suspectedMembers.contains(member.getAddress())) {
                continue;
            }

            membersToAsk.add(member);
        }
        return membersToAsk;
    }

    private boolean tryStartMastershipClaim() {
        ClusterJoinManager clusterJoinManager = clusterService.getClusterJoinManager();
        if (clusterJoinManager.isMastershipClaimInProgress()) {
            return false;
        }

        MemberMap memberMap = memberMapRef.get();
        if (!shouldClaimMastership(memberMap)) {
            return false;
        }

        logger.info("Starting mastership claim process...");

        // Make sure that all pending join requests are cancelled temporarily.
        clusterJoinManager.setMastershipClaimInProgress();

        // pause migrations until mastership claim process completes
        node.getPartitionService().pauseMigration();

        clusterService.setMasterAddress(node.getThisAddress());
        return true;
    }

    private boolean addSuspectedMember(MemberImpl suspectedMember, String reason,
            boolean shouldCloseConn) {

        if (getMember(suspectedMember.getAddress(), suspectedMember.getUuid()) == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Cannot suspect " + suspectedMember + ", since it's not a member.");
            }

            return false;
        }

        if (suspectedMembers.add(suspectedMember.getAddress())) {
            if (reason != null) {
                logger.warning(suspectedMember + " is suspected to be dead for reason: " + reason);
            } else {
                logger.warning(suspectedMember + " is suspected to be dead");
            }
        }

        if (shouldCloseConn) {
            closeConnection(suspectedMember.getAddress(), reason);
        }
        return true;
    }

    private void removeMember(MemberImpl member, String reason, boolean shouldCloseConn) {
        clusterServiceLock.lock();
        try {
            assert clusterService.isMaster() : "Master: " + clusterService.getMasterAddress();

            if (!clusterService.isJoined()) {
                logger.warning("Not removing " + member + " for reason: " + reason + ", because not joined!");
                return;
            }

            if (shouldCloseConn) {
                closeConnection(member.getAddress(), reason);
            }

            MemberMap currentMembers = memberMapRef.get();
            if (currentMembers.getMember(member.getAddress(), member.getUuid()) == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("No need to remove " + member + ", not a member.");
                }

                return;
            }

            logger.info("Removing " + member);
            clusterService.getClusterJoinManager().removeJoin(member.getAddress());
            clusterService.getClusterHeartbeatManager().removeMember(member);

            MemberMap newMembers = MemberMap.cloneExcluding(currentMembers, member);
            setMembers(newMembers);

            if (logger.isFineEnabled()) {
                logger.fine(member + " is removed. Publishing new member list.");
            }
            sendMemberListToOthers();

            handleMemberRemove(newMembers, member);
            clusterService.printMemberList();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void closeConnection(Address address, String reason) {
        Connection conn = node.getEndpointManager(MEMBER).getConnection(address);
        if (conn != null) {
            conn.close(reason, null);
        }
    }

    private void handleMemberRemove(MemberMap newMembers, MemberImpl removedMember) {
        ClusterState clusterState = clusterService.getClusterState();
        if (!clusterState.isJoinAllowed()) {
            if (logger.isFineEnabled()) {
                logger.fine(removedMember + " is removed, added to members left while cluster is " + clusterState + " state");
            }

            InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
            if (!hotRestartService.isMemberExcluded(removedMember.getAddress(), removedMember.getUuid())) {
                addToMissingMembers(removedMember);
            }
        }

        onMemberRemove(removedMember);

        // async events
        sendMembershipEventNotifications(removedMember,
                unmodifiableSet(new LinkedHashSet<Member>(newMembers.getMembers())), false);
    }

    void onMemberRemove(MemberImpl deadMember) {
        // sync calls
        node.getPartitionService().memberRemoved(deadMember);
        nodeEngine.onMemberLeft(deadMember);
        node.getNodeExtension().onMemberListChange();
    }

    void sendMembershipEvents(Collection<MemberImpl> currentMembers, Collection<MemberImpl> newMembers) {
        Set<Member> eventMembers = new LinkedHashSet<>(currentMembers);
        if (!newMembers.isEmpty()) {
            for (MemberImpl newMember : newMembers) {
                // sync calls
                node.getPartitionService().memberAdded(newMember);
                node.getNodeExtension().onMemberListChange();

                // async events
                eventMembers.add(newMember);
                sendMembershipEventNotifications(newMember, unmodifiableSet(new LinkedHashSet<>(eventMembers)), true);
            }
        }
    }

    private void sendMembershipEventNotifications(MemberImpl member, Set<Member> members, final boolean added) {
        int eventType = added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED;
        MembershipEvent membershipEvent = new MembershipEvent(clusterService, member, eventType, members);
        Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            final MembershipServiceEvent event = new MembershipServiceEvent(membershipEvent);
            for (final MembershipAwareService service : membershipAwareServices) {
                nodeEngine.getExecutionService().execute(MEMBERSHIP_EVENT_EXECUTOR_NAME, () -> {
                    if (added) {
                        service.memberAdded(event);
                    } else {
                        service.memberRemoved(event);
                    }
                });
            }
        }
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        for (EventRegistration reg : registrations) {
            eventService.publishEvent(SERVICE_NAME, reg, membershipEvent, reg.getId().hashCode());
        }
    }

    private boolean shouldClaimMastership(MemberMap memberMap) {
        if (clusterService.isMaster()) {
            return false;
        }

        for (MemberImpl m : memberMap.headMemberSet(clusterService.getLocalMember(), false)) {
            if (!isMemberSuspected(m.getAddress())) {
                return false;
            }
        }

        return true;
    }

    private MembersView decideNewMembersView(MemberMap localMemberMap, Set<Member> members) {
        Map<Address, Future<MembersView>> futures = new HashMap<>();
        MembersView latestMembersView = fetchLatestMembersView(localMemberMap, members, futures);

        if (logger.isFineEnabled()) {
            logger.fine("Latest " + latestMembersView + " before final decision...");
        }

        // within the most recent members view, select the members that have reported their members view successfully
        List<MemberInfo> finalMembers = new ArrayList<>();
        for (MemberInfo memberInfo : latestMembersView.getMembers()) {
            Address address = memberInfo.getAddress();
            if (node.getThisAddress().equals(address)) {
                finalMembers.add(memberInfo);
                continue;
            }

            // if it is not certain if a member has accepted the mastership claim, its response will be ignored

            Future<MembersView> future = futures.get(address);
            if (isMemberSuspected(address)) {
                if (logger.isFineEnabled()) {
                    logger.fine(memberInfo + " is excluded because suspected");
                }

                continue;
            } else if (future == null || !future.isDone()) {
                if (logger.isFineEnabled()) {
                    logger.fine(memberInfo + " is excluded because I don't know its response");
                }

                continue;
            }

            addAcceptedMemberInfo(finalMembers, memberInfo, future);
        }

        int finalVersion = latestMembersView.getVersion() + 1;
        return new MembersView(finalVersion, finalMembers);
    }

    private void addAcceptedMemberInfo(List<MemberInfo> finalMembers, MemberInfo memberInfo, Future<MembersView> future) {
        try {
            future.get();
            finalMembers.add(memberInfo);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (logger.isFineEnabled()) {
                logger.fine(memberInfo + " is excluded because I couldn't get its acceptance", e);
            }
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    private MembersView fetchLatestMembersView(MemberMap localMemberMap,
                                               Set<Member> members,
                                               Map<Address, Future<MembersView>> futures) {
        MembersView latestMembersView = localMemberMap.toTailMembersView(node.getLocalMember(), true);

        // once an address is put into the futures map,
        // we wait until either we suspect of that address or find its result in the futures.

        for (Member member : members) {
            futures.put(member.getAddress(), invokeFetchMembersViewOp(member.getAddress(), member.getUuid()));
        }

        long mastershipClaimTimeout = TimeUnit.SECONDS.toMillis(mastershipClaimTimeoutSeconds);
        while (clusterService.isJoined()) {

            boolean done = true;
            for (Entry<Address, Future<MembersView>> e : new ArrayList<>(futures.entrySet())) {
                Address address = e.getKey();
                Future<MembersView> future = e.getValue();

                long start = System.nanoTime();
                try {
                    long timeout = Math.min(FETCH_MEMBER_LIST_MILLIS, Math.max(mastershipClaimTimeout, 1));
                    MembersView membersView = future.get(timeout, TimeUnit.MILLISECONDS);
                    if (membersView.isLaterThan(latestMembersView)) {
                        if (logger.isFineEnabled()) {
                            logger.fine("A more recent " + membersView + " is received from " + address);
                        }
                        latestMembersView = membersView;

                        // If we discover a new member via a fetched member list, we should also ask for its members view.
                        // there are some new members added to the futures map. lets wait for their results.
                        done &= !fetchMembersViewFromNewMembers(membersView, futures);
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException ignored) {
                    // we couldn't learn MembersView of 'address'. It will be removed from the cluster.
                    EmptyStatement.ignore(ignored);
                } catch (TimeoutException ignored) {
                    MemberInfo memberInfo = latestMembersView.getMember(address);
                    if (mastershipClaimTimeout > 0 && !isMemberSuspected(address) && memberInfo != null) {
                        // we don't suspect from 'address' and we need to learn its response
                        done = false;

                        // Mastership claim is idempotent.
                        // We will retry our claim to member until it explicitly rejects or accepts our claim.
                        // We can't just rely on invocation retries, because if connection is dropped while
                        // our claim is on the wire, invocation won't get any response and will eventually timeout.
                        futures.put(address, invokeFetchMembersViewOp(address, memberInfo.getUuid()));
                    }
                }

                mastershipClaimTimeout -= TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            }

            if (done) {
                break;
            }
        }

        return latestMembersView;
    }

    private boolean fetchMembersViewFromNewMembers(MembersView membersView, Map<Address, Future<MembersView>> futures) {
        boolean isNewMemberPresent = false;

        for (MemberInfo memberInfo : membersView.getMembers()) {
            Address memberAddress = memberInfo.getAddress();
            if (!(node.getThisAddress().equals(memberAddress)
                    || isMemberSuspected(memberAddress)
                    || futures.containsKey(memberAddress))) {
                // this is a new member for us. lets ask its members view
                if (logger.isFineEnabled()) {
                    logger.fine("Asking MembersView of " + memberAddress);
                }

                futures.put(memberAddress, invokeFetchMembersViewOp(memberAddress, memberInfo.getUuid()));
                isNewMemberPresent = true;
            }
        }

        return isNewMemberPresent;
    }

    private Future<MembersView> invokeFetchMembersViewOp(Address target, UUID targetUuid) {
        Operation op = new FetchMembersViewOp(targetUuid).setCallerUuid(clusterService.getThisUuid());

        return nodeEngine.getOperationService()
                .createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(mastershipClaimTimeoutSeconds)
                .setCallTimeout(TimeUnit.SECONDS.toMillis(mastershipClaimTimeoutSeconds)).invoke();
    }

    /**
     * Returns whether member with given identity (either {@code UUID} or {@code Address}
     * depending on Hot Restart is enabled or not) is a known missing member or not.
     *
     * @param address Address of the missing member
     * @param uuid Uuid of the missing member
     * @return true if it's a known missing member, false otherwise
     */
    boolean isMissingMember(Address address, UUID uuid) {
        Map<Object, MemberImpl> m = missingMembersRef.get();
        return isHotRestartEnabled() ? m.containsKey(uuid) : m.containsKey(address);
    }

    /**
     * Returns the missing member using either its {@code UUID} or its {@code Address}
     * depending on Hot Restart is enabled or not.
     *
     * @param address Address of the missing member
     * @param uuid Uuid of the missing member
     * @return the missing member
     */
    MemberImpl getMissingMember(Address address, UUID uuid) {
        Map<Object, MemberImpl> m = missingMembersRef.get();
        return isHotRestartEnabled() ? m.get(uuid) : m.get(address);
    }

    /**
     * Returns all missing members.
     */
    Collection<MemberImpl> getMissingMembers() {
        return Collections.unmodifiableCollection(missingMembersRef.get().values());
    }

    private void addToMissingMembers(MemberImpl... members) {
        Map<Object, MemberImpl> m = new HashMap<>(missingMembersRef.get());
        if (isHotRestartEnabled()) {
            for (MemberImpl member : members) {
                m.put(member.getUuid(), member);
            }
        } else {
            for (MemberImpl member : members) {
                m.put(member.getAddress(), member);
            }
        }
        missingMembersRef.set(unmodifiableMap(m));
    }

    private void removeFromMissingMembers(MemberImpl... members) {
        Map<Object, MemberImpl> m = new HashMap<>(missingMembersRef.get());
        if (isHotRestartEnabled()) {
            for (MemberImpl member : members) {
                m.remove(member.getUuid());
            }
        } else {
            for (MemberImpl member : members) {
                m.remove(member.getAddress());
            }
        }
        missingMembersRef.set(unmodifiableMap(m));
    }

    private boolean isHotRestartEnabled() {
        InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
        return hotRestartService.isEnabled();
    }

    Collection<Member> getActiveAndMissingMembers() {
        clusterServiceLock.lock();
        try {
            Map<Object, MemberImpl> m = missingMembersRef.get();
            if (m.isEmpty()) {
                return getMemberSet();
            }

            Collection<MemberImpl> removedMembers = m.values();
            Collection<MemberImpl> members = memberMapRef.get().getMembers();

            Collection<Member> allMembers = new ArrayList<>(members.size() + removedMembers.size());
            allMembers.addAll(members);
            allMembers.addAll(removedMembers);

            return allMembers;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void setMissingMembers(Collection<MemberImpl> members) {
        clusterServiceLock.lock();
        try {
            Map<Object, MemberImpl> m = new HashMap<>(members.size());
            if (isHotRestartEnabled()) {
                for (MemberImpl member : members) {
                    m.put(member.getUuid(), member);
                }
            } else {
                for (MemberImpl member : members) {
                    m.put(member.getAddress(), member);
                }
            }
            missingMembersRef.set(unmodifiableMap(m));
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void shrinkMissingMembers(Collection<UUID> memberUuidsToRemove) {
        clusterServiceLock.lock();
        try {
            Map<Object, MemberImpl> m = new HashMap<>(missingMembersRef.get());
            Iterator<MemberImpl> it = m.values().iterator();
            while (it.hasNext()) {
                MemberImpl member = it.next();
                if (memberUuidsToRemove.contains(member.getUuid())) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Removing " + member + " from members removed in not joinable state.");
                    }

                    it.remove();
                }
            }
            missingMembersRef.set(unmodifiableMap(m));
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void removeAllMissingMembers() {
        clusterServiceLock.lock();
        try {
            Map<Object, MemberImpl> m = missingMembersRef.get();
            Collection<MemberImpl> members = m.values();
            missingMembersRef.set(Collections.emptyMap());
            for (MemberImpl member : members) {
                onMemberRemove(member);
            }

        } finally {
            clusterServiceLock.unlock();
        }
    }

    public MembersView promoteToDataMember(Address address, UUID uuid) {
        clusterServiceLock.lock();
        try {
            ensureLiteMemberPromotionIsAllowed();

            MemberMap memberMap = getMemberMap();
            MemberImpl member = memberMap.getMember(address, uuid);
            if (member == null) {
                throw new IllegalStateException(uuid + "/" + address + " is not a member!");
            }

            if (!member.isLiteMember()) {
                if (logger.isFineEnabled()) {
                    logger.fine(member + " is not lite member, no promotion is required.");
                }

                return memberMap.toMembersView();
            }

            logger.info("Promoting " + member + " to normal member.");
            MemberImpl[] members = memberMap.getMembers().toArray(new MemberImpl[0]);
            for (int i = 0; i < members.length; i++) {
                if (member.equals(members[i])) {
                    if (member.localMember()) {
                        member = clusterService.promoteAndGetLocalMember();
                    } else {
                        member = new MemberImpl.Builder(member.getAddressMap())
                                .version(member.getVersion())
                                .localMember(member.localMember())
                                .uuid(member.getUuid())
                                .attributes(member.getAttributes())
                                .memberListJoinVersion(members[i].getMemberListJoinVersion())
                                .instance(node.hazelcastInstance)
                                .build();
                    }
                    members[i] = member;
                    break;
                }
            }

            MemberMap newMemberMap = MemberMap.createNew(memberMap.getVersion() + 1, members);
            setMembers(newMemberMap);
            sendMemberListToOthers();
            node.partitionService.memberAdded(member);
            clusterService.printMemberList();
            return newMemberMap.toMembersView();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private void ensureLiteMemberPromotionIsAllowed() {
        if (!clusterService.isMaster()) {
            throw new IllegalStateException("This node is not master!");
        }
        if (clusterService.getClusterJoinManager().isMastershipClaimInProgress()) {
            throw new IllegalStateException("Mastership claim is in progress!");
        }
        ClusterState state = clusterService.getClusterState();
        if (!state.isMigrationAllowed()) {
            throw new IllegalStateException("Lite member promotion is not allowed when cluster state is " + state);
        }
    }

    public boolean verifySplitBrainMergeMemberListVersion(SplitBrainJoinMessage joinMessage) {
        Address caller = joinMessage.getAddress();
        int callerMemberListVersion = joinMessage.getMemberListVersion();

        clusterServiceLock.lock();
        try {
            if (!clusterService.isMaster()) {
                logger.warning("Cannot verify member list version: " + callerMemberListVersion + " from " + caller
                        + " because this node is not master");
                return false;
            } else if (clusterService.getClusterJoinManager().isMastershipClaimInProgress()) {
                logger.warning("Cannot verify member list version: " + callerMemberListVersion + " from " + caller
                        + " because mastership claim is in progress");
                return false;
            }

            MemberMap memberMap = getMemberMap();
            if (memberMap.getVersion() < callerMemberListVersion) {
                int newVersion = callerMemberListVersion + 1;

                logger.info("Updating local member list version: " + memberMap.getVersion() + " to " + newVersion
                        + " because of split brain merge caller: " + caller + " with member list version: "
                        + callerMemberListVersion);

                MemberImpl[] members = memberMap.getMembers().toArray(new MemberImpl[0]);
                MemberMap newMemberMap = MemberMap.createNew(newVersion, members);
                setMembers(newMemberMap);
                sendMemberListToOthers();

                clusterService.printMemberList();
            }

            return true;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void reset() {
        clusterServiceLock.lock();
        try {
            memberMapRef.set(MemberMap.singleton(clusterService.getLocalMember()));
            missingMembersRef.set(Collections.emptyMap());
            suspectedMembers.clear();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    /**
     * This task is only created on master node.
     */
    private class DecideNewMembersViewTask implements Runnable {
        final MemberMap localMemberMap;
        final Set<Member> membersToAsk;

        DecideNewMembersViewTask(MemberMap localMemberMap, Set<Member> membersToAsk) {
            this.localMemberMap = localMemberMap;
            this.membersToAsk = membersToAsk;
        }

        @Override
        public void run() {
            try {
                innerRun();
            } catch (Throwable e) {
                logger.warning("Exception thrown while running DecideNewMembersViewTask", e);
            } finally {
                // Resume migrations, they are disabled when mastership claim is started
                node.getPartitionService().resumeMigration();
            }
        }

        private void innerRun() {
            MembersView newMembersView = decideNewMembersView(localMemberMap, membersToAsk);
            clusterServiceLock.lock();
            try {
                if (!clusterService.isJoined()) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Ignoring decided members view after mastership claim: " + newMembersView
                                + ", because not joined!");
                    }

                    return;
                }

                MemberImpl localMember = clusterService.getLocalMember();
                if (!newMembersView.containsMember(localMember.getAddress(), localMember.getUuid())) {
                    // local member UUID is changed because of force start or split brain merge...
                    if (logger.isFineEnabled()) {
                        logger.fine("Ignoring decided members view after mastership claim: " + newMembersView
                                + ", because current local member: " + localMember + " not in decided members view.");
                    }

                    return;
                }

                updateMembers(newMembersView);
                clusterService.getClusterJoinManager().reset();
                sendMemberListToOthers();
                logger.info("Mastership is claimed with: " + newMembersView);
            } finally {
                clusterServiceLock.unlock();
            }
        }
    }
}

