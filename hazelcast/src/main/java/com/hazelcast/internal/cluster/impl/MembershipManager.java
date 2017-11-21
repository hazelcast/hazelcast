/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.operations.FetchMembersViewOp;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.EmptyStatement;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.EXECUTOR_NAME;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.MEMBERSHIP_EVENT_EXECUTOR_NAME;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.SERVICE_NAME;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.spi.properties.GroupProperty.MASTERSHIP_CLAIM_TIMEOUT_SECONDS;
import static java.util.Collections.unmodifiableSet;

/**
 * MembershipManager maintains member list and version, manages member update, suspicion and removal mechanisms.
 * Also initiates and manages mastership claim process.
 *
 * @since 3.9
 */
@SuppressWarnings("checkstyle:methodcount")
public class MembershipManager {

    private static final long FETCH_MEMBERLIST_SLEEP_INTERVAL = 100;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final Lock clusterServiceLock;
    private final ILogger logger;

    private final AtomicReference<MemberMap> memberMapRef = new AtomicReference<MemberMap>(MemberMap.empty());

    private final AtomicReference<MemberMap> membersRemovedInNotJoinableStateRef
            = new AtomicReference<MemberMap>(MemberMap.empty());

    private final Set<Address> suspectedMembers = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());
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

        long memberListPublishInterval = hazelcastProperties.getSeconds(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS);
        memberListPublishInterval = (memberListPublishInterval > 0 ? memberListPublishInterval : 1);
        executionService.scheduleWithRepetition(EXECUTOR_NAME, new Runnable() {
            public void run() {
                publishMemberList();
            }
        }, memberListPublishInterval, memberListPublishInterval, TimeUnit.SECONDS);
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

    public MemberImpl getMember(String uuid) {
        assert uuid != null : "UUID required!";

        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(uuid);
    }

    public MemberImpl getMember(Address address, String uuid) {
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
                logger.fine("Cannot publish member list to " + target + ". Is-master: "
                        + clusterService.isMaster() + ", joined: " + clusterService.isJoined());
                return;
            }
            if (clusterService.getThisAddress().equals(target)) {
                return;
            }

            MemberMap memberMap = memberMapRef.get();
            MemberImpl member = memberMap.getMember(target);
            if (member == null) {
                logger.fine("Not member: " + target + ", cannot send member list.");
                return;
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
            logger.fine("Cannot publish member list to cluster. Is-master: "
                    + clusterService.isMaster() + ", joined: " + clusterService.isJoined()
                    + " , mastership claim in progress: " + clusterService.getClusterJoinManager().isMastershipClaimInProgress());
            return;
        }

        MemberMap memberMap = getMemberMap();
        MembersView membersView = memberMap.toMembersView();

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

    // handles both new and left members
    void updateMembers(MembersView membersView) {
        MemberMap currentMemberMap = memberMapRef.get();

        Collection<MemberImpl> addedMembers = new LinkedList<MemberImpl>();
        Collection<MemberImpl> removedMembers = new LinkedList<MemberImpl>();
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

                // uuid changed: means member has gone and come back with a new uuid
                removedMembers.add(member);
            }

            member = createMember(memberInfo, memberInfo.getAttributes());
            addedMembers.add(member);

            long now = clusterService.getClusterTime();
            clusterHeartbeatManager.onHeartbeat(member, now);
            clusterHeartbeatManager.acceptMasterConfirmation(member, now);

            clusterService.repairPartitionTableIfReturningMember(member);
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
            handleMemberRemove(memberMapRef.get(), member);
        }

        sendMembershipEvents(currentMemberMap.getMembers(), addedMembers);

        MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
        membersRemovedInNotJoinableStateRef.set(MemberMap.cloneExcluding(membersRemovedInNotJoinableState, members));

        clusterHeartbeatManager.heartbeat();
        clusterService.printMemberList();
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
        }
        return member;
    }

    private MemberImpl createMember(MemberInfo memberInfo, Map<String, Object> attributes) {
        Address address = memberInfo.getAddress();
        Address thisAddress = node.getThisAddress();
        String ipV6ScopeId = thisAddress.getScopeId();
        address.setScopeId(ipV6ScopeId);
        boolean localMember = thisAddress.equals(address);

        return new MemberImpl(address, memberInfo.getVersion(), localMember, memberInfo.getUuid(), attributes,
                memberInfo.isLiteMember(), node.hazelcastInstance);
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
                logger.fine("Removing suspected address " + suspectedAddress + ", it's no longer a member.");
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
                logger.fine("Not removing suspicion of " + address + " since it is before than current master "
                        + masterAddress + " in member list.");
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
                logger.fine("Ignoring explicit suspicion trigger for " + suspectedMembersViewMetadata
                        + ". Caller: " + caller + ", caller member list version: " + callerMemberListVersion
                        + ", known master: " + masterAddress + ", local member list version: " + memberListVersion);
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
                logger.fine("Ignoring explicit suspicion of " + suspectedAddress
                        + ". Expected: " + expectedMembersViewMetadata + ", Local: " + localMembersViewMetadata);
                return;
            }

            MemberImpl suspectedMember = getMember(suspectedAddress);
            if (suspectedMember == null) {
                logger.fine("No need for explicit suspicion, " + suspectedAddress + " is not a member.");
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

        final MembersView localMemberView;
        final Set<Member> membersToAsk;

        clusterServiceLock.lock();
        try {
            if (!clusterService.isJoined()) {
                logger.fine("Cannot handle suspect of " + suspectedMember + " because this node is not joined...");
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

            MemberMap memberMap = getMemberMap();
            localMemberView = memberMap.toMembersView();
            membersToAsk = new HashSet<Member>();
            for (MemberImpl member : memberMap.getMembers()) {
                if (member.localMember() || suspectedMembers.contains(member.getAddress())) {
                    continue;
                }

                membersToAsk.add(member);
            }
            logger.info("Local " + localMemberView + " with suspected members: "  + suspectedMembers
                    + " and initial addresses to ask: " + membersToAsk);
        } finally {
            clusterServiceLock.unlock();
        }

        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        executor.submit(new DecideNewMembersViewTask(localMemberView, membersToAsk));
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

        clusterService.setMasterAddress(node.getThisAddress());
        return true;
    }

    private boolean addSuspectedMember(MemberImpl suspectedMember, String reason,
            boolean shouldCloseConn) {

        if (getMember(suspectedMember.getAddress(), suspectedMember.getUuid()) == null) {
            logger.fine("Cannot suspect " + suspectedMember + ", since it's not a member.");
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
            assert clusterService.getClusterVersion().isGreaterOrEqual(Versions.V3_9);

            if (!clusterService.isJoined()) {
                logger.warning("Not removing " + member + " for reason: " + reason + ", because not joined!");
                return;
            }

            if (shouldCloseConn) {
                closeConnection(member.getAddress(), reason);
            }

            MemberMap currentMembers = memberMapRef.get();
            if (currentMembers.getMember(member.getAddress(), member.getUuid()) == null) {
                logger.fine("No need to remove " + member + ", not a member.");
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
        Connection conn = node.connectionManager.getConnection(address);
        if (conn != null) {
            conn.close(reason, null);
        }
    }

    void handleMemberRemove(MemberMap newMembers, MemberImpl removedMember) {
        ClusterState clusterState = clusterService.getClusterState();
        if (!clusterState.isJoinAllowed()) {
            if (logger.isFineEnabled()) {
                logger.fine(removedMember + " is removed, added to members left while cluster is " + clusterState + " state");
            }

            final InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
            if (!hotRestartService.isMemberExcluded(removedMember.getAddress(), removedMember.getUuid())) {
                MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
                membersRemovedInNotJoinableStateRef
                        .set(MemberMap.cloneAdding(membersRemovedInNotJoinableState, removedMember));
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
        Set<Member> eventMembers = new LinkedHashSet<Member>(currentMembers);
        if (!newMembers.isEmpty()) {
            for (MemberImpl newMember : newMembers) {
                // sync calls
                node.getPartitionService().memberAdded(newMember);
                node.getNodeExtension().onMemberListChange();

                // async events
                eventMembers.add(newMember);
                sendMembershipEventNotifications(newMember, unmodifiableSet(new LinkedHashSet<Member>(eventMembers)), true);
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
                nodeEngine.getExecutionService().execute(MEMBERSHIP_EVENT_EXECUTOR_NAME, new Runnable() {
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

    private MembersView decideNewMembersView(MembersView localMembersView, Set<Member> members) {
        Map<Address, Future<MembersView>> futures = new HashMap<Address, Future<MembersView>>();
        MembersView latestMembersView = fetchLatestMembersView(localMembersView, members, futures);

        logger.fine("Latest " + latestMembersView + " before final decision...");

        // within the most recent members view, select the members that have reported their members view successfully
        int finalVersion = latestMembersView.getVersion() + 1;
        List<MemberInfo> finalMembers = new ArrayList<MemberInfo>();
        for (MemberInfo memberInfo : latestMembersView.getMembers()) {
            Address address = memberInfo.getAddress();
            if (node.getThisAddress().equals(address)) {
                finalMembers.add(memberInfo);
                continue;
            }

            // if it is not certain if a member has accepted the mastership claim, its response will be ignored

            Future<MembersView> future = futures.get(address);
            if (isMemberSuspected(address)) {
                logger.fine(memberInfo + " is excluded because suspected");
                continue;
            } else if (future == null || !future.isDone()) {
                logger.fine(memberInfo + " is excluded because I don't know its response");
                continue;
            }

            try {
                future.get();
                finalMembers.add(memberInfo);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                logger.fine(memberInfo + " is excluded because I couldn't get its acceptance", e);
            }
        }

        return new MembersView(finalVersion, finalMembers);
    }

    private MembersView fetchLatestMembersView(MembersView localMembersView,
                                               Set<Member> members,
                                               Map<Address, Future<MembersView>> futures) {
        MembersView latestMembersView = localMembersView;

        // once an address is put into the futures map,
        // we wait until either we suspect of that address or find its result in the futures.

        for (Member member : members) {
            futures.put(member.getAddress(), invokeFetchMembersViewOp(member.getAddress(), member.getUuid()));
        }

        while (clusterService.isJoined()) {
            boolean done = true;

            for (Entry<Address, Future<MembersView>> e : new ArrayList<Entry<Address, Future<MembersView>>>(futures.entrySet())) {
                Address address = e.getKey();
                Future<MembersView> future = e.getValue();

                if (future.isDone()) {
                    try {
                        MembersView membersView = future.get();
                        if (membersView.isLaterThan(latestMembersView)) {
                            logger.fine("A more recent " + membersView + " is received from " + address);
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
                    }
                } else if (!isMemberSuspected(address) && latestMembersView.containsAddress(address)) {
                    // we don't suspect from 'address' and we need to learn its response
                    done = false;
                }
            }

            if (done) {
                break;
            }

            try {
                Thread.sleep(FETCH_MEMBERLIST_SLEEP_INTERVAL);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
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
                logger.fine("Asking MembersView of " + memberAddress);
                futures.put(memberAddress, invokeFetchMembersViewOp(memberAddress, memberInfo.getUuid()));
                isNewMemberPresent = true;
            }
        }

        return isNewMemberPresent;
    }

    private Future<MembersView> invokeFetchMembersViewOp(Address target, String targetUuid) {
        Operation op = new FetchMembersViewOp(targetUuid).setCallerUuid(clusterService.getThisUuid());

        return nodeEngine.getOperationService()
                .createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(mastershipClaimTimeoutSeconds)
                .setCallTimeout(TimeUnit.SECONDS.toMillis(mastershipClaimTimeoutSeconds)).invoke();
    }

    boolean isMemberRemovedInNotJoinableState(Address target) {
        MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
        return membersRemovedInNotJoinableState.contains(target);
    }

    boolean isMemberRemovedInNotJoinableState(String uuid) {
        MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
        return membersRemovedInNotJoinableState.contains(uuid);
    }

    MemberImpl getMemberRemovedInNotJoinableState(String uuid) {
        MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
        return membersRemovedInNotJoinableState.getMember(uuid);
    }

    Collection<Member> getCurrentMembersAndMembersRemovedInNotJoinableState() {
        clusterServiceLock.lock();
        try {
            MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
            if (membersRemovedInNotJoinableState.size() == 0) {
                return getMemberSet();
            }

            Collection<MemberImpl> removedMembers = membersRemovedInNotJoinableState.getMembers();
            Collection<MemberImpl> members = memberMapRef.get().getMembers();

            Collection<Member> allMembers = new ArrayList<Member>(members.size() + removedMembers.size());
            allMembers.addAll(members);
            allMembers.addAll(removedMembers);

            return allMembers;
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void addMembersRemovedInNotJoinableState(Collection<MemberImpl> members) {
        clusterServiceLock.lock();
        try {
            members.remove(clusterService.getLocalMember());
            MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
            membersRemovedInNotJoinableStateRef.set(MemberMap.cloneAdding(membersRemovedInNotJoinableState,
                    members.toArray(new MemberImpl[0])));
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void shrinkMembersRemovedInNotJoinableState(Collection<String> memberUuidsToRemove) {
        clusterServiceLock.lock();
        try {
            Set<MemberImpl> membersRemoved
                    = new LinkedHashSet<MemberImpl>(membersRemovedInNotJoinableStateRef.get().getMembers());

            Iterator<MemberImpl> it = membersRemoved.iterator();
            while (it.hasNext()) {
                MemberImpl member = it.next();
                if (memberUuidsToRemove.contains(member.getUuid())) {
                    logger.fine("Removing " + member + " from members removed in not joinable state.");
                    it.remove();
                }
            }
            membersRemovedInNotJoinableStateRef.set(MemberMap.createNew(membersRemoved.toArray(new MemberImpl[0])));
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void removeMembersDeadInNotJoinableState() {
        clusterServiceLock.lock();
        try {
            MemberMap membersRemovedInNotJoinableState = membersRemovedInNotJoinableStateRef.get();
            Collection<MemberImpl> members = membersRemovedInNotJoinableState.getMembers();
            membersRemovedInNotJoinableStateRef.set(MemberMap.empty());
            for (MemberImpl member : members) {
                onMemberRemove(member);
            }

        } finally {
            clusterServiceLock.unlock();
        }
    }

    public MembersView promoteToNormalMember(Address address, String uuid) {
        clusterServiceLock.lock();
        try {
            ensureLiteMemberPromotionIsAllowed();

            MemberMap memberMap = memberMapRef.get();
            MemberImpl member = memberMap.getMember(address, uuid);
            if (member == null) {
                throw new IllegalStateException(uuid + "/" + address + " is not a member!");
            }

            if (!member.isLiteMember()) {
                logger.fine(member + " is not lite member, no promotion is required.");
                return memberMap.toMembersView();
            }

            logger.info("Promoting " + member + " to normal member.");
            MemberImpl[] members = memberMap.getMembers().toArray(new MemberImpl[0]);
            for (int i = 0; i < members.length; i++) {
                if (member.equals(members[i])) {
                    if (member.localMember()) {
                        member = clusterService.promoteAndGetLocalMember();
                    } else {
                        member = new MemberImpl(member.getAddress(), member.getVersion(), member.localMember(), member.getUuid(),
                                member.getAttributes(), false, node.hazelcastInstance);
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

    void reset() {
        clusterServiceLock.lock();
        try {
            memberMapRef.set(MemberMap.singleton(clusterService.getLocalMember()));
            membersRemovedInNotJoinableStateRef.set(MemberMap.empty());
            suspectedMembers.clear();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private class DecideNewMembersViewTask implements Runnable {

        final MembersView localMemberView;
        final Set<Member> membersToAsk;

        DecideNewMembersViewTask(MembersView localMemberView, Set<Member> membersToAsk) {
            this.localMemberView = localMemberView;
            this.membersToAsk = membersToAsk;
        }

        @Override
        public void run() {
            MembersView newMembersView = decideNewMembersView(localMemberView, membersToAsk);
            clusterServiceLock.lock();
            try {
                if (!clusterService.isJoined()) {
                    logger.fine("Ignoring decided members view after mastership claim: " + newMembersView
                            + ", because not joined!");
                    return;
                }

                MemberImpl localMember = clusterService.getLocalMember();
                if (!newMembersView.containsMember(localMember.getAddress(), localMember.getUuid())) {
                    // local member uuid is changed because of force start or split brain merge...
                    logger.fine("Ignoring decided members view after mastership claim: " + newMembersView
                            + ", because current local member: " + localMember + " not in decided members view.");
                    return;
                }

                updateMembers(newMembersView);
                clusterService.getClusterHeartbeatManager().resetMemberMasterConfirmations();
                clusterService.getClusterJoinManager().reset();
                sendMemberListToOthers();
                logger.info("Mastership is claimed with: " + newMembersView);
            } finally {
                clusterServiceLock.unlock();
            }
        }
    }
}

