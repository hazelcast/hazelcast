/*
 * Copyright (c) 2008 - 2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.impl.operations.FetchMemberListStateOperation;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOperation;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
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
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.EXECUTOR_NAME;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.MEMBERSHIP_EVENT_EXECUTOR_NAME;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.SERVICE_NAME;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static java.util.Collections.unmodifiableSet;

/**
 * TODO: Javadoc Pending...
 *
 */
public class MembershipManager {

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ClusterServiceImpl clusterService;
    private final Lock clusterServiceLock;
    private final ILogger logger;

    private final AtomicReference<MemberMap> memberMapRef = new AtomicReference<MemberMap>(MemberMap.empty());

    private final AtomicReference<MemberMap> membersRemovedInNotActiveStateRef
            = new AtomicReference<MemberMap>(MemberMap.empty());

    private final Map<Address, Long> suspectedMembers = new HashMap<Address, Long>();

    MembershipManager(Node node, ClusterServiceImpl clusterService, Lock clusterServiceLock) {
        this.node = node;
        this.clusterService = clusterService;
        this.clusterServiceLock = clusterServiceLock;
        this.nodeEngine = node.getNodeEngine();
        this.logger = node.getLogger(getClass());
        
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
                sendMemberListToOthers();
            }
        }, memberListPublishInterval, memberListPublishInterval, TimeUnit.SECONDS);
    }

    private void registerThisMember() {
        MemberImpl thisMember = node.getLocalMember();
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

    MemberImpl getMember(Address address, String uuid) {
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

    public MemberMap getMemberMap() {
        return memberMapRef.get();
    }

    MembersView createMembersView() {
        return memberMapRef.get().toMembersView();
    }

    public int getMemberListVersion() {
        return memberMapRef.get().getVersion();
    }

    public void sendMemberListToMember(Address target) {
        if (!node.isMaster()) {
            return;
        }
        if (clusterService.getThisAddress().equals(target)) {
            return;
        }

        MemberMap memberMap = memberMapRef.get();
        MemberImpl member = memberMap.getMember(target);
        String memberUuid = member != null ? member.getUuid() : null;

        MembersUpdateOperation op = new MembersUpdateOperation(memberUuid, memberMap.toMembersView(),
                clusterService.getClusterTime(), null, false);
        nodeEngine.getOperationService().send(op, target);
    }

    /** Invoked on the master to send the member list (see {@link MembersUpdateOperation}) to non-master nodes. */
    private void sendMemberListToOthers() {
        if (!node.isMaster()) {
            return;
        }

        MemberMap memberMap = getMemberMap();
        MembersView membersView = memberMap.toMembersView();

        for (MemberImpl member : memberMap.getMembers()) {
            if (member.localMember()) {
                continue;
            }

            MembersUpdateOperation op = new MembersUpdateOperation(member.getUuid(), membersView,
                    clusterService.getClusterTime(), null, false);
            nodeEngine.getOperationService().send(op, member.getAddress());
        }
    }

    // handles both new and left members
    // TODO: improve member update path, handling new & removed members etc...
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
                members[memberIndex++] = member;
                continue;
            }

            if (member != null) {
                // uuid changed: means member has gone and come back with a new uuid
                removedMembers.add(member);
            }

            member = createMember(memberInfo);
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

        // TODO: handle removed members
        for (MemberImpl member : removedMembers) {
            handleMemberRemove(memberMapRef.get(), member);
        }

        sendMembershipEvents(currentMemberMap.getMembers(), addedMembers);

        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        membersRemovedInNotActiveStateRef.set(MemberMap.cloneExcluding(membersRemovedInNotActiveState, members));

        clusterHeartbeatManager.heartbeat();
        clusterService.printMemberList();
    }

    private MemberImpl createMember(MemberInfo memberInfo) {
        Address address = memberInfo.getAddress();
        Address thisAddress = node.getThisAddress();
        String ipV6ScopeId = thisAddress.getScopeId();
        address.setScopeId(ipV6ScopeId);
        boolean localMember = thisAddress.equals(address);
        
        return new MemberImpl(address, memberInfo.getVersion(), localMember, memberInfo.getUuid(),
                node.hazelcastInstance, memberInfo.getAttributes(), memberInfo.isLiteMember());
    }

    void setMembers(MemberMap memberMap) {
        if (logger.isFineEnabled()) {
            logger.fine("Setting members " + memberMap.getMembers() + ", version: " + memberMap.getVersion());
        }
        clusterServiceLock.lock();
        try {
            memberMapRef.set(memberMap);
            retainSuspectedMembers();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    // called under cluster service lock
    private void retainSuspectedMembers() {
        Iterator<Map.Entry<Address, Long>> it = suspectedMembers.entrySet().iterator();
        while (it.hasNext()) {
            Address suspectedAddress = it.next().getKey();
            if (getMember(suspectedAddress) == null) {
                it.remove();
            }
        }
    }

    boolean isMemberSuspected(Address address) {
        clusterServiceLock.lock();
        try {
            return suspectedMembers.containsKey(address);
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void suspectAddress(Address suspectedAddress, String suspectedUuid, String reason, boolean destroyConnection) {
        if (!ensureMemberIsRemovable(suspectedAddress)) {
            return;
        }

        final MembersView localMemberView;
        final Set<Address> membersToAsk;
        clusterServiceLock.lock();
        try {
            boolean claimMastership = doSuspectAddress(suspectedAddress, suspectedUuid, reason, destroyConnection);
            if (!claimMastership) {
                return;
            }

            MemberMap memberMap = getMemberMap();
            localMemberView = memberMap.toMembersView();
            membersToAsk = new HashSet<Address>();
            for (MemberImpl member : memberMap.getMembers()) {
                if (member.localMember() || suspectedMembers.containsKey(member.getAddress())) {
                    continue;
                }

                membersToAsk.add(member.getAddress());
            }
        } finally {
            clusterServiceLock.unlock();
        }

        logger.info("Local " + localMemberView + " with suspected members: "  + suspectedMembers.keySet() + " and initial addresses to ask: " + membersToAsk);
        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        executor.submit(new FetchMostRecentMemberListTask(localMemberView, membersToAsk));
    }

    // called under cluster service lock
    private boolean doSuspectAddress(Address suspectedAddress, String suspectedUuid, String reason, boolean destroyConnection) {
        MemberImpl suspectedMember = getMember(suspectedAddress);
        if (suspectedUuid != null && (suspectedMember == null || !suspectedUuid.equals(suspectedMember.getUuid()))) {
            if (logger.isFineEnabled()) {
                logger.fine("Cannot suspect " + suspectedAddress + ", either member is not present "
                        + "or uuid is not matching. Uuid: " + suspectedUuid + ", member: " + suspectedMember);
            }
            return false;
        }

        ClusterJoinManager clusterJoinManager = clusterService.getClusterJoinManager();
        if (node.isMaster() && !clusterJoinManager.isMastershipClaimInProgress()) {
            doRemoveAddress(suspectedAddress, reason, destroyConnection);
            return false;
        } else {
            if (suspectedMember == null || suspectedMembers.containsKey(suspectedAddress)) {
                return false;
            }

            suspectedMembers.put(suspectedAddress, 0L);
            if (reason != null) {
                logger.warning(suspectedAddress + " is suspected to be dead for reason: " + reason);
            } else {
                logger.warning(suspectedAddress + " is suspected to be dead");
            }

            Connection conn = node.connectionManager.getConnection(suspectedAddress);
            if (destroyConnection && conn != null) {
                conn.close(reason, null);
            }

            if (clusterJoinManager.isMastershipClaimInProgress()) {
                return false;
            }

            MemberMap memberMap = memberMapRef.get();
            if (!shouldClaimMastership(memberMap)) {
                return false;
            }

            logger.info("Starting mastership claim process...");

            // TODO [basri] should be here or after the master address is updated?
            // TODO [basri] We need to make sure that all pending join requests are cancelled temporarily.
            clusterJoinManager.setMastershipClaimInProgress();

            // TODO [basri] update master address
            node.setMasterAddress(node.getThisAddress());
            return true;
        }
    }

    // TODO: called only on master 
    private void doRemoveAddress(Address address, String reason, boolean destroyConnection) {
        if (!ensureMemberIsRemovable(address)) {
            return;
        }

        assert node.isMaster() : "Master: " + node.getMasterAddress();

        clusterServiceLock.lock();
        try {
            clusterService.getClusterJoinManager().removeJoin(address);

            Connection conn = node.connectionManager.getConnection(address);
            if (destroyConnection && conn != null) {
                conn.close(reason, null);
            }

            removeMember(address);

        } finally {
            clusterServiceLock.unlock();
        }
    }

    // TODO [basri] should be called only within master
    private void removeMember(Address address) {
        assert clusterService.getClusterVersion().isGreaterOrEqual(Versions.V3_9);
        assert node.isMaster() : "Master: " + node.getMasterAddress();

        clusterServiceLock.lock();
        try {
            ClusterHeartbeatManager clusterHeartbeatManager = clusterService.getClusterHeartbeatManager();
            MemberMap currentMembers = memberMapRef.get();
            MemberImpl member = currentMembers.getMember(address);
            if (member != null) {
                logger.info("Removing " + member);
                clusterHeartbeatManager.removeMember(member);
                MemberMap newMembers = MemberMap.cloneExcluding(currentMembers, member);
                setMembers(newMembers);

                if (logger.isFineEnabled()) {
                    logger.fine(member + " is removed. Publishing new member list.");
                }
                sendMemberListToOthers();

                handleMemberRemove(newMembers, member);
            } else {
                logger.fine("No member to remove with address: " + address);
            }
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void handleMemberRemove(MemberMap newMembers, MemberImpl removedMember) {
        ClusterState clusterState = clusterService.getClusterState();
        if (clusterState != ClusterState.ACTIVE) {
            if (logger.isFineEnabled()) {
                logger.fine(removedMember + " is removed, added to members left while cluster is " + clusterState + " state");
            }

            final InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
            if (!hotRestartService.isMemberExcluded(removedMember.getAddress(), removedMember.getUuid())) {
                MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
                membersRemovedInNotActiveStateRef
                        .set(MemberMap.cloneAdding(membersRemovedInNotActiveState, removedMember));
            }

            InternalPartitionServiceImpl partitionService = node.partitionService;
            partitionService.cancelReplicaSyncRequestsTo(removedMember.getAddress());
        } else {
            onMemberRemove(removedMember);
        }

        // async events
        sendMembershipEventNotifications(removedMember,
                unmodifiableSet(new LinkedHashSet<Member>(newMembers.getMembers())), false);
    }

    void onMemberRemove(MemberImpl deadMember) {
        // sync call
        node.getPartitionService().memberRemoved(deadMember);
        // sync call
        nodeEngine.onMemberLeft(deadMember);
    }

    void sendMembershipEvents(Collection<MemberImpl> currentMembers, Collection<MemberImpl> newMembers) {
        Set<Member> eventMembers = new LinkedHashSet<Member>(currentMembers);
        if (!newMembers.isEmpty()) {
            if (newMembers.size() == 1) {
                MemberImpl newMember = newMembers.iterator().next();
                // sync call
                node.getPartitionService().memberAdded(newMember);

                // async events
                eventMembers.add(newMember);
                sendMembershipEventNotifications(newMember, unmodifiableSet(eventMembers), true);
            } else {
                for (MemberImpl newMember : newMembers) {
                    // sync call
                    node.getPartitionService().memberAdded(newMember);

                    // async events
                    eventMembers.add(newMember);
                    sendMembershipEventNotifications(newMember, unmodifiableSet(new LinkedHashSet<Member>(eventMembers)), true);
                }
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
        if (node.isMaster()) {
            return false;
        }

        // TODO [basri] what if I am shutting down?

        for (MemberImpl m : memberMap.headMemberSet(node.getLocalMember(), false)) {
            if (!isMemberSuspected(m.getAddress())) {
                return false;
            }
        }

        return true;
    }

    private MembersView decideNewMembersView(MembersView localMembersView, Set<Address> addresses) {
        Map<Address, Future<MembersView>> futures = new HashMap<Address, Future<MembersView>>();

        MembersView mostRecentMembersView = fetchMembersViews(localMembersView, addresses, futures);

        logger.fine("Most recent " + mostRecentMembersView + " before final decision...");

        // within the most recent members view, select the members that have reported their members view successfully
        int finalVersion = mostRecentMembersView.getVersion() + 1;
        List<MemberInfo> finalMembers = new ArrayList<MemberInfo>();
        for (MemberInfo memberInfo : mostRecentMembersView.getMembers()) {
            Address address = memberInfo.getAddress();
            if (clusterService.getThisAddress().equals(address)) {
                finalMembers.add(memberInfo);
                continue;
            }

            // if we are not sure that a member has accepted my mastership claim, ignore its result

            Future<MembersView> membersViewFuture = futures.get(address);
            // TODO [basri] could it be that `membersViewFuture == null` ?
            if (isMemberSuspected(address)) {
                logger.fine(memberInfo + " is excluded because suspected");
                continue;
            } else if (membersViewFuture == null) {
                logger.fine(memberInfo + " is excluded because I haven't asked to it");
                continue;
            } else if (!membersViewFuture.isDone()) {
                logger.fine(memberInfo + " is excluded because I don't know its response");
                continue;
            }

            try {
                membersViewFuture.get();
                finalMembers.add(memberInfo);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                logger.fine(memberInfo + " is excluded because I couldn't get its response", e);
            }
        }

        return new MembersView(finalVersion, finalMembers);
    }

    private MembersView fetchMembersViews(MembersView localMembersView,
            Set<Address> addresses,
            Map<Address, Future<MembersView>> futures) {
        MembersView mostRecentMembersView = localMembersView;

        // once an address is put into the futures map,
        // we wait until either we suspect of that address or find its result in the futures.

        for (Address address : addresses) {
            futures.put(address, invokeFetchMemberListStateOperation(address));
        }

        while (true) {
            boolean done = true;

            for (Map.Entry<Address, Future<MembersView>> e : new ArrayList<Map.Entry<Address, Future<MembersView>>>(futures.entrySet())) {
                Address address = e.getKey();
                Future<MembersView> future = e.getValue();

                // If we started to suspect a member after asking its member list, we don't need to wait for its result.
                if (!isMemberSuspected(address)) {
                    // If there is no suspicion yet, we just keep waiting till we have a successful or failed result.
                    if (future.isDone()) {
                        try {
                            MembersView membersView = future.get();
                            if (membersView.getVersion() > mostRecentMembersView.getVersion()) {
                                logger.fine("A more recent " + membersView + " is received from " + address);
                                mostRecentMembersView = membersView;

                                // If we discover a new member via a fetched member list, we should also ask for its members view.
                                if (checkFetchedMembersView(membersView, futures)) {
                                    // there are some new addresses added to the futures map. lets wait for their results.
                                    done = false;
                                }
                            }
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        } catch (ExecutionException ignored) {
                            // we couldn't fetch MembersView of 'address'. It will be removed from the cluster.
                        }
                    } else if (mostRecentMembersView.containsAddress(address)) {
                        done = false;
                    }
                }
            }

            if (done) {
                break;
            } else {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        return mostRecentMembersView;
    }

    private boolean checkFetchedMembersView(MembersView membersView, Map<Address, Future<MembersView>> futures) {
        boolean done = false;

        for (MemberInfo memberInfo : membersView.getMembers()) {
            Address memberAddress = memberInfo.getAddress();
            if (!(node.getThisAddress().equals(memberAddress) || isMemberSuspected(memberAddress) || futures.containsKey(memberAddress))) {
                // this is a new member for us. lets ask its members view
                logger.fine("Asking MembersView of " + memberAddress);
                futures.put(memberAddress, invokeFetchMemberListStateOperation(memberAddress));
                done = true;
            }
        }

        return done;
    }

    private Future<MembersView> invokeFetchMemberListStateOperation(Address target) {
        // TODO [basri] define config param
        long fetchMemberListStateTimeoutMs = TimeUnit.SECONDS.toMillis(30);
        Operation op = new FetchMemberListStateOperation().setCallerUuid(node.getThisUuid());

        return nodeEngine.getOperationService()
                .createInvocationBuilder(SERVICE_NAME, op, target)
                .setTryCount(Integer.MAX_VALUE)
                .setCallTimeout(fetchMemberListStateTimeoutMs).invoke();
    }

    private boolean ensureMemberIsRemovable(Address deadAddress) {
        return node.joined() && !deadAddress.equals(node.getThisAddress());
    }

    boolean isMemberRemovedWhileClusterIsNotActive(Address target) {
        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        return membersRemovedInNotActiveState.contains(target);
    }

    boolean isMemberRemovedWhileClusterIsNotActive(String uuid) {
        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        return membersRemovedInNotActiveState.contains(uuid);
    }

    MemberImpl getMemberRemovedWhileClusterIsNotActive(String uuid) {
        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        return membersRemovedInNotActiveState.getMember(uuid);
    }

    Collection<Member> getCurrentMembersAndMembersRemovedWhileClusterIsNotActive() {
        clusterServiceLock.lock();
        try {
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            if (membersRemovedInNotActiveState.size() == 0) {
                return getMemberSet();
            }

            Collection<MemberImpl> removedMembers = membersRemovedInNotActiveState.getMembers();
            Collection<MemberImpl> members = memberMapRef.get().getMembers();

            Collection<Member> allMembers = new ArrayList<Member>(members.size() + removedMembers.size());
            allMembers.addAll(members);
            allMembers.addAll(removedMembers);

            return allMembers;
        } finally {
            clusterServiceLock.unlock();
        }
    }


    void addMembersRemovedInNotActiveState(Collection<MemberImpl> members) {
        clusterServiceLock.lock();
        try {
            members.remove(node.getLocalMember());
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            membersRemovedInNotActiveStateRef.set(MemberMap.cloneAdding(membersRemovedInNotActiveState,
                    members.toArray(new MemberImpl[0])));
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void shrinkMembersRemovedWhileClusterIsNotActiveState(Collection<String> memberUuidsToRemove) {
        clusterServiceLock.lock();
        try {
            Set<MemberImpl> membersRemovedInNotActiveState
                    = new LinkedHashSet<MemberImpl>(membersRemovedInNotActiveStateRef.get().getMembers());

            Iterator<MemberImpl> it = membersRemovedInNotActiveState.iterator();
            while (it.hasNext()) {
                MemberImpl member = it.next();
                if (memberUuidsToRemove.contains(member.getUuid())) {
                    logger.fine("Removing " + member + " from members removed while in cluster not active state");
                    it.remove();
                }
            }
            membersRemovedInNotActiveStateRef.set(MemberMap.createNew(membersRemovedInNotActiveState.toArray(new MemberImpl[0])));
        } finally {
            clusterServiceLock.unlock();
        }
    }

    void removeMembersDeadWhileClusterIsNotActive() {
        clusterServiceLock.lock();
        try {
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            Collection<MemberImpl> members = membersRemovedInNotActiveState.getMembers();
            membersRemovedInNotActiveStateRef.set(MemberMap.empty());
            for (MemberImpl member : members) {
                onMemberRemove(member);
            }

        } finally {
            clusterServiceLock.unlock();
        }
    }

    void reset() {
        clusterServiceLock.lock();
        try {
            memberMapRef.set(MemberMap.singleton(node.getLocalMember()));
            membersRemovedInNotActiveStateRef.set(MemberMap.empty());
            suspectedMembers.clear();
        } finally {
            clusterServiceLock.unlock();
        }
    }

    private class FetchMostRecentMemberListTask implements Runnable {

        final MembersView localMemberView;
        final Set<Address> membersToAsk;

        FetchMostRecentMemberListTask(MembersView localMemberView, Set<Address> membersToAsk) {
            this.localMemberView = localMemberView;
            this.membersToAsk = membersToAsk;
        }

        @Override
        public void run() {
            MembersView newMembersView = decideNewMembersView(localMemberView, membersToAsk);
            clusterServiceLock.lock();
            try {
                updateMembers(newMembersView);
                sendMemberListToOthers();
                // TODO [basri] what about membersRemovedWhileClusterNotActive ???
                clusterService.getClusterJoinManager().reset();
                logger.info("Mastership is claimed with: " + newMembersView);
            } finally {
                clusterServiceLock.unlock();
            }
        }
    }
}

