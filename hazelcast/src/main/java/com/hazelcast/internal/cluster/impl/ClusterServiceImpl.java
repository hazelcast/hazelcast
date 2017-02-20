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
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.operations.FetchMemberListStateOperation;
import com.hazelcast.internal.cluster.impl.operations.MemberRemoveOperation;
import com.hazelcast.internal.cluster.impl.operations.MembersUpdateOperation;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOperation;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOperation;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.version.Version;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class ClusterServiceImpl implements ClusterService, ConnectionListener, ManagedService,
        EventPublishingService<MembershipEvent, MembershipListener>, TransactionalService {

    public static final String SERVICE_NAME = "hz:core:clusterService";

    static final String EXECUTOR_NAME = "hz:cluster";

    private static final int DEFAULT_MERGE_RUN_DELAY_MILLIS = 100;
    private static final int CLUSTER_EXECUTOR_QUEUE_CAPACITY = 1000;
    private static final long CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS = 1000;
    private static final boolean ASSERTION_ENABLED = ClusterServiceImpl.class.desiredAssertionStatus();

    private static final String MEMBERSHIP_EVENT_EXECUTOR_NAME = "hz:cluster:event";

    private final Address thisAddress;

    private final Lock lock = new ReentrantLock();

    private final AtomicReference<MemberMap> memberMapRef = new AtomicReference<MemberMap>(MemberMap.empty());

    private final AtomicReference<MemberMap> membersRemovedInNotActiveStateRef
            = new AtomicReference<MemberMap>(MemberMap.empty());

    private final ConcurrentMap<Address, Long> suspectedMembers = new ConcurrentHashMap<Address, Long>();

    private final Node node;

    private final NodeEngineImpl nodeEngine;

    private final ILogger logger;

    private final ClusterClockImpl clusterClock;

    private final ClusterStateManager clusterStateManager;

    private final ClusterJoinManager clusterJoinManager;

    private final ClusterHeartbeatManager clusterHeartbeatManager;

    private volatile String clusterId;

    public ClusterServiceImpl(Node node) {

        this.node = node;
        nodeEngine = node.nodeEngine;

        logger = node.getLogger(ClusterService.class.getName());
        clusterClock = new ClusterClockImpl(logger);

        thisAddress = node.getThisAddress();

        clusterStateManager = new ClusterStateManager(node, lock);
        clusterJoinManager = new ClusterJoinManager(node, this, lock);
        clusterHeartbeatManager = new ClusterHeartbeatManager(node, this);

        registerThisMember();

        node.connectionManager.addConnectionListener(this);
        //MEMBERSHIP_EVENT_EXECUTOR is a single threaded executor to ensure that events are executed in correct order.
        nodeEngine.getExecutionService().register(MEMBERSHIP_EVENT_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
        registerMetrics();
    }

    private void registerThisMember() {
        MemberImpl thisMember = node.getLocalMember();
        setMembers(0, thisMember);
    }

    private void registerMetrics() {
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        metricsRegistry.scanAndRegister(clusterClock, "cluster.clock");
        metricsRegistry.scanAndRegister(clusterHeartbeatManager, "cluster.heartbeat");
        metricsRegistry.scanAndRegister(this, "cluster");
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        long mergeFirstRunDelayMs = node.getProperties().getMillis(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS);
        mergeFirstRunDelayMs = (mergeFirstRunDelayMs > 0 ? mergeFirstRunDelayMs : DEFAULT_MERGE_RUN_DELAY_MILLIS);

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(EXECUTOR_NAME, 2, CLUSTER_EXECUTOR_QUEUE_CAPACITY, ExecutorType.CACHED);

        long mergeNextRunDelayMs = node.getProperties().getMillis(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS);
        mergeNextRunDelayMs = (mergeNextRunDelayMs > 0 ? mergeNextRunDelayMs : DEFAULT_MERGE_RUN_DELAY_MILLIS);
        executionService.scheduleWithRepetition(EXECUTOR_NAME, new SplitBrainHandler(node), mergeFirstRunDelayMs,
                mergeNextRunDelayMs, TimeUnit.MILLISECONDS);

        clusterHeartbeatManager.init();
    }

    public void sendLocalMembershipEvent() {
        sendMembershipEvents(Collections.<MemberImpl>emptySet(), Collections.singleton(node.getLocalMember()));
    }

    public void sendMemberListToMember(Address target) {
        if (!isMaster()) {
            return;
        }
        if (thisAddress.equals(target)) {
            return;
        }

        MemberMap memberMap = memberMapRef.get();
        MemberImpl member = memberMap.getMember(target);
        String memberUuid = member != null ? member.getUuid() : null;

        MembersUpdateOperation op = new MembersUpdateOperation(memberUuid, memberMap.toMembersView(),
                                                                     clusterClock.getClusterTime(), null, false);
        nodeEngine.getOperationService().send(op, target);
    }

    public boolean isMemberSuspected(Address address) {
        return suspectedMembers.containsKey(address);
    }

    public void removeAddress(Address deadAddress, String uuid, String reason) {
        lock.lock();
        try {
            MemberImpl member = getMember(deadAddress);
            if (member == null || !uuid.equals(member.getUuid())) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot remove " + deadAddress + ", either member is not present "
                            + "or uuid is not matching. Uuid: " + uuid + ", member: " + member);
                }
                return;
            }
            doRemoveAddress(deadAddress, reason, true);
        } finally {
            lock.unlock();
        }
    }

    // TODO [basri] implement this
    public void suspectAddress(Address suspectedAddress, String reason) {
        if (!ensureMemberIsRemovable(suspectedAddress)) {
            return;
        }

        MembersView localMemberView = null;
        Set<Address> membersToAsk = new HashSet<Address>();
        lock.lock();
        try {
            if (node.isMaster() && !clusterJoinManager.isMastershipClaimInProgress()) {
                removeAddress(suspectedAddress, reason);
            } else {
                if (suspectedMembers.containsKey(suspectedAddress)) {
                    return;
                }

                suspectedMembers.put(suspectedAddress, 0L);
                if (reason != null) {
                    logger.warning(suspectedAddress + " is suspected to be dead for reason: " + reason);
                } else {
                    logger.warning(suspectedAddress + " is suspected to be dead");
                }

                if (clusterJoinManager.isMastershipClaimInProgress()) {
                    return;
                }

                MemberMap memberMap = memberMapRef.get();
                if (!shouldClaimMastership(memberMap)) {
                    return;
                }

                logger.info("Claiming mastership...");

                // TODO [basri] should be here or after the master address is updated?
                // TODO [basri] We need to make sure that all pending join requests are cancelled temporarily.
                clusterJoinManager.setMastershipClaimInProgress();

                // TODO [basri] update master address
                node.setMasterAddress(node.getThisAddress());

                // TODO [basri] fix this
                localMemberView = memberMap.toMembersView();
                for (MemberImpl member : memberMap.getMembers()) {
                    if (member.localMember() || suspectedMembers.containsKey(member.getAddress())) {
                        continue;
                    }

                    membersToAsk.add(member.getAddress());
                }
            }
        } finally {
            lock.unlock();
        }

        MembersView newMembersView = decideNewMembersView(localMemberView, membersToAsk);
        lock.lock();
        try {
            memberMapRef.set(newMembersView.toMemberMap());
            // TODO [basri] publish the new member list
            // TODO [basri] what about membersRemovedWhileClusterNotActive ???
            clusterJoinManager.reset();
            logger.info("Mastership is declared upon: " + newMembersView);
        } finally {
            lock.unlock();
        }
    }

    private boolean shouldClaimMastership(MemberMap memberMap) {
        if (node.isMaster()) {
            return false;
        }

        // TODO [basri] what if I am shutting down?

        for (MemberImpl m : memberMap.toMembersViewBeforeMember(node.getThisAddress())) {
            if (!isMemberSuspected(m.getAddress())) {
                return false;
            }
        }

        return true;
    }

    private MembersView decideNewMembersView(MembersView localMembersView, Set<Address> addresses) {
        Map<Address, Future<MembersView>> futures = new HashMap<Address, Future<MembersView>>();

        MembersView mostRecentMembersView = fetchMembersViews(localMembersView, addresses, futures);

        // within the most recent members view, select the members that have reported their members view successfully
        int finalVersion = mostRecentMembersView.getVersion() + 1;
        List<MemberInfo> finalMembers = new ArrayList<MemberInfo>();
        for (MemberInfo memberInfo : mostRecentMembersView.getMembers()) {
            Address address = memberInfo.getAddress();
            Future<MembersView> membersViewFuture = futures.get(address);
            // if a member is suspected during the mastership claim process, ignore its result
            // TODO [basri] could it be that `membersViewFuture == null` ?
            if (suspectedMembers.containsKey(address) || membersViewFuture == null || !membersViewFuture.isDone()) {
                continue;
            }

            try {
                membersViewFuture.get();
                finalMembers.add(memberInfo);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ignored) {
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

            for (Entry<Address, Future<MembersView>> e : new ArrayList<Entry<Address, Future<MembersView>>>(futures.entrySet())) {
                Address address = e.getKey();
                Future<MembersView> future = e.getValue();

                // If we started to suspect a member after asking its member list, we don't need to wait for its result.
                if (!suspectedMembers.containsKey(address)) {
                    // If there is no suspicion yet, we just keep waiting till we have a successful or failed result.
                   if (future.isDone()) {
                       try {
                           MembersView membersView = future.get();
                           if (membersView.getVersion() > mostRecentMembersView.getVersion()) {
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
                           // we couldn't fetch the members view of this member. It will be removed from the cluster.
                       }
                   } else {
                       done = false;
                   }
                }
            }

            if (done) {
                break;
            } else {
                try {
                    Thread.sleep(100);
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
            if (!(suspectedMembers.containsKey(memberAddress) || futures.containsKey(memberAddress))) {
                // this is a new member for us. lets ask its members view
                futures.put(memberAddress, invokeFetchMemberListStateOperation(memberAddress));
                done = true;
            }
        }

        return done;
    }

    private Future<MembersView> invokeFetchMemberListStateOperation(Address target) {
        // TODO [basri] define config param
        long fetchMemberListStateTimeoutMs = TimeUnit.SECONDS.toMillis(30);
        FetchMemberListStateOperation op = new FetchMemberListStateOperation(node.getThisUuid());
        Future<MembersView> future = nodeEngine.getOperationService()
                                               .createInvocationBuilder(SERVICE_NAME, op, target)
                                               .setTryCount(Integer.MAX_VALUE)
                                               .setCallTimeout(fetchMemberListStateTimeoutMs).invoke();

        return future;
    }

    public MembersView acceptMastershipClaim(Address targetAddress, String targetUuid) {
        // TODO [basri] check targetAddress is not me DONE
        // TODO [basri] check I am not master DONE
        // TODO [basri] check target address is not current master DONE
        // TODO [basri] target address is a valid member with its uuid DONE
        // TODO [basri] check that I suspect everyone before the target address DONE
        // TODO [basri] check that target address is not suspected DONE

        checkNotNull(targetAddress);
        checkNotNull(targetUuid);

        lock.lock();
        try {
            checkFalse(node.getThisAddress().equals(targetAddress), "cannot accept my own mastership claim!");
            checkFalse(node.isMaster(),
                    targetAddress + " claims mastership but this node is master!");
            checkFalse(targetAddress.equals(node.getMasterAddress()),
                    targetAddress + " claims mastership but it is already the known master!");
            MemberImpl newMaster = getMember(targetAddress);
            checkTrue(newMaster != null ,
                    targetAddress + " claims mastership but it is not a member!");
            checkTrue(newMaster.getUuid().equals(targetUuid),
                    targetAddress + " claims mastership but it has a different uuid: " + targetUuid
                            + " than its known uuid: " + newMaster.getUuid() );

            MemberMap memberMap = memberMapRef.get();
            if (!shouldAcceptMastership(memberMap, targetAddress)) {
                throw new RetryableHazelcastException();
            }

            logger.info("Mastership of " + targetAddress + " is accepted.");
            node.setMasterAddress(newMaster.getAddress());

            return memberMap.toMembersViewWithFirstMember(newMaster);
        } finally {
            lock.unlock();
        }
    }

    // mastership is accepted when all members before the target is suspected, and target is not suspected
    private boolean shouldAcceptMastership(MemberMap memberMap, Address target) {
        for (MemberImpl member : memberMap.toMembersViewBeforeMember(target)) {
            if (!isMemberSuspected(member.getAddress())) {
                return false;
            }
        }

        return !suspectedMembers.containsKey(target);
    }

    // TODO [basri] If this node is a slave, deadAddress can be only the master address. Is that so ????
    public void removeAddress(Address deadAddress, String reason) {
        doRemoveAddress(deadAddress, reason, true);
    }

    void doRemoveAddress(Address deadAddress, String reason, boolean destroyConnection) {
        if (!ensureMemberIsRemovable(deadAddress)) {
            return;
        }

        lock.lock();
        try {
            if (deadAddress.equals(node.getMasterAddress())) {
                assignNewMaster();
            }
            if (node.isMaster()) {
                clusterJoinManager.removeJoin(deadAddress);
            }
            Connection conn = node.connectionManager.getConnection(deadAddress);
            if (destroyConnection && conn != null) {
                conn.close(reason, null);
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

    private boolean ensureMemberIsRemovable(Address deadAddress) {
        return node.joined() && !deadAddress.equals(thisAddress);
    }

    private void assignNewMaster() {
        Address oldMasterAddress = node.getMasterAddress();
        if (node.joined()) {
            Collection<Member> members = getMembers();
            Member newMaster = null;
            int size = members.size();
            if (size > 1) {
                Iterator<Member> iterator = members.iterator();
                Member member = iterator.next();
                if (member.getAddress().equals(oldMasterAddress)) {
                    newMaster = iterator.next();
                } else {
                    logger.severe(format("Old master %s is dead, but the first of member list is a different member %s!",
                            oldMasterAddress, member));
                    newMaster = member;
                }
            } else {
                logger.warning(format("Old master %s is dead and this node is not master, "
                        + "but member list contains only %d members: %s", oldMasterAddress, size, members));
            }
            logger.info(format("Old master %s left the cluster, assigning new master %s", oldMasterAddress, newMaster));
            if (newMaster != null) {
                node.setMasterAddress(newMaster.getAddress());
            } else {
                node.setMasterAddress(null);
            }
        } else {
            node.setMasterAddress(null);
        }

        if (logger.isFineEnabled()) {
            logger.fine(format("Old master: %s, new master: %s ", oldMasterAddress, node.getMasterAddress()));
        }

        if (node.isMaster()) {
            clusterHeartbeatManager.resetMemberMasterConfirmations();
        } else {
            clusterHeartbeatManager.sendMasterConfirmation();
        }
    }

    public void merge(Address newTargetAddress) {
        node.getJoiner().setTargetAddress(newTargetAddress);
        LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
        lifecycleService.runUnderLifecycleLock(new ClusterMergeTask(node));
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            memberMapRef.set(MemberMap.singleton(node.getLocalMember()));
            clusterHeartbeatManager.reset();
            clusterStateManager.reset();
            clusterJoinManager.reset();
            membersRemovedInNotActiveStateRef.set(MemberMap.empty());
            resetClusterId();
        } finally {
            lock.unlock();
        }
    }

    MemberMap getMemberMap() {
        return memberMapRef.get();
    }

    public int getMemberListVersion() {
        return memberMapRef.get().getVersion();
    }

//    static List<MemberInfo> createMemberInfoList(Collection<MemberImpl> members) {
//        List<MemberInfo> memberInfos = new LinkedList<MemberInfo>();
//        for (MemberImpl member : members) {
//            memberInfos.add(new MemberInfo(member));
//        }
//        return memberInfos;
//    }
//

    public boolean finalizeJoin(MembersView membersView, Address callerAddress, String clusterId,
                                ClusterState clusterState, Version clusterVersion,
                                long clusterStartTime, long masterTime) {
        lock.lock();
        try {
            if (!checkValidMaster(callerAddress)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Not finalizing join because caller: " + callerAddress + " is not known master: "
                            + getMasterAddress());
                }
                return false;
            }

            if (node.joined()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Node is already joined... No need to finalize join...");
                }

                return false;
            }

            initialClusterState(clusterState, clusterVersion);
            setClusterId(clusterId);
            ClusterClockImpl clusterClock = getClusterClock();
            clusterClock.setClusterStartTime(clusterStartTime);
            clusterClock.setMasterTime(masterTime);
            doUpdateMembers(membersView);
            clusterHeartbeatManager.heartbeat();

            node.setJoined();

            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean updateMembers(MembersView membersView, Address callerAddress) {
        lock.lock();
        try {
            if (!checkValidMaster(callerAddress)) {
                logger.warning("Not updating members because caller: " + callerAddress  + " is not known master: "
                        + getMasterAddress());
                return false;
            }

            if (!node.joined()) {
                logger.warning("Not updating members received from caller: " + callerAddress +  " because node is not joined! ");
                return false;
            }

            if (!shouldProcessMemberUpdate(membersView)) {
                return false;
            }

            doUpdateMembers(membersView);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private boolean checkValidMaster(Address callerAddress) {
        return  (callerAddress != null && callerAddress.equals(getMasterAddress()));
    }

    // handles both new and left members
    private void doUpdateMembers(MembersView membersView) {
        MemberMap currentMemberMap = memberMapRef.get();

        String scopeId = thisAddress.getScopeId();
        Collection<MemberImpl> addedMembers = new LinkedList<MemberImpl>();
        Collection<MemberImpl> removedMembers = new LinkedList<MemberImpl>();

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

            member = createMember(memberInfo, scopeId);
            addedMembers.add(member);
            long now = clusterClock.getClusterTime();
            clusterHeartbeatManager.onHeartbeat(member, now);
            clusterHeartbeatManager.acceptMasterConfirmation(member, now);

            repairPartitionTableIfReturningMember(member);
            members[memberIndex++] = member;
        }

        MemberMap newMemberMap = membersView.toMemberMap();
        for (MemberImpl member : currentMemberMap.getMembers()) {
            if (!newMemberMap.contains(member.getAddress())) {
                removedMembers.add(member);
            }
        }

        setMembers(membersView.getVersion(), members);

        // TODO: handle removed members
        for (MemberImpl member : removedMembers) {
            handleMemberRemove(memberMapRef.get(), member);
        }

        sendMembershipEvents(currentMemberMap.getMembers(), addedMembers);

        MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
        membersRemovedInNotActiveStateRef.set(MemberMap.cloneExcluding(membersRemovedInNotActiveState, members));

        clusterHeartbeatManager.heartbeat();
        logger.info(membersString());
    }

    private void repairPartitionTableIfReturningMember(MemberImpl member) {
        if (!isMaster()) {
            return;
        }

        if (getClusterState() == ClusterState.ACTIVE) {
            return;
        }

        if (!node.getNodeExtension().isStartCompleted()) {
            return;
        }

        Address address = member.getAddress();
        MemberImpl memberRemovedWhileClusterIsNotActive = getMemberRemovedWhileClusterIsNotActive(member.getUuid());
        if (memberRemovedWhileClusterIsNotActive != null) {
            Address oldAddress = memberRemovedWhileClusterIsNotActive.getAddress();
            if (!oldAddress.equals(address)) {
                assert !isMemberRemovedWhileClusterIsNotActive(address);

                logger.warning(member + " is returning with a new address. Old one was: " + oldAddress
                        + ". Will update partition table with the new address.");
                InternalPartitionServiceImpl partitionService = node.partitionService;
                partitionService.replaceAddress(oldAddress, address);
            }
        }
    }

    private boolean shouldProcessMemberUpdate(MembersView membersView) {
        if (getClusterVersion().isLessThan(Versions.V3_9)) {
            return shouldProcessMemberUpdate(getMemberMap(), membersView.getMembers());
        }

        int memberListVersion = getMemberListVersion();

        if (memberListVersion > membersView.getVersion()) {
            logger.fine("Received an older member update, ignoring... Current version: "
                    + memberListVersion + ", Received version: " + membersView.getVersion());
            return false;
            
        }

        if (memberListVersion == membersView.getVersion()) {
            if (ASSERTION_ENABLED) {
                MemberMap memberMap = getMemberMap();
                Collection<Address> currentAddresses = memberMap.getAddresses();
                Collection<Address> newAddresses = membersView.getAddresses();

                assert currentAddresses.size() == newAddresses.size()
                        && newAddresses.containsAll(currentAddresses)
                        : "Member view versions are same but new member view doesn't match the current!"
                        + " Current: " + memberMap.toMembersView() + ", New: " + membersView;
            }

            logger.fine("Received a periodic member update, ignoring... Version: " + memberListVersion);
            return false;
        }

        return true;
    }

    /**
     * @deprecated in 3.9
     */
    @Deprecated
    private boolean shouldProcessMemberUpdate(MemberMap currentMembers,
                                              Collection<MemberInfo> newMemberInfos) {
        int currentMembersSize = currentMembers.size();
        int newMembersSize = newMemberInfos.size();

        if (currentMembersSize > newMembersSize) {
            logger.warning("Received an older member update, no need to process...");
            nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            return false;
        }

        // member-update process only accepts new member updates
        if (currentMembersSize == newMembersSize) {
            Set<MemberInfo> currentMemberInfos = createMemberInfoSet(currentMembers.getMembers());
            if (currentMemberInfos.containsAll(newMemberInfos)) {
                logger.fine("Received a periodic member update, no need to process...");
            } else {
                logger.warning("Received an inconsistent member update "
                        + "which contains new members and removes some of the current members! "
                        + "Ignoring and requesting a new member update...");
                nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            }
            return false;
        }

        Set<MemberInfo> currentMemberInfos = createMemberInfoSet(currentMembers.getMembers());
        currentMemberInfos.removeAll(newMemberInfos);
        if (currentMemberInfos.isEmpty()) {
            return true;
        } else {
            logger.warning("Received an inconsistent member update."
                    + " It has more members but also removes some of the current members!"
                    + " Ignoring and requesting a new member update...");
            nodeEngine.getOperationService().send(new TriggerMemberListPublishOperation(), getMasterAddress());
            return false;
        }
    }

    private static Set<MemberInfo> createMemberInfoSet(Collection<MemberImpl> members) {
        Set<MemberInfo> memberInfos = new HashSet<MemberInfo>();
        for (MemberImpl member : members) {
            memberInfos.add(new MemberInfo(member));
        }
        return memberInfos;
    }


    private void sendMembershipEvents(Collection<MemberImpl> currentMembers, Collection<MemberImpl> newMembers) {
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

    public void updateMemberAttribute(String uuid, MemberAttributeOperationType operationType, String key, Object value) {
        lock.lock();
        try {
            MemberMap memberMap = memberMapRef.get();
            for (MemberImpl member : memberMap.getMembers()) {
                if (member.getUuid().equals(uuid)) {
                    if (!member.equals(getLocalMember())) {
                        member.updateAttribute(operationType, key, value);
                    }
                    sendMemberAttributeEvent(member, operationType, key, value);
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAdded(Connection connection) {
    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (logger.isFineEnabled()) {
            logger.fine("Removed connection to " + connection.getEndPoint());
        }
        if (!node.joined()) {
            Address masterAddress = node.getMasterAddress();
            if (masterAddress != null && masterAddress.equals(connection.getEndPoint())) {
                clusterJoinManager.setMasterAddress(null);
            }
        }
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    private void setMembers(int version, MemberImpl... members) {
        if (members == null || members.length == 0) {
            return;
        }
        if (logger.isFineEnabled()) {
            logger.fine("Setting members " + Arrays.toString(members) + ", version: " + version);
        }
        lock.lock();
        try {
            memberMapRef.set(MemberMap.createNew(version, members));
        } finally {
            lock.unlock();
        }
    }

    // TODO: not used by 3.9+
    private void removeMember(MemberImpl deadMember) {
        logger.info("Removing " + deadMember);
        lock.lock();
        try {
            MemberMap currentMembers = memberMapRef.get();
            if (currentMembers.contains(deadMember.getAddress())) {
                clusterHeartbeatManager.removeMember(deadMember);
                MemberMap newMembers = MemberMap.cloneExcluding(currentMembers, deadMember);
                memberMapRef.set(newMembers);

                if (node.isMaster()) {
                    if (logger.isFineEnabled()) {
                        logger.fine(deadMember + " is dead, sending remove to all other members...");
                    }
                    // TODO [basri] I will publish a new member list
                    sendMemberRemoveOperation(getMemberListVersion(), deadMember);
                }

                handleMemberRemove(newMembers, deadMember);
            }
        } finally {
            lock.unlock();
        }
    }

    private void handleMemberRemove(MemberMap newMembers, MemberImpl removedMember) {
        ClusterState clusterState = clusterStateManager.getState();
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

    private void onMemberRemove(MemberImpl deadMember) {
        // sync call
        node.getPartitionService().memberRemoved(deadMember);
        // sync call
        nodeEngine.onMemberLeft(deadMember);
    }

    public boolean isMemberRemovedWhileClusterIsNotActive(Address target) {
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

    public Collection<Member> getCurrentMembersAndMembersRemovedWhileClusterIsNotActive() {
        lock.lock();
        try {
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            if (membersRemovedInNotActiveState.size() == 0) {
                return getMembers();
            }

            Collection<MemberImpl> removedMembers = membersRemovedInNotActiveState.getMembers();
            Collection<MemberImpl> members = memberMapRef.get().getMembers();

            Collection<Member> allMembers = new ArrayList<Member>(members.size() + removedMembers.size());
            allMembers.addAll(members);
            allMembers.addAll(removedMembers);

            return allMembers;
        } finally {
            lock.unlock();
        }
    }

    void removeMembersDeadWhileClusterIsNotActive() {
        lock.lock();
        try {
            MemberMap memberMap = memberMapRef.get();
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            Collection<MemberImpl> members = membersRemovedInNotActiveState.getMembers();
            membersRemovedInNotActiveStateRef.set(MemberMap.empty());
            for (MemberImpl member : members) {
                onMemberRemove(member);
            }

        } finally {
            lock.unlock();
        }
    }

    public void notifyForRemovedMember(MemberImpl member) {
        lock.lock();
        try {
            onMemberRemove(member);
        } finally {
            lock.unlock();
        }
    }

    public void shrinkMembersRemovedWhileClusterIsNotActiveState(Collection<String> memberUuidsToRemove) {
        lock.lock();
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
            lock.unlock();
        }
    }

    private void sendMemberRemoveOperation(int memberListVersion, Member deadMember) {
        for (Member member : getMembers()) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address) && !address.equals(deadMember.getAddress())) {
                MemberRemoveOperation op = new MemberRemoveOperation(memberListVersion, deadMember.getAddress(), deadMember.getUuid());
                nodeEngine.getOperationService().send(op, address);
            }
        }
    }

    // TODO [basri] think about this part. I am explicitly saying that I am leaving the cluster.
    public void sendShutdownMessage() {
        sendMemberRemoveOperation(getMemberListVersion(), getLocalMember());
    }

    private void sendMembershipEventNotifications(MemberImpl member, Set<Member> members, final boolean added) {
        int eventType = added ? MembershipEvent.MEMBER_ADDED : MembershipEvent.MEMBER_REMOVED;
        MembershipEvent membershipEvent = new MembershipEvent(this, member, eventType, members);
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

    private void sendMemberAttributeEvent(MemberImpl member, MemberAttributeOperationType operationType, String key,
                                          Object value) {
        final MemberAttributeServiceEvent event
                = new MemberAttributeServiceEvent(this, member, operationType, key, value);
        MemberAttributeEvent attributeEvent = new MemberAttributeEvent(this, member, operationType, key, value);
        Collection<MembershipAwareService> membershipAwareServices = nodeEngine.getServices(MembershipAwareService.class);
        if (membershipAwareServices != null && !membershipAwareServices.isEmpty()) {
            for (final MembershipAwareService service : membershipAwareServices) {
                // service events should not block each other
                nodeEngine.getExecutionService().execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                    public void run() {
                        service.memberAttributeChanged(event);
                    }
                });
            }
        }
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        for (EventRegistration reg : registrations) {
            eventService.publishEvent(SERVICE_NAME, reg, attributeEvent, reg.getId().hashCode());
        }
    }

    private MemberImpl createMember(MemberInfo memberInfo, String ipV6ScopeId) {
        Address address = memberInfo.getAddress();
        address.setScopeId(ipV6ScopeId);
        return new MemberImpl(address, memberInfo.getVersion(), thisAddress.equals(address), memberInfo.getUuid(),
                (HazelcastInstanceImpl) nodeEngine.getHazelcastInstance(), memberInfo.getAttributes(), memberInfo.isLiteMember());
    }

    @Override
    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(address);
    }

    @Override
    public MemberImpl getMember(String uuid) {
        if (uuid == null) {
            return null;
        }

        MemberMap memberMap = memberMapRef.get();
        return memberMap.getMember(uuid);
    }

    @Override
    public Collection<MemberImpl> getMemberImpls() {
        return memberMapRef.get().getMembers();
    }

    public Collection<Address> getMemberAddresses() {
        return memberMapRef.get().getAddresses();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Member> getMembers() {
        return (Set) memberMapRef.get().getMembers();
    }

    @Override
    public Collection<Member> getMembers(MemberSelector selector) {
        return (Collection) new MemberSelectingCollection(memberMapRef.get().getMembers(), selector);
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    @Override
    public boolean isMaster() {
        return node.isMaster();
    }

    @Override
    public Address getThisAddress() {
        return thisAddress;
    }

    public Member getLocalMember() {
        return node.getLocalMember();
    }

    @Probe
    @Override
    public int getSize() {
        return getMembers().size();
    }

    @Override
    public int getSize(MemberSelector selector) {
        int size = 0;
        for (MemberImpl member : memberMapRef.get().getMembers()) {
            if (selector.select(member)) {
                size++;
            }
        }

        return size;
    }

    @Override
    public ClusterClockImpl getClusterClock() {
        return clusterClock;
    }

    @Override
    public long getClusterTime() {
        return clusterClock.getClusterTime();
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    // called under cluster service lock
    void setClusterId(String newClusterId) {
        assert clusterId == null : "Cluster id should be null: " + clusterId;
        clusterId = newClusterId;
    }

    // called under cluster service lock
    private void resetClusterId() {
        clusterId = null;
    }

    public String addMembershipListener(MembershipListener listener) {
        checkNotNull(listener, "listener cannot be null");

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration;
        if (listener instanceof InitialMembershipListener) {
            lock.lock();
            try {
                ((InitialMembershipListener) listener).init(new InitialMembershipEvent(this, getMembers()));
                registration = eventService.registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
            } finally {
                lock.unlock();
            }
        } else {
            registration = eventService.registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
        }

        return registration.getId();
    }

    public boolean removeMembershipListener(String registrationId) {
        checkNotNull(registrationId, "registrationId cannot be null");

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    @Override
    public void dispatchEvent(MembershipEvent event, MembershipListener listener) {
        switch (event.getEventType()) {
            case MembershipEvent.MEMBER_ADDED:
                listener.memberAdded(event);
                break;
            case MembershipEvent.MEMBER_REMOVED:
                listener.memberRemoved(event);
                break;
            case MembershipEvent.MEMBER_ATTRIBUTE_CHANGED:
                MemberAttributeEvent memberAttributeEvent = (MemberAttributeEvent) event;
                listener.memberAttributeChanged(memberAttributeEvent);
                break;
            default:
                throw new IllegalArgumentException("Unhandled event: " + event);
        }
    }

    public String membersString() {
        StringBuilder sb = new StringBuilder("\n\nMembers [");
        Collection<MemberImpl> members = getMemberImpls();
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
    public ClusterState getClusterState() {
        return clusterStateManager.getState();
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(String name, Transaction transaction) {
        throw new UnsupportedOperationException(SERVICE_NAME + " does not support TransactionalObjects!");
    }

    @Override
    public void rollbackTransaction(String transactionId) {
        logger.info("Rolling back cluster state. Transaction: " + transactionId);
        clusterStateManager.rollbackClusterState(transactionId);
    }

    @Override
    public void changeClusterState(ClusterState newState) {
        changeClusterState(newState, false);
    }

    private void changeClusterState(ClusterState newState, boolean isTransient) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(newState), getMembers(), partitionStateVersion,
                isTransient);
    }

    @Override
    public void changeClusterState(ClusterState newState, TransactionOptions options) {
        changeClusterState(newState, options, false);
    }

    private void changeClusterState(ClusterState newState, TransactionOptions options, boolean isTransient) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(newState), getMembers(), options, partitionStateVersion,
                isTransient);
    }

    @Override
    public Version getClusterVersion() {
        return clusterStateManager.getClusterVersion();
    }

    @Override
    public HotRestartService getHotRestartService() {
        return node.getNodeExtension().getHotRestartService();
    }

    @Override
    public void changeClusterVersion(Version version) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(version), getMembers(), partitionStateVersion, false);
    }

    @Override
    public void changeClusterVersion(Version version, TransactionOptions options) {
        int partitionStateVersion = node.getPartitionService().getPartitionStateVersion();
        clusterStateManager.changeClusterState(ClusterStateChange.from(version), getMembers(), options, partitionStateVersion,
                false);
    }

    void addMembersRemovedInNotActiveState(Collection<MemberImpl> members) {
        lock.lock();
        try {
            members.remove(node.getLocalMember());
            MemberMap membersRemovedInNotActiveState = membersRemovedInNotActiveStateRef.get();
            membersRemovedInNotActiveStateRef.set(MemberMap.cloneAdding(membersRemovedInNotActiveState,
                    members.toArray(new MemberImpl[0])));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        changeClusterState(ClusterState.PASSIVE, true);
        shutdownNodes();
    }

    @Override
    public void shutdown(TransactionOptions options) {
        changeClusterState(ClusterState.PASSIVE, options, true);
        shutdownNodes();
    }

    private void shutdownNodes() {
        final Operation op = new ShutdownNodeOperation();

        logger.info("Sending shutting down operations to all members...");

        Collection<Member> members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        final long timeout = node.getProperties().getNanos(GroupProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS);
        final long startTime = System.nanoTime();

        while ((System.nanoTime() - startTime) < timeout && !members.isEmpty()) {
            for (Member member : members) {
                nodeEngine.getOperationService().send(op, member.getAddress());
            }

            try {
                Thread.sleep(CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Shutdown sleep interrupted. ", e);
                break;
            }

            members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        }

        logger.info("Number of other nodes remaining: " + getSize(NON_LOCAL_MEMBER_SELECTOR) + ". Shutting down itself.");

        final HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
        hazelcastInstance.getLifecycleService().shutdown();
    }

    void initialClusterState(ClusterState clusterState, Version version) {
        if (node.joined()) {
            throw new IllegalStateException("Cannot set initial state after node joined! -> " + clusterState);
        }
        clusterStateManager.initialClusterState(clusterState, version);
    }

    public ClusterStateManager getClusterStateManager() {
        return clusterStateManager;
    }

    public ClusterJoinManager getClusterJoinManager() {
        return clusterJoinManager;
    }

    public ClusterHeartbeatManager getClusterHeartbeatManager() {
        return clusterHeartbeatManager;
    }

    @Override
    public String toString() {
        return "ClusterService"
                + "{address=" + thisAddress
                + '}';
    }
}
