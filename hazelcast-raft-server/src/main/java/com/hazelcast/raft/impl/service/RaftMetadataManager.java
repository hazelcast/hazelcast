package com.hazelcast.raft.impl.service;

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.config.raft.RaftMetadataGroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.internal.cluster.impl.operations.MemberAttributeChangedOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.MetadataRaftGroupNotInitializedException;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.RaftGroup.RaftGroupStatus;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.exception.CannotRemoveMemberException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateMetadataRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftNodeOp;
import com.hazelcast.raft.impl.service.operation.metadata.DestroyRaftNodesOp;
import com.hazelcast.raft.impl.service.operation.metadata.SendActiveRaftMembersOp;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.config.raft.RaftMetadataGroupConfig.RAFT_MEMBER_ATTRIBUTE_NAME;
import static com.hazelcast.raft.impl.service.MembershipChangeContext.RaftGroupMembershipChangeContext;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.singleton;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 */
public class RaftMetadataManager implements SnapshotAwareService<MetadataSnapshot>  {

    public static final RaftGroupId METADATA_GROUP_ID = new RaftGroupIdImpl("METADATA", 0);
    private static final RaftMemberSelector RAFT_MEMBER_SELECTOR = new RaftMemberSelector();

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftMetadataGroupConfig config;

    private final AtomicReference<RaftMemberImpl> localMember = new AtomicReference<RaftMemberImpl>();
    // groups are read outside of Raft
    private final ConcurrentMap<RaftGroupId, RaftGroupInfo> groups = new ConcurrentHashMap<RaftGroupId, RaftGroupInfo>();
    // activeMembers must be an ordered non-null collection
    private volatile Collection<RaftMemberImpl> activeMembers = Collections.emptySet();
    private MembershipChangeContext membershipChangeContext;

    RaftMetadataManager(NodeEngine nodeEngine, RaftService raftService, RaftMetadataGroupConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
    }

    void initIfInitialRaftMember() {
        boolean initialRaftMember = config != null && config.isInitialRaftMember();
        if (!initialRaftMember) {
            logger.warning("I am not a Raft member :(");
            return;
        }

        init();

        // task for initial Raft members
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(new DiscoverInitialRaftMembersTask(), 500, TimeUnit.MILLISECONDS);
    }

    void init() {
        Member localMember = nodeEngine.getLocalMember();
        if (!this.localMember.compareAndSet(null, new RaftMemberImpl(localMember))) {
            // already initialized
            return;
        }

        logger.info("Raft members: " + activeMembers + ", local: " + this.localMember);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new BroadcastActiveMembersTask(), 10, 10, TimeUnit.SECONDS);

        RaftCleanupHandler cleanupHandler = new RaftCleanupHandler(nodeEngine, raftService);
        cleanupHandler.init();
    }

    void reset() {
        activeMembers = Collections.emptySet();
        groups.clear();

        if (config == null) {
            return;
        }

        init();

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(new DiscoverInitialRaftMembersTask(), 500, TimeUnit.MILLISECONDS);
    }

    @Override
    public MetadataSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        if (!METADATA_GROUP_ID.equals(groupId)) {
            return null;
        }

        logger.info("Taking snapshot for commit-index: " + commitIndex);
        MetadataSnapshot snapshot = new MetadataSnapshot();
        for (RaftGroupInfo group : groups.values()) {
            assert group.commitIndex() <= commitIndex
                    : "Group commit index: " + group.commitIndex() + ", snapshot commit index: " + commitIndex;
            snapshot.addRaftGroup(group);
        }
        for (RaftMemberImpl member : activeMembers) {
            snapshot.addMember(member);
        }
        snapshot.setMembershipChangeContext(membershipChangeContext);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, MetadataSnapshot snapshot) {
        ensureMetadataGroupId(groupId);
        checkNotNull(snapshot);

        logger.info("Restoring snapshot for commit-index: " + commitIndex);
        for (RaftGroupInfo group : snapshot.getRaftGroups()) {
            RaftGroupInfo existingGroup = groups.get(group.id());

            if (group.status() == RaftGroupStatus.ACTIVE && existingGroup == null) {
                createRaftGroup(group);
                continue;
            }

            if (group.status() == RaftGroupStatus.DESTROYING) {
                if (existingGroup == null) {
                    createRaftGroup(group);
                } else {
                    existingGroup.setDestroying();
                }
                continue;
            }

            if (group.status() == RaftGroupStatus.DESTROYED) {
                if (existingGroup == null) {
                    addRaftGroup(group);
                } else {
                    completeDestroyRaftGroup(existingGroup);
                }
            }
        }

        activeMembers = unmodifiableCollection(new LinkedHashSet<RaftMemberImpl>(snapshot.getMembers()));
        membershipChangeContext = snapshot.getMembershipChangeContext();

        updateInvocationManagerMembers(getActiveMembers());
    }

    private static void ensureMetadataGroupId(RaftGroupId groupId) {
        checkTrue(METADATA_GROUP_ID.equals(groupId), "Invalid RaftGroupId! Expected: " + METADATA_GROUP_ID
                + ", Actual: " + groupId);
    }

    public RaftMemberImpl getLocalMember() {
        return localMember.get();
    }

    public RaftGroupInfo getRaftGroup(RaftGroupId groupId) {
        checkNotNull(groupId);

        return groups.get(groupId);
    }

    public Collection<RaftGroupId> getRaftGroupIds() {
        return groups.keySet();
    }

    public void createInitialMetadataRaftGroup(Collection<RaftMemberImpl> members) {
        checkNotNull(members);
        checkTrue(members.size() > 1, "initial metadata raft group must contain at least 2 members: " + members);

        RaftGroupInfo metadataGroup = new RaftGroupInfo(METADATA_GROUP_ID, members);
        RaftGroupInfo existingMetadataGroup = groups.putIfAbsent(METADATA_GROUP_ID, metadataGroup);
        if (existingMetadataGroup != null) {
            checkTrue(members.size() == existingMetadataGroup.memberCount(), "Cannot create metadata raft group with "
                    + members + " because it already exists with a different member list: " + existingMetadataGroup);

            for (RaftMemberImpl member : members) {
                checkTrue(existingMetadataGroup.containsMember(member), "Cannot create metadata raft group with " + members
                        + " because it already exists with a different member list: " + existingMetadataGroup);
            }

            return;
        }

        logger.fine("METADATA raft group is created: " + metadataGroup);
    }

    public RaftGroupId createRaftGroup(String groupName, Collection<RaftMemberImpl> members, long commitIndex) {
        checkFalse(METADATA_GROUP_ID.name().equals(groupName), groupName + " is reserved for internal usage!");
        failIfMetadataRaftGroupNotInitialized();

        // keep configuration on every metadata node
        RaftGroupInfo group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == members.size()) {
                logger.warning("Raft group " + groupName + " already exists. Ignoring add raft node request.");
                return group.id();
            }

            throw new IllegalStateException("Raft group " + groupName + " already exists with different group size.");
        }

        RaftMemberImpl leavingMember = membershipChangeContext != null ? membershipChangeContext.getLeavingMember() : null;
        for (RaftMemberImpl member : members) {
            if (member.equals(leavingMember) || !activeMembers.contains(member)) {
                throw new CannotCreateRaftGroupException("Cannot create raft group: " + groupName + " since " + member
                        + " is not active");
            }
        }

        return createRaftGroup(new RaftGroupInfo(new RaftGroupIdImpl(groupName, commitIndex), members));
    }

    private RaftGroupId createRaftGroup(RaftGroupInfo group) {
        addRaftGroup(group);
        logger.info("New raft group: " + group.id() + " is created with members: " + group.members());

        RaftGroupId groupId = group.id();
        if (group.containsMember(localMember.get())) {
            raftService.createRaftNode(groupId, group.members());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            RaftGroupInfo metadataGroup = groups.get(METADATA_GROUP_ID);
            for (RaftMemberImpl member : group.memberImpls()) {
                if (!metadataGroup.containsMember(member)) {
                    operationService.send(new CreateRaftNodeOp(group.id(), group.initialMembers()), member.getAddress());
                }
            }
        }

        return groupId;
    }

    private void addRaftGroup(RaftGroupInfo group) {
        RaftGroupId groupId = group.id();
        checkState(!groups.containsKey(groupId), group + " already exists!" );
        groups.put(groupId, group);
    }

    private RaftGroupInfo getRaftGroupByName(String name) {
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() != RaftGroupStatus.DESTROYED && group.name().equals(name)) {
                return group;
            }
        }
        return null;
    }

    public void triggerRebalanceRaftGroups() {
        failIfMetadataRaftGroupNotInitialized();

        if (membershipChangeContext != null) {
            checkState(membershipChangeContext.getLeavingMember() == null,
                    "Cannot rebalance raft groups because there is ongoing " + membershipChangeContext);
            return;
        }

        Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = getMemberMissingActiveRaftGroups();
        if (memberMissingGroups.size() > 0) {
            logger.info("Raft group rebalancing is triggered for " + memberMissingGroups);
            membershipChangeContext = new MembershipChangeContext(memberMissingGroups);
        }
    }

    public MembershipChangeContext triggerExpandRaftGroups(Map<RaftGroupId, RaftMemberImpl> membersToAdd) {
        checkNotNull(membersToAdd);
        checkState(membershipChangeContext != null, "There is no membership context to expand groups with members: " + membersToAdd);

        for (RaftMemberImpl member : membersToAdd.values()) {
            checkTrue(activeMembers.contains(member), membersToAdd + " is not in active members: " + activeMembers);
        }

        Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = membershipChangeContext.getMemberMissingGroups();
        List<RaftGroupMembershipChangeContext> changes = new ArrayList<RaftGroupMembershipChangeContext>();
        for (Entry<RaftGroupId, RaftMemberImpl> e : membersToAdd.entrySet()) {
            RaftGroupId groupId = e.getKey();
            RaftMemberImpl memberToAdd = e.getValue();
            RaftGroupInfo group = groups.get(groupId);
            checkTrue(group != null, groupId + " not found in the raft groups");

            Collection<RaftMemberImpl> candidates = memberMissingGroups.get(groupId);
            checkTrue(candidates != null, groupId + " has no membership change");
            checkTrue(candidates.contains(memberToAdd), groupId + " does not have " + membersToAdd
                    + " in its candidate list");

            if (group.status() == RaftGroupStatus.DESTROYED) {
                logger.warning("Will not expand " + groupId + " with " + membersToAdd + " since the group is already destroyed");
                continue;
            }

            long idx = group.getMembersCommitIndex();
            Collection<RaftMemberImpl> members = group.memberImpls();
            changes.add(new RaftGroupMembershipChangeContext(groupId, idx, members, memberToAdd, null));
        }

        logger.info("Raft groups will be expanded with the following changes: " + changes);

        membershipChangeContext = membershipChangeContext.setChanges(changes);
        return membershipChangeContext;
    }

    public void triggerDestroyRaftGroup(RaftGroupId groupId) {
        checkNotNull(groupId);
        checkState(membershipChangeContext == null,
                "Cannot destroy raft group while there are raft group membership changes");

        RaftGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No raft group exists for " + groupId + " to trigger destroy");

        if (group.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else {
            logger.info(groupId + " is already " + group.status());
        }
    }

    public void completeDestroyRaftGroups(Set<RaftGroupId> groupIds) {
        checkNotNull(groupIds);

        for (RaftGroupId groupId : groupIds) {
            checkNotNull(groupId);

            RaftGroupInfo group = groups.get(groupId);
            checkNotNull(group, "No raft group exists for " + groupId + " to complete destroy");

            completeDestroyRaftGroup(group);
        }
    }

    private void completeDestroyRaftGroup(RaftGroupInfo group) {
        RaftGroupId groupId = group.id();
        if (group.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            sendDestroyRaftNodeOps(group);
        } else {
            logger.fine(groupId + " is already destroyed.");
        }
    }

    public void forceDestroyRaftGroup(RaftGroupId groupId) {
        checkNotNull(groupId);
        checkFalse(METADATA_GROUP_ID.equals(groupId), "Cannot force-destroy the METADATA raft group");

        RaftGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No raft group exists for " + groupId + " to force-destroy");

        if (group.forceSetDestroyed()) {
            logger.info(groupId + " is force-destroyed.");
            sendDestroyRaftNodeOps(group);
        } else {
            logger.fine(groupId + " is already force-destroyed.");
        }
    }

    private void sendDestroyRaftNodeOps(RaftGroupInfo group) {
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new DestroyRaftNodesOp(singleton(group.id()));
        for (RaftMemberImpl member : group.memberImpls())  {
            if (member.equals(getLocalMember())) {
                raftService.destroyRaftNode(group.id());
            } else {
                operationService.send(op, member.getAddress());
            }
        }
    }

    /**
     * this method is idempotent
     */
    public void triggerRemoveMember(RaftMemberImpl leavingMember) {
        checkNotNull(leavingMember);
        failIfMetadataRaftGroupNotInitialized();

        if (!activeMembers.contains(leavingMember)) {
            logger.warning("Not removing " + leavingMember + " since it is not present in the active members");
            return;
        }

        if (membershipChangeContext != null) {
            if (leavingMember.equals(membershipChangeContext.getLeavingMember())) {
                logger.info(leavingMember + " is already marked as leaving.");
                return;
            }

            throw new CannotRemoveMemberException("There is already an ongoing raft group membership change process. "
                    + "Cannot process remove request of " + leavingMember);
        }

        if (activeMembers.size() <= 2) {
            logger.warning(leavingMember + " is directly removed as there are only " + activeMembers.size() + " members");
            removeActiveMember(leavingMember);
            return;
        }

        List<RaftGroupMembershipChangeContext> leavingGroups = new ArrayList<RaftGroupMembershipChangeContext>();
        for (RaftGroupInfo group : groups.values()) {
            RaftGroupId groupId = group.id();
            if (group.containsMember(leavingMember)) {
                if (group.status() == RaftGroupStatus.DESTROYED) {
                    logger.warning("Cannot remove " + leavingMember + " from " + groupId + " since the group is DESTROYED");
                    continue;
                }

                boolean foundSubstitute = false;
                for (RaftMemberImpl substitute : activeMembers) {
                    if (activeMembers.contains(substitute) && !group.containsMember(substitute)) {
                        leavingGroups.add(new RaftGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                                group.memberImpls(), substitute, leavingMember));
                        logger.fine("Substituted " + leavingMember + " with " + substitute + " in " + group);
                        foundSubstitute = true;
                        break;
                    }
                }
                if (!foundSubstitute) {
                    logger.fine("Cannot find a substitute for " + leavingMember + " in " + group);
                    leavingGroups.add(new RaftGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                            group.memberImpls(), null, leavingMember));
                }
            }
        }

        if (leavingGroups.isEmpty()) {
            logger.info(leavingMember + " is not present in any raft group. Removing it directly.");
            removeActiveMember(leavingMember);
            return;
        }

        membershipChangeContext = new MembershipChangeContext(leavingMember, leavingGroups);
        logger.info("Removing " + leavingMember + " from raft groups: " + leavingGroups);
    }

    public MembershipChangeContext completeRaftGroupMembershipChanges(Map<RaftGroupId, Tuple2<Long, Long>> changedGroups) {
        checkNotNull(changedGroups);
        checkState(membershipChangeContext != null,"Cannot apply raft group membership changes: "
                + changedGroups + " since there is no membership change context!");

        for (RaftGroupMembershipChangeContext ctx : membershipChangeContext.getChanges()) {
            RaftGroupId groupId = ctx.getGroupId();
            RaftGroupInfo group = groups.get(groupId);
            checkState(group != null, groupId + "not found in raft groups: " + groups.keySet()
                    + "to apply " + ctx);
            Tuple2<Long, Long> t = changedGroups.get(groupId);

            if (t == null) {
                if (group.status() == RaftGroupStatus.DESTROYED && !changedGroups.containsKey(groupId)) {
                    logger.warning(groupId + " is already destroyed so will skip: " + ctx);
                    changedGroups.put(groupId, Tuple2.of(0L, 0L));
                }
                continue;
            }

            long expectedMembersCommitIndex = t.element1;
            long newMembersCommitIndex = t.element2;

            RaftMemberImpl addedMember = ctx.getMemberToAdd();
            RaftMemberImpl removedMember = ctx.getMemberToRemove();

            if (group.applyMembershipChange(removedMember, addedMember, expectedMembersCommitIndex, newMembersCommitIndex)) {
                logger.fine("Applied add-member: " + (addedMember != null ? addedMember : "-") + " and remove-member: "
                        + (removedMember != null ? removedMember : "-") + " in "  + groupId
                        + " with new members commit index: " + newMembersCommitIndex);
                if (localMember.get().equals(addedMember)) {
                    // we are the added member to the group, we can try to create the local raft node if not created already
                    raftService.createRaftNode(groupId, group.members());
                } else if (addedMember != null) {
                    // publish group-info to the joining member
                    Operation op = new CreateRaftNodeOp(group.id(), group.initialMembers());
                    nodeEngine.getOperationService().send(op, addedMember.getAddress());
                }
            } else {
                logger.severe("Could not apply add-member: " + (addedMember != null ? addedMember : "-")
                        + " and remove-member: " + (removedMember != null ? removedMember : "-") + " in "  + group
                        + " with new members commit index: " + newMembersCommitIndex + " expected members commit index: "
                        + expectedMembersCommitIndex + " known members commit index: " + group.getMembersCommitIndex());
            }
        }

        membershipChangeContext = membershipChangeContext.excludeCompletedChanges(changedGroups.keySet());

        RaftMemberImpl leavingMember = membershipChangeContext.getLeavingMember();
        if (leavingMember != null) {
            boolean safeToRemove = true;
            for (RaftGroupInfo group : groups.values()) {
                if (group.containsMember(leavingMember)) {
                    if (group.status() == RaftGroupStatus.DESTROYED) {
                        logger.warning("Leaving " + leavingMember + " was in the destroyed " + group.id());
                    } else {
                        safeToRemove = false;
                        break;
                    }
                }
            }

            if (safeToRemove) {
                checkState(membershipChangeContext.hasNoPendingChanges(), "Leaving " + leavingMember
                        + " is removed from all groups but there are still pending membership changes: "
                        + membershipChangeContext);
                logger.info(leavingMember + " is removed from all raft groups and active members");
                removeActiveMember(leavingMember);
                membershipChangeContext = null;
                return null;
            }
        }

        if (membershipChangeContext.hasNoPendingChanges() && leavingMember == null) {
            // the current raft group rebalancing step is completed. let's attempt for another one
            Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = getMemberMissingActiveRaftGroups();
            if (memberMissingGroups.size() > 0) {
                membershipChangeContext = new MembershipChangeContext(memberMissingGroups);
                logger.info("Raft group rebalancing continues with " + memberMissingGroups);
            } else {
                membershipChangeContext = null;
                logger.info("Rebalancing is completed.");
            }
        }

        return membershipChangeContext;
    }

    private Map<RaftGroupId, List<RaftMemberImpl>> getMemberMissingActiveRaftGroups() {
        Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = new HashMap<RaftGroupId, List<RaftMemberImpl>>();
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() == RaftGroupStatus.ACTIVE &&
                    group.initialMemberCount() > group.memberCount() && activeMembers.size() > group.memberCount()) {
                List<RaftMemberImpl> candidates = new ArrayList<RaftMemberImpl>(activeMembers);
                candidates.removeAll(group.memberImpls());
                memberMissingGroups.put(group.id(), candidates);
            }
        }
        return memberMissingGroups;
    }

    public boolean isMemberRemoved(RaftMemberImpl member) {
        checkNotNull(member);

        return !activeMembers.contains(member);
    }

    public Collection<RaftMemberImpl> getActiveMembers() {
        if (membershipChangeContext == null) {
            return activeMembers;
        }
        List<RaftMemberImpl> active = new ArrayList<RaftMemberImpl>(activeMembers);
        active.remove(membershipChangeContext.getLeavingMember());
        return active;
    }

    public void setActiveMembers(Collection<RaftMemberImpl> members) {
        checkNotNull(members);
        checkTrue(members.size() > 1, "active members must contain at least 2 members: " + members);
        checkState(getLocalMember() == null, "This node is already part of Raft members!");

        logger.fine("Setting active members to " + members);
        activeMembers = unmodifiableCollection(new LinkedHashSet<RaftMemberImpl>(members));
        updateInvocationManagerMembers(members);
    }

    private void updateInvocationManagerMembers(Collection<RaftMemberImpl> members) {
        raftService.getInvocationManager().setAllMembers(members);
    }

    public Collection<RaftGroupId> getDestroyingRaftGroupIds() {
        Collection<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() == RaftGroupStatus.DESTROYING) {
                groupIds.add(group.id());
            }
        }
        return groupIds;
    }

    public MembershipChangeContext getMembershipChangeContext() {
        return membershipChangeContext;
    }

    boolean isMetadataLeader() {
        RaftMemberImpl member = localMember.get();
        if (member == null) {
            return false;
        }
        RaftNode raftNode = raftService.getRaftNode(METADATA_GROUP_ID);
        // even if the local leader information is stale, it is fine.
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && member.equals(raftNode.getLeader());
    }

    /**
     * this method is idempotent
     */
    public void addActiveMember(RaftMemberImpl member) {
        checkNotNull(member);
        failIfMetadataRaftGroupNotInitialized();

        if (activeMembers.contains(member)) {
            logger.fine(member + " already exists. Silently returning from addActiveMember().");
            return;
        }
        if (membershipChangeContext != null && member.equals(membershipChangeContext.getLeavingMember())) {
            throw new IllegalArgumentException(member + " is already being removed!");
        }
        Collection<RaftMemberImpl> newMembers = new LinkedHashSet<RaftMemberImpl>(activeMembers);
        newMembers.add(member);
        activeMembers = unmodifiableCollection(newMembers);
        updateInvocationManagerMembers(newMembers);
        broadcastActiveMembers();
        logger.info("Added " + member + ". Active members are " + newMembers);
    }

    private void removeActiveMember(RaftMemberImpl member) {
        Collection<RaftMemberImpl> newMembers = new LinkedHashSet<RaftMemberImpl>(activeMembers);
        newMembers.remove(member);
        activeMembers = unmodifiableCollection(newMembers);
        updateInvocationManagerMembers(newMembers);
        broadcastActiveMembers();
    }

    @SuppressWarnings("unchecked")
    private void commitInitialMetadataRaftGroup(List<RaftMemberImpl> metadataMembers) {
        if (metadataMembers.contains(localMember.get())) {
            raftService.createRaftNode(METADATA_GROUP_ID, (Collection) metadataMembers);
            try {
                RaftOp op = new CreateMetadataRaftGroupOp(metadataMembers);
                raftService.getInvocationManager().invoke(METADATA_GROUP_ID, op).get();
                logger.info("METADATA raft group is created with " + metadataMembers);
            } catch (Exception e) {
                logger.severe("COULD NOT CREATE METADATA RAFT GROUP WITH " + metadataMembers, e);
            }
        }
    }

    private void failIfMetadataRaftGroupNotInitialized() {
        if (!groups.containsKey(METADATA_GROUP_ID)) {
            throw new MetadataRaftGroupNotInitializedException();
        }
    }

    void broadcastActiveMembers() {
        if (localMember.get() == null) {
            return;
        }
        Collection<RaftMemberImpl> members = activeMembers;
        if (members.isEmpty()) {
            return;
        }

        Set<Address> addresses = new HashSet<Address>(members.size());
        for (RaftMemberImpl member : members) {
            addresses.add(member.getAddress());
        }

        Set<Member> clusterMembers = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new SendActiveRaftMembersOp(getActiveMembers());
        for (Member member : clusterMembers) {
            if (addresses.contains(member.getAddress())) {
                continue;
            }
            operationService.send(op, member.getAddress());
        }
    }

    private class BroadcastActiveMembersTask implements Runnable {
        @Override
        public void run() {
            if (!isMetadataLeader()) {
                return;
            }
            broadcastActiveMembers();
        }
    }

    private class DiscoverInitialRaftMembersTask implements Runnable {
        @Override
        public void run() {
            ExecutionService executionService = nodeEngine.getExecutionService();
            Collection<Member> members = nodeEngine.getClusterService().getMembers(RAFT_MEMBER_SELECTOR);

            setInitialRaftMemberAttribute();

            if (members.size() < config.getGroupSize()) {
                logger.warning("Waiting for " + config.getGroupSize() + " Raft members to join the cluster. "
                        + "Current Raft members count: " + members.size());
                executionService.schedule(this, 500, TimeUnit.MILLISECONDS);
                return;
            }

            if (members.size() > config.getGroupSize()) {
                logger.severe("INVALID RAFT MEMBERS INITIALIZATION !!! "
                        + "Expected Raft member count: " + config.getGroupSize()
                        + ", Current member count: " + members.size());
            }

            List<RaftMemberImpl> raftMembers = getInitialRaftMembers(members);

            if (!raftMembers.contains(localMember.get())) {
                logger.warning("I am demoting to the non-raft member node!");
                localMember.set(null);
                return;
            }

            activeMembers = unmodifiableCollection(new LinkedHashSet<RaftMemberImpl>(raftMembers));
            updateInvocationManagerMembers(activeMembers);

            List<RaftMemberImpl> metadataMembers = raftMembers.subList(0, config.getMetadataGroupSize());
            commitInitialMetadataRaftGroup(metadataMembers);

            broadcastActiveMembers();
        }

        private void setInitialRaftMemberAttribute() {
            Member localMember = nodeEngine.getLocalMember();
            if (Boolean.TRUE.equals(localMember.getBooleanAttribute(RAFT_MEMBER_ATTRIBUTE_NAME))) {
                // Member attributes have a weak replication guarantee.
                // Broadcast member attribute to the cluster again to make sure everyone learns it eventually.
                Operation op = new MemberAttributeChangedOp(MemberAttributeOperationType.PUT, RAFT_MEMBER_ATTRIBUTE_NAME, true);
                for (Member member : nodeEngine.getClusterService().getMembers(NON_LOCAL_MEMBER_SELECTOR)) {
                    nodeEngine.getOperationService().send(op, member.getAddress());
                }
            } else {
                localMember.setBooleanAttribute(RAFT_MEMBER_ATTRIBUTE_NAME, true);
            }
        }
    }

    private List<RaftMemberImpl> getInitialRaftMembers(Collection<Member> members) {
        List<RaftMemberImpl> raftMembers = new ArrayList<RaftMemberImpl>(config.getGroupSize());
        for (Member member : members) {
            RaftMemberImpl raftMember = new RaftMemberImpl(member);
            raftMembers.add(raftMember);
        }

        sort(raftMembers, new Comparator<RaftMemberImpl>() {
            @Override
            public int compare(RaftMemberImpl e1, RaftMemberImpl e2) {
                return e1.getUid().compareTo(e2.getUid());
            }
        });

        return raftMembers.subList(0, config.getGroupSize());
    }

    private static class RaftMemberSelector implements MemberSelector {
        @Override
        public boolean select(Member member) {
            Boolean raftMember = member.getBooleanAttribute(RAFT_MEMBER_ATTRIBUTE_NAME);
            return Boolean.TRUE.equals(raftMember);
        }
    }
}
