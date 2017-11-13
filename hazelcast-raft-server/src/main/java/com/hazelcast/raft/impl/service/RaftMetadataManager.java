package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.exception.CannotRemoveEndpointException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftNodeOp;
import com.hazelcast.raft.impl.util.Pair;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.raft.impl.RaftEndpoint.parseEndpoints;
import static com.hazelcast.raft.impl.service.LeavingRaftEndpointContext.RaftGroupLeavingEndpointContext;
import static com.hazelcast.raft.impl.service.RaftService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftMetadataManager implements SnapshotAwareService<MetadataSnapshot>  {

    private static final String METADATA_RAFT = "METADATA";
    public static final RaftGroupId METADATA_GROUP_ID = new RaftGroupIdImpl(METADATA_RAFT, 0);

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftConfig config;

    // raftGroups are read outside of Raft
    private final Map<RaftGroupId, RaftGroupInfo> raftGroups = new ConcurrentHashMap<RaftGroupId, RaftGroupInfo>();
    private final List<RaftEndpoint> allEndpoints;
    private final RaftEndpoint localEndpoint;

    private final Collection<RaftEndpoint> removedEndpoints = new HashSet<RaftEndpoint>();
    private LeavingRaftEndpointContext leavingEndpointContext;

    RaftMetadataManager(NodeEngine nodeEngine, RaftService raftService, RaftConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;

        try {
            this.allEndpoints = unmodifiableList(sort(parseEndpoints(config.getMembers())));
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
        this.localEndpoint = findLocalEndpoint(allEndpoints);
    }

    public void init() {
        logger.info("CP nodes: " + allEndpoints + ", local: " + localEndpoint);
        if (localEndpoint == null) {
            logger.warning("We are not in CP nodes group :(");
            return;
        }
        try {
            List<RaftEndpoint> metadataEndpoints = parseEndpoints(config.getMetadataGroupMembers());
            if (metadataEndpoints.contains(localEndpoint)) {
                createRaftGroup(new RaftGroupInfo(METADATA_GROUP_ID, sort(metadataEndpoints), SERVICE_NAME));
            }
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
    }

    private List<RaftEndpoint> sort(List<RaftEndpoint> endpoints) {
        Collections.sort(endpoints, new Comparator<RaftEndpoint>() {
            @Override
            public int compare(RaftEndpoint e1, RaftEndpoint e2) {
                return e1.getUid().compareTo(e2.getUid());
            }
        });

        return endpoints;
    }

    private RaftEndpoint findLocalEndpoint(Collection<RaftEndpoint> endpoints) {
        for (RaftEndpoint endpoint : endpoints) {
            if (nodeEngine.getThisAddress().equals(endpoint.getAddress())) {
                return endpoint;
            }
        }
        return null;
    }

    @Override
    public MetadataSnapshot takeSnapshot(RaftGroupId raftGroupId, long commitIndex) {
        ensureMetadataGroupId(raftGroupId);

        MetadataSnapshot snapshot = new MetadataSnapshot();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            assert groupInfo.commitIndex() <= commitIndex
                    : "Group commit index: " + groupInfo.commitIndex() + ", snapshot commit index: " + commitIndex;
            snapshot.addRaftGroup(groupInfo);
        }
        for (RaftEndpoint endpoint : allEndpoints) {
            snapshot.addEndpoint(endpoint);
        }
        for (RaftEndpoint endpoint : removedEndpoints) {
            snapshot.addRemovedEndpoint(endpoint);
        }
        snapshot.setLeavingRaftEndpointContext(leavingEndpointContext);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, MetadataSnapshot snapshot) {
        ensureMetadataGroupId(raftGroupId);

        for (RaftGroupInfo groupInfo : snapshot.getRaftGroups()) {
            RaftGroupInfo existingGroupInfo = raftGroups.get(groupInfo.id());

            if (groupInfo.status() == RaftGroupStatus.ACTIVE && existingGroupInfo == null) {
                createRaftGroup(groupInfo);
                continue;
            }

            if (groupInfo.status() == RaftGroupStatus.DESTROYING) {
                if (existingGroupInfo == null) {
                    createRaftGroup(groupInfo);
                } else {
                    existingGroupInfo.setDestroying();
                }
                continue;
            }

            if (groupInfo.status() == RaftGroupStatus.DESTROYED) {
                if (existingGroupInfo == null) {
                    addGroupInfo(groupInfo);
                } else {
                    completeDestroy(existingGroupInfo);
                }
                continue;
            }
        }

        for (RaftEndpoint endpoint : snapshot.getEndpoints()) {
            // TODO: restore endpoints
            // currently we don't modify endpoints
        }

        removedEndpoints.clear();
        removedEndpoints.addAll(snapshot.getRemovedEndpoints());

        leavingEndpointContext = snapshot.getLeavingRaftEndpointContext();
    }

    private void ensureMetadataGroupId(RaftGroupId raftGroupId) {
        if (!METADATA_GROUP_ID.equals(raftGroupId)) {
            throw new IllegalArgumentException("Invalid RaftGroupId! Expected: " + METADATA_GROUP_ID
                    + ", Actual: " + raftGroupId);
        }
    }

    public Collection<RaftEndpoint> getAllEndpoints() {
        return allEndpoints;
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    public RaftGroupInfo getRaftGroupInfo(RaftGroupId id) {
        return raftGroups.get(id);
    }

    public RaftGroupId createRaftGroup(String serviceName, String name, Collection<RaftEndpoint> endpoints, long commitIndex) {
        // keep configuration on every metadata node
        RaftGroupInfo groupInfo = getRaftGroupInfoByName(name);
        if (groupInfo != null) {
            if (groupInfo.memberCount() == endpoints.size()) {
                logger.warning("Raft group " + name + " already exists. Ignoring add raft node request.");
                return groupInfo.id();
            }

            throw new IllegalStateException("Raft group " + name
                    + " already exists with different group size. Ignoring add raft node request.");
        }

        RaftEndpoint leavingEndpoint = leavingEndpointContext != null ? leavingEndpointContext.getEndpoint() : null;
        for (RaftEndpoint endpoint : endpoints) {
            if (endpoint.equals(leavingEndpoint) || removedEndpoints.contains(endpoint)) {
                throw new CannotCreateRaftGroupException("Cannot create raft group: " + name + " since " + endpoint
                        + " is not active");
            }
        }

        return createRaftGroup(new RaftGroupInfo(new RaftGroupIdImpl(name, commitIndex), endpoints, serviceName));
    }

    private RaftGroupId createRaftGroup(RaftGroupInfo groupInfo) {
        addGroupInfo(groupInfo);
        logger.info("New raft group: " + groupInfo.id() + " is created with members: " + groupInfo.members());

        RaftGroupId groupId = groupInfo.id();
        if (groupInfo.containsMember(localEndpoint)) {
            raftService.createRaftNode(groupId, groupInfo.serviceName(), groupInfo.members());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            RaftGroupInfo metadataGroup = raftGroups.get(RaftService.METADATA_GROUP_ID);
            for (RaftEndpoint endpoint : groupInfo.members()) {
                if (!metadataGroup.containsMember(endpoint)) {
                    operationService.send(new CreateRaftNodeOp(groupInfo), endpoint.getAddress());
                }
            }
        }

        return groupId;
    }

    private void addGroupInfo(RaftGroupInfo groupInfo) {
        RaftGroupId groupId = groupInfo.id();
        if (raftGroups.containsKey(groupId)) {
            throw new IllegalStateException(groupInfo + " already exists.");
        }
        raftGroups.put(groupId, groupInfo);
    }

    private RaftGroupInfo getRaftGroupInfoByName(String name) {
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.status() != RaftGroupStatus.DESTROYED && groupInfo.name().equals(name)) {
                return groupInfo;
            }
        }
        return null;
    }

    public void triggerDestroy(RaftGroupId groupId) {
        RaftGroupInfo groupInfo = raftGroups.get(groupId);
        checkNotNull(groupInfo, "No raft group exists for " + groupId + " to trigger destroy");

        if (groupInfo.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else {
            logger.info(groupId + " is already " + groupInfo.status());
        }
    }

    public void completeDestroy(Set<RaftGroupId> groupIds) {
        for (RaftGroupId groupId : groupIds) {
            completeDestroy(groupId);
        }
    }

    private void completeDestroy(RaftGroupId groupId) {
        RaftGroupInfo groupInfo = raftGroups.get(groupId);
        checkNotNull(groupInfo, "No raft group exists for " + groupId + " to commit destroy");

        completeDestroy(groupInfo);
    }

    private void completeDestroy(RaftGroupInfo groupInfo) {
        RaftGroupId groupId = groupInfo.id();
        if (groupInfo.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            raftService.destroyRaftNode(groupId);
        }
    }

    public void triggerRemoveEndpoint(RaftEndpoint leavingEndpoint) {
        if (!allEndpoints.contains(leavingEndpoint)) {
            throw new IllegalArgumentException(leavingEndpoint + " doesn't exist!");
        }

        if (leavingEndpointContext != null) {
            if (leavingEndpointContext.getEndpoint().equals(leavingEndpoint)) {
                logger.info(leavingEndpoint + " is already marked as leaving.");
                return;
            }

            throw new CannotRemoveEndpointException("Another node " + leavingEndpointContext.getEndpoint()
                    + " is currently leaving, cannot process remove request of " + leavingEndpoint);
        }

        logger.info("Removing " + leavingEndpoint);

        if (allEndpoints.size() - removedEndpoints.size() <= 2) {
            logger.warning("Removed member directly for " + leavingEndpoint);
            removedEndpoints.add(leavingEndpoint);
            return;
        }

        Map<RaftGroupId, RaftGroupLeavingEndpointContext> leavingGroups = new LinkedHashMap<RaftGroupId, RaftGroupLeavingEndpointContext>();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            RaftGroupId groupId = groupInfo.id();
            if (groupInfo.containsMember(leavingEndpoint)) {
                boolean foundSubstitute = false;
                for (RaftEndpoint substitute : allEndpoints) {
                    if (!removedEndpoints.contains(substitute) && !groupInfo.containsMember(substitute)) {
                        leavingGroups.put(groupId, new RaftGroupLeavingEndpointContext(groupInfo.getMembersCommitIndex(),
                                groupInfo.members(), substitute));
                        logger.fine("Substituted " + leavingEndpoint + " with " + substitute + " in " + groupInfo);
                        foundSubstitute = true;
                        break;
                    }
                }
                if (!foundSubstitute) {
                    logger.fine("Cannot find a substitute for " + leavingEndpoint + " in " + groupInfo);
                    leavingGroups.put(groupId, new RaftGroupLeavingEndpointContext(groupInfo.getMembersCommitIndex(),
                            groupInfo.members(), null));
                }
            }
        }

        leavingEndpointContext = new LeavingRaftEndpointContext(leavingEndpoint, leavingGroups);
    }

    public void completeRemoveEndpoint(RaftEndpoint leavingEndpoint, Map<RaftGroupId, Pair<Long, Long>> leftGroups) {
        if (!allEndpoints.contains(leavingEndpoint)) {
            throw new IllegalArgumentException("Cannot remove " + leavingEndpoint + " from groups: " + leftGroups.keySet()
                    + " since " +  leavingEndpoint + " doesn't exist!");
        }

        if (leavingEndpointContext == null) {
            throw new IllegalStateException("Cannot remove " + leavingEndpoint + " from groups: " + leftGroups.keySet()
                    + " since there is no leaving endpoint!");
        }

        if (!leavingEndpointContext.getEndpoint().equals(leavingEndpoint)) {
            throw new IllegalArgumentException("Cannot remove " + leavingEndpoint + " from groups: " + leftGroups.keySet()
                    + " since " + leavingEndpointContext.getEndpoint() + " is currently leaving.");
        }

        Map<RaftGroupId, RaftGroupLeavingEndpointContext> leavingGroups = leavingEndpointContext.getGroups();
        for (Entry<RaftGroupId, Pair<Long, Long>> e : leftGroups.entrySet()) {
            RaftGroupId groupId = e.getKey();
            RaftGroupInfo groupInfo = raftGroups.get(groupId);

            Pair<Long, Long> value = e.getValue();
            long expectedMembersCommitIndex = value.getPrimary();
            long newMembersCommitIndex = value.getSecondary();
            RaftEndpoint joining = leavingGroups.get(groupId).getSubstitute();

            if (groupInfo.substitute(leavingEndpoint, joining, expectedMembersCommitIndex, newMembersCommitIndex)) {
                logger.fine("Removed " + leavingEndpoint + " from " + groupInfo + " with new members commit index: "
                        + newMembersCommitIndex);
                if (localEndpoint.equals(joining)) {
                    // we are the added member to the group, we can try to create the local raft node if not created already
                    raftService.createRaftNode(groupId, groupInfo.serviceName(), groupInfo.members());
                } else if (joining != null) {
                    // publish group-info to the joining member
                    nodeEngine.getOperationService().send(new CreateRaftNodeOp(groupInfo), joining.getAddress());
                }
            } else {
                logger.warning("Could not substitute " + leavingEndpoint + " with " + joining + " in " + groupId);
            }
        }

        boolean safeToRemove = true;
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.containsMember(leavingEndpoint)) {
                safeToRemove = false;
                break;
            }
        }

        if (safeToRemove) {
            logger.info("Remove member procedure completed for " + leavingEndpoint);
            removedEndpoints.add(leavingEndpoint);
            leavingEndpointContext = null;
        } else if (!leftGroups.isEmpty()) {
            // no need to re-attempt for successfully left groups
            leavingEndpointContext = leavingEndpointContext.exclude(leftGroups.keySet());
        }
    }

    public boolean isRemoved(RaftEndpoint endpoint) {
        return removedEndpoints.contains(endpoint);
    }

    public List<RaftEndpoint> getActiveEndpoints() {
        List<RaftEndpoint> active = new ArrayList<RaftEndpoint>(allEndpoints);
        if (leavingEndpointContext != null) {
            active.remove(leavingEndpointContext.getEndpoint());
        }
        active.removeAll(removedEndpoints);

        return active;
    }

    public Collection<RaftGroupId> getDestroyingRaftGroupIds() {
        Collection<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            if (groupInfo.status() == RaftGroupStatus.DESTROYING) {
                groupIds.add(groupInfo.id());
            }
        }
        return groupIds;
    }

    public LeavingRaftEndpointContext getLeavingEndpointContext() {
        return leavingEndpointContext;
    }

}
