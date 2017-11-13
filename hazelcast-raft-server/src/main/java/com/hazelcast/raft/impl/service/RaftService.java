package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.exception.CannotRemoveEndpointException;
import com.hazelcast.raft.impl.service.operation.metadata.CheckRemovedEndpointOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerRemoveEndpointOp;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.GracefulShutdownAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftCleanupHandler.CLEANUP_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singletonList;

/**
 * TODO: Javadoc Pending...
 */
public class RaftService implements ManagedService, ConfigurableService<RaftConfig>,
        SnapshotAwareService<MetadataSnapshot>, GracefulShutdownAwareService {

    public static final String SERVICE_NAME = "hz:core:raft";
    public static final RaftGroupId METADATA_GROUP_ID = RaftMetadataManager.METADATA_GROUP_ID;

    private final ConcurrentMap<RaftGroupId, RaftNode> nodes = new ConcurrentHashMap<RaftGroupId, RaftNode>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final Set<RaftGroupId> destroyedGroupIds = newSetFromMap(new ConcurrentHashMap<RaftGroupId, Boolean>());

    private volatile RaftInvocationManager invocationManager;
    private volatile RaftMetadataManager metadataManager;

    private volatile RaftConfig config;

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void configure(RaftConfig config) {
        // cloning given RaftConfig to avoid further mutations
        this.config = new RaftConfig(config);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        RaftCleanupHandler cleanupHandler = new RaftCleanupHandler(nodeEngine, this);
        metadataManager = new RaftMetadataManager(nodeEngine, this, config);
        invocationManager = new RaftInvocationManager(nodeEngine, this, config);

        metadataManager.init();
        invocationManager.init();
        cleanupHandler.init();
    }

    @Override
    public void reset() {
        if (invocationManager != null) {
            invocationManager.reset();
        }
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public MetadataSnapshot takeSnapshot(RaftGroupId raftGroupId, long commitIndex) {
        return metadataManager.takeSnapshot(raftGroupId, commitIndex);
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, long commitIndex, MetadataSnapshot snapshot) {
        metadataManager.restoreSnapshot(raftGroupId, commitIndex, snapshot);
    }

    @Override
    public boolean onShutdown(final long timeout, TimeUnit unit) {
        RaftEndpoint localEndpoint = getLocalEndpoint();
        if (localEndpoint == null) {
            return true;
        }

        logger.fine("Triggering remove member procedure for " + localEndpoint);

        long remainingTimeNanos = unit.toNanos(timeout);
        long start = System.nanoTime();

        ensureTriggerShutdown(localEndpoint, remainingTimeNanos);
        remainingTimeNanos -= (System.nanoTime() - start);

        // wait for us being replaced in all raft groups we are participating
        // and removed from all raft groups
        logger.fine("Waiting remove member procedure to be completed for " + localEndpoint
                + ", remaining time: " + TimeUnit.NANOSECONDS.toMillis(remainingTimeNanos) + " ms.");
        while (remainingTimeNanos > 0) {
            if (isRemoved(localEndpoint)) {
                logger.fine("Remove member procedure completed for " + localEndpoint);
                return true;
            }
            try {
                Thread.sleep(CLEANUP_TASK_PERIOD_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            remainingTimeNanos -= CLEANUP_TASK_PERIOD_IN_MILLIS;
        }
        logger.fine("Remove member procedure NOT completed for " + localEndpoint + " in "
                + unit.toMillis(timeout) + " ms.");
        return false;
    }

    private void ensureTriggerShutdown(RaftEndpoint endpoint, long remainingTimeNanos) {
        while (remainingTimeNanos > 0) {
            long start = System.nanoTime();
            try {
                // mark us as shutting-down in metadata
                Future<RaftGroupId> future = triggerRemoveEndpointAsync(endpoint);
                future.get(remainingTimeNanos, TimeUnit.NANOSECONDS);
                logger.fine(endpoint + " is marked as being removed.");
                return;
            } catch (CannotRemoveEndpointException e) {
                remainingTimeNanos -= (System.nanoTime() - start);
                if (remainingTimeNanos <= 0) {
                    throw e;
                }
                logger.fine(e.getMessage());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    public RaftMetadataManager getMetadataManager() {
        return metadataManager;
    }

    public RaftInvocationManager getInvocationManager() {
        return invocationManager;
    }

    public void handlePreVoteRequest(RaftGroupId groupId, PreVoteRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handlePreVoteRequest(request);
    }

    public void handlePreVoteResponse(RaftGroupId groupId, PreVoteResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handlePreVoteResponse(response);
    }

    public void handleVoteRequest(RaftGroupId groupId, VoteRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(RaftGroupId groupId, VoteResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(RaftGroupId groupId, AppendRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendSuccessResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendFailureResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleSnapshot(RaftGroupId groupId, InstallSnapshot request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleInstallSnapshot(request);
    }

    public Collection<RaftNode> getAllRaftNodes() {
        return new ArrayList<RaftNode>(nodes.values());
    }

    public RaftNode getRaftNode(final RaftGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftNode getOrInitRaftNode(final RaftGroupId groupId) {
        RaftNode node = nodes.get(groupId);
        if (node == null && !destroyedGroupIds.contains(groupId)) {
            logger.fine("There is no RaftNode for " + groupId + ". Asking to the metadata group...");
            nodeEngine.getExecutionService().execute(ASYNC_EXECUTOR, new QueryRaftGroupInfoTask(groupId));
        }
        return node;
    }

    public RaftGroupInfo getRaftGroupInfo(RaftGroupId id) {
        return metadataManager.getRaftGroupInfo(id);
    }

    public boolean isDestroyed(RaftGroupId groupId) {
        return destroyedGroupIds.contains(groupId);
    }

    public RaftConfig getConfig() {
        return config;
    }

    public Collection<RaftEndpoint> getAllEndpoints() {
        return metadataManager.getAllEndpoints();
    }

    public RaftEndpoint getLocalEndpoint() {
        return metadataManager.getLocalEndpoint();
    }

    void createRaftNode(RaftGroupId groupId, String serviceName, Collection<RaftEndpoint> endpoints) {
        if (nodes.containsKey(groupId)) {
            logger.info("Not creating RaftNode for " + groupId + " since it is already created...");
            return;
        }

        if (destroyedGroupIds.contains(groupId)) {
            logger.warning("Not creating RaftNode for " + groupId + " since it is already destroyed");
            return;
        }

        RaftIntegration raftIntegration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftNodeImpl node = new RaftNodeImpl(serviceName, groupId, getLocalEndpoint(), endpoints, config, raftIntegration);

        if (nodes.putIfAbsent(groupId, node) == null) {
            if (destroyedGroupIds.contains(groupId)) {
                node.forceSetTerminatedStatus();
                logger.warning("Not creating RaftNode for " + groupId + " since it is already destroyed");
                return;
            }

            node.start();
            logger.info("RaftNode created for: " + groupId + " with members: " + endpoints);
        }
    }

    public void createRaftNode(RaftGroupInfo groupInfo) {
        RaftEndpoint localEndpoint = getLocalEndpoint();
        if (groupInfo.containsMember(localEndpoint)) {
            Collection<RaftEndpoint> members = groupInfo.isInitialMember(localEndpoint)
                    ? groupInfo.members() : singletonList(localEndpoint);
            createRaftNode(groupInfo.id(), groupInfo.serviceName(), members);
        }
    }

    public void destroyRaftNode(RaftGroupId groupId) {
        destroyedGroupIds.add(groupId);
        RaftNode node = nodes.remove(groupId);
        if (node != null) {
            node.forceSetTerminatedStatus();
            logger.info("Local raft node of " + groupId + " is destroyed.");
        }
    }

    private ICompletableFuture<RaftGroupId> triggerRemoveEndpointAsync(final RaftEndpoint endpoint) {
        return invocationManager.invoke(METADATA_GROUP_ID, new TriggerRemoveEndpointOp(endpoint));
    }

    private boolean isRemoved(final RaftEndpoint endpoint) {
        ICompletableFuture<Boolean> f = invocationManager.query(METADATA_GROUP_ID, new CheckRemovedEndpointOp(endpoint),
                QueryPolicy.LEADER_LOCAL);
        try {
            return f.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private class QueryRaftGroupInfoTask implements Runnable {
        private final RaftGroupId groupId;

        QueryRaftGroupInfoTask(RaftGroupId groupId) {
            this.groupId = groupId;
        }

        @Override
        public void run() {
            ICompletableFuture<RaftGroupInfo> f = invocationManager.query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId),
                    QueryPolicy.LEADER_LOCAL);
            f.andThen(new ExecutionCallback<RaftGroupInfo>() {
                @Override
                public void onResponse(RaftGroupInfo groupInfo) {
                    if (groupInfo == null) {
                        return;
                    }

                    if (groupInfo.status() == RaftGroupStatus.DESTROYED) {
                        destroyRaftNode(groupId);
                        return;
                    }

                    createRaftNode(groupInfo);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Cannot get raft group: " + groupId + " from the metadata group", t);
                }
            });
        }
    }
}
