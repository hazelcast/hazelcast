package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.CannotRunLocalQueryException;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveEndpointsOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.proxy.ChangeRaftGroupMembershipOp;
import com.hazelcast.raft.impl.service.proxy.RaftQueryOp;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftReplicateOp;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.proxy.ChangeRaftGroupMembershipOp.NAN_MEMBERS_COMMIT_INDEX;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

public class RaftInvocationManager {

    private static final int MAX_RETRY_DELAY = 250;
    private static final Executor CALLER_RUNS_EXECUTOR = new CallerRunsExecutor();

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final ConcurrentMap<RaftGroupId, RaftEndpoint> knownLeaders = new ConcurrentHashMap<RaftGroupId, RaftEndpoint>();
    private final RaftEndpoint[] allEndpoints;
    private final boolean failOnIndeterminateOperationState;

    RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService, RaftConfig config) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.allEndpoints = raftService.getMetadataManager().getAllEndpoints().toArray(new RaftEndpoint[0]);
        this.failOnIndeterminateOperationState = config.isFailOnIndeterminateOperationState();
    }

    public void init() {
    }

    public void reset() {
        knownLeaders.clear();
    }

    public ICompletableFuture<RaftGroupId> createRaftGroupAsync(String serviceName, String raftName, int nodeCount) {
        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        ILogger logger = nodeEngine.getLogger(getClass());
        SimpleCompletableFuture<RaftGroupId> resultFuture = new SimpleCompletableFuture<RaftGroupId>(executor, logger);
        invokeGetEndpointsToCreateRaftGroup(serviceName, raftName, nodeCount, resultFuture);
        return resultFuture;
    }

    public RaftGroupId createRaftGroup(String serviceName, String raftName, int nodeCount)
            throws ExecutionException, InterruptedException {
        return createRaftGroupAsync(serviceName, raftName, nodeCount).get();
    }

    private void invokeGetEndpointsToCreateRaftGroup(final String serviceName, final String raftName, final int nodeCount,
                                                     final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<List<RaftEndpoint>> f = query(METADATA_GROUP_ID, new GetActiveEndpointsOp(), QueryPolicy.LEADER_LOCAL);

        f.andThen(new ExecutionCallback<List<RaftEndpoint>>() {
            @Override
            public void onResponse(List<RaftEndpoint> endpoints) {
                endpoints = new ArrayList<RaftEndpoint>(endpoints);

                if (endpoints.size() < nodeCount) {
                    Exception result = new IllegalArgumentException("There are not enough active endpoints to create raft group "
                            + raftName + ". Active endpoints: " + endpoints.size() + ", Requested count: " + nodeCount);
                    resultFuture.setResult(result);
                    return;
                }

                Collections.shuffle(endpoints);
                endpoints = endpoints.subList(0, nodeCount);
                invokeCreateRaftGroup(serviceName, raftName, nodeCount, endpoints, resultFuture);
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.setResult(t);
            }
        });
    }

    private void invokeCreateRaftGroup(final String serviceName, final String raftName, final int nodeCount,
                                       final List<RaftEndpoint> endpoints,
                                       final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<RaftGroupId> f = invoke(METADATA_GROUP_ID, new CreateRaftGroupOp(serviceName, raftName, endpoints));

        f.andThen(new ExecutionCallback<RaftGroupId>() {
            @Override
            public void onResponse(RaftGroupId groupId) {
                resultFuture.setResult(groupId);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t.getCause() instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create raft group: " + raftName + " with endpoints: " + endpoints,
                            t.getCause());
                    invokeGetEndpointsToCreateRaftGroup(serviceName, raftName, nodeCount, resultFuture);
                    return;
                }

                resultFuture.setResult(t);
            }
        });
    }

    public ICompletableFuture<RaftGroupId> triggerDestroyRaftGroupAsync(final RaftGroupId groupId) {
        return invoke(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOp(groupId));
    }

    public void triggerDestroyRaftGroup(RaftGroupId groupId) throws ExecutionException, InterruptedException {
        triggerDestroyRaftGroupAsync(groupId).get();
    }

    <T> ICompletableFuture<T> changeRaftGroupMembership(RaftGroupId groupId, RaftEndpoint endpoint,
                                                        MembershipChangeType changeType) {
        ChangeRaftGroupMembershipInvocationFuture<T> invocationFuture =
                new ChangeRaftGroupMembershipInvocationFuture<T>(groupId, NAN_MEMBERS_COMMIT_INDEX, endpoint, changeType);
        invocationFuture.invoke();
        return invocationFuture;
    }

    <T> ICompletableFuture<T> changeRaftGroupMembership(RaftGroupId groupId, long membersCommitIndex, RaftEndpoint endpoint,
                                                        MembershipChangeType changeType) {
        ChangeRaftGroupMembershipInvocationFuture<T> invocationFuture =
                new ChangeRaftGroupMembershipInvocationFuture<T>(groupId, membersCommitIndex, endpoint, changeType);
        invocationFuture.invoke();
        return invocationFuture;
    }

    public <T> ICompletableFuture<T> invoke(RaftGroupId groupId, RaftOperation raftOperation) {
        RaftInvocationFuture<T> invocationFuture = new RaftInvocationFuture<T>(groupId, raftOperation);
        invocationFuture.invoke();
        return invocationFuture;
    }

    public <T> ICompletableFuture<T> query(RaftGroupId groupId, RaftOperation raftOperation, QueryPolicy queryPolicy) {
        RaftQueryInvocationFuture<T> invocationFuture = new RaftQueryInvocationFuture<T>(groupId, raftOperation, queryPolicy);
        invocationFuture.invoke();
        return invocationFuture;
    }

    public <T> ICompletableFuture<T> queryOnLocal(RaftGroupId groupId, RaftOperation raftOperation, QueryPolicy queryPolicy) {
        RaftQueryOp queryOperation = new RaftQueryOp(groupId, raftOperation);
        return nodeEngine.getOperationService().invokeOnTarget(RaftService.SERVICE_NAME,
                queryOperation.setQueryPolicy(queryPolicy), nodeEngine.getThisAddress());
    }

    private void resetKnownLeader(RaftGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    private void setKnownLeader(RaftGroupId groupId, RaftEndpoint leader) {
        logger.fine("Setting known leader for raft: " + groupId + " to " + leader);
        knownLeaders.put(groupId, leader);
    }

    private RaftEndpoint getKnownLeader(RaftGroupId groupId) {
        return knownLeaders.get(groupId);
    }

    private void updateKnownLeaderOnFailure(RaftGroupId groupId, Throwable cause) {
        if (cause instanceof RaftException) {
            RaftException e = (RaftException) cause;
            RaftEndpoint leader = e.getLeader();
            if (leader != null) {
                setKnownLeader(groupId, leader);
            } else {
                resetKnownLeader(groupId);
            }
        } else {
            resetKnownLeader(groupId);
        }
    }

    private abstract class AbstractRaftInvocationFuture<T, O extends Operation>
            extends AbstractCompletableFuture<T> implements ExecutionCallback<T> {

        final RaftGroupId groupId;
        private final int partitionId;
        private volatile EndpointCursor endpointCursor;

        AbstractRaftInvocationFuture(RaftGroupId groupId) {
            super(nodeEngine, RaftInvocationManager.this.logger);
            this.groupId = groupId;
            partitionId = nodeEngine.getPartitionService().getPartitionId(groupId);
        }

        @Override
        public void onResponse(T response) {
            setResult(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            if (isRetryable(cause)) {
                logger.warning("Failure while invoking " + operationToString() + " -> " + cause);
                updateKnownLeaderOnFailure(groupId, cause);
                try {
                    scheduleRetry();
                } catch (Throwable e) {
                    logger.warning(e);
                    setResult(e);
                }
            } else {
                logger.severe("Failure while invoking " + operationToString(), cause);
                setResult(cause);
            }
        }

        abstract String operationToString();

        boolean isRetryable(Throwable cause) {
            return cause instanceof NotLeaderException
                    || cause instanceof LeaderDemotedException
                    || cause instanceof MemberLeftException
                    || cause instanceof CallerNotMemberException
                    || cause instanceof TargetNotMemberException;
        }

        final void scheduleRetry() {
            // TODO: needs a back-off strategy
            nodeEngine.getExecutionService().schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        invoke();
                    } catch (Throwable e) {
                        logger.warning(e);
                        setResult(e);
                    }
                }
            }, MAX_RETRY_DELAY, TimeUnit.MILLISECONDS);
        }

        final void invoke() {
            O operation = createOp();
            RaftEndpoint target = getTarget();
            if (target == null) {
                scheduleRetry();
                return;
            }

            InternalCompletableFuture<T> future = nodeEngine.getOperationService()
                    .invokeOnTarget(RaftService.SERVICE_NAME, operation.setPartitionId(partitionId), target.getAddress());
            afterInvoke(target);
            future.andThen(this, CALLER_RUNS_EXECUTOR);
        }

        abstract O createOp();

        void afterInvoke(RaftEndpoint target) {
        }

        private RaftEndpoint getTarget() {
            RaftEndpoint target = getKnownLeader(groupId);
            if (target != null) {
                return target;
            }

            EndpointCursor cursor = endpointCursor;
            if (cursor == null || !cursor.advance()) {
                cursor = newEndpointCursor();
                cursor.advance();
                endpointCursor = cursor;
            }
            return cursor.get();
        }

        private EndpointCursor newEndpointCursor() {
            RaftGroupInfo raftGroupInfo = raftService.getRaftGroupInfo(groupId);
            RaftEndpoint[] endpoints = raftGroupInfo != null ? raftGroupInfo.membersArray() : allEndpoints;
            return new EndpointCursor(endpoints);
        }
    }

    private class RaftInvocationFuture<T> extends AbstractRaftInvocationFuture<T, RaftReplicateOp> {

        private final RaftOperation raftOperation;
        private volatile RaftEndpoint lastInvocationEndpoint;

        RaftInvocationFuture(RaftGroupId groupId, RaftOperation raftOperation) {
            super(groupId);
            this.raftOperation = raftOperation;
        }

        @Override
        RaftReplicateOp createOp() {
            return new DefaultRaftReplicateOp(groupId, raftOperation);
        }

        @Override
        String operationToString() {
            return raftOperation.toString();
        }

        @Override
        public void onResponse(T response) {
            setKnownLeader(groupId, lastInvocationEndpoint);
            setResult(response);
        }

        @Override
        boolean isRetryable(Throwable cause) {
            if (failOnIndeterminateOperationState && cause instanceof MemberLeftException) {
                return false;
            }
            return super.isRetryable(cause);
        }

        @Override
        void afterInvoke(RaftEndpoint target) {
            lastInvocationEndpoint = target;
        }
    }

    private class RaftQueryInvocationFuture<T> extends AbstractRaftInvocationFuture<T, RaftQueryOp> {

        private final RaftOperation raftOperation;
        private volatile QueryPolicy queryPolicy;

        RaftQueryInvocationFuture(RaftGroupId groupId, RaftOperation raftOperation, QueryPolicy queryPolicy) {
            super(groupId);
            this.raftOperation = raftOperation;
            this.queryPolicy = queryPolicy;
        }

        @Override
        RaftQueryOp createOp() {
            RaftQueryOp op = new RaftQueryOp(groupId, raftOperation);
            op.setQueryPolicy(queryPolicy);
            return op;
        }

        @Override
        String operationToString() {
            return raftOperation.toString();
        }

        @Override
        boolean isRetryable(Throwable cause) {
            if (cause instanceof CannotRunLocalQueryException) {
                queryPolicy = QueryPolicy.LINEARIZABLE;
                return true;
            }

            return super.isRetryable(cause);
        }
    }

    private class ChangeRaftGroupMembershipInvocationFuture<T> extends AbstractRaftInvocationFuture<T, RaftReplicateOp> {

        private final long membersCommitIndex;
        private final RaftEndpoint endpoint;
        private final MembershipChangeType changeType;
        private volatile RaftEndpoint lastInvocationEndpoint;

        ChangeRaftGroupMembershipInvocationFuture(RaftGroupId groupId, long membersCommitIndex, RaftEndpoint endpoint,
                                                  MembershipChangeType changeType) {
            super(groupId);
            this.membersCommitIndex = membersCommitIndex;
            this.endpoint = endpoint;
            this.changeType = changeType;
        }

        @Override
        RaftReplicateOp createOp() {
            return new ChangeRaftGroupMembershipOp(groupId, membersCommitIndex, endpoint, changeType);
        }

        @Override
        String operationToString() {
            return "MembershipChange{groupId=" + groupId + ", membersCommitIndex=" + membersCommitIndex
                    + ", endpoint=" + endpoint + ", changeType=" + changeType + '}';
        }

        @Override
        public void onResponse(T response) {
            setKnownLeader(groupId, lastInvocationEndpoint);
            setResult(response);
        }

        @Override
        boolean isRetryable(Throwable cause) {
            if (failOnIndeterminateOperationState && cause instanceof MemberLeftException) {
                return false;
            }
            return super.isRetryable(cause);
        }

        @Override
        void afterInvoke(RaftEndpoint target) {
            lastInvocationEndpoint = target;
        }
    }

    private static class EndpointCursor {
        private final RaftEndpoint[] endpoints;
        private int index = -1;

        private EndpointCursor(RaftEndpoint[] endpoints) {
            this.endpoints = endpoints;
        }

        boolean advance() {
            return ++index < endpoints.length;
        }

        RaftEndpoint get() {
            return endpoints[index];
        }
    }

    private static class CallerRunsExecutor implements Executor {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
