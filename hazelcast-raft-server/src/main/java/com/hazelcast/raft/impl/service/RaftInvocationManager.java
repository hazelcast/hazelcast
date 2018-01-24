package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.raft.exception.CannotRunLocalQueryException;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveEndpointsOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.proxy.ChangeRaftGroupMembershipOp;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftReplicateOp;
import com.hazelcast.raft.impl.service.proxy.RaftQueryOp;
import com.hazelcast.raft.impl.service.proxy.RaftReplicateOp;
import com.hazelcast.raft.impl.service.proxy.TerminateRaftGroupOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
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
    private final ConcurrentMap<RaftGroupId, RaftEndpointImpl> knownLeaders = new ConcurrentHashMap<RaftGroupId, RaftEndpointImpl>();
    private final RaftEndpointImpl[] allEndpoints;
    private final boolean failOnIndeterminateOperationState;

    RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService, RaftConfig config) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.allEndpoints = raftService.getMetadataManager().getAllEndpoints().toArray(new RaftEndpointImpl[0]);
        this.failOnIndeterminateOperationState = config.isFailOnIndeterminateOperationState();
    }

    public void init() {
    }

    public void reset() {
        knownLeaders.clear();
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String groupNameRef) {
        RaftGroupConfig groupConfig = raftService.getConfig().getGroupConfig(groupNameRef);
        if (groupConfig == null) {
            throw new IllegalArgumentException("No RaftGroupConfig found with name '" + groupNameRef + "'.");
        }
        return createRaftGroup(groupConfig);
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(RaftGroupConfig groupConfig) {
        return createRaftGroup(groupConfig.getName(), groupConfig.getSize());
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String groupName, int groupSize) {
        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        ILogger logger = nodeEngine.getLogger(getClass());
        SimpleCompletableFuture<RaftGroupId> resultFuture = new SimpleCompletableFuture<RaftGroupId>(executor, logger);
        invokeGetEndpointsToCreateRaftGroup(groupName, groupSize, resultFuture);
        return resultFuture;
    }

    private void invokeGetEndpointsToCreateRaftGroup(final String groupName, final int groupSize,
                                                     final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<List<RaftEndpointImpl>> f = query(METADATA_GROUP_ID, new GetActiveEndpointsOp(), QueryPolicy.LEADER_LOCAL);

        f.andThen(new ExecutionCallback<List<RaftEndpointImpl>>() {
            @Override
            public void onResponse(List<RaftEndpointImpl> endpoints) {
                endpoints = new ArrayList<RaftEndpointImpl>(endpoints);

                if (endpoints.size() < groupSize) {
                    Exception result = new IllegalArgumentException("There are not enough active endpoints to create raft group "
                            + groupName + ". Active endpoints: " + endpoints.size() + ", Requested count: " + groupSize);
                    resultFuture.setResult(result);
                    return;
                }

                Collections.shuffle(endpoints);
                endpoints = endpoints.subList(0, groupSize);
                invokeCreateRaftGroup(groupName, groupSize, endpoints, resultFuture);
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.setResult(t);
            }
        });
    }

    private void invokeCreateRaftGroup(final String groupName, final int groupSize,
                                       final List<RaftEndpointImpl> endpoints,
                                       final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<RaftGroupId> f = invoke(METADATA_GROUP_ID, new CreateRaftGroupOp(groupName, endpoints));

        f.andThen(new ExecutionCallback<RaftGroupId>() {
            @Override
            public void onResponse(RaftGroupId groupId) {
                resultFuture.setResult(groupId);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t.getCause() instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create raft group: " + groupName + " with endpoints: " + endpoints,
                            t.getCause());
                    invokeGetEndpointsToCreateRaftGroup(groupName, groupSize, resultFuture);
                    return;
                }

                resultFuture.setResult(t);
            }
        });
    }

    public ICompletableFuture<RaftGroupId> triggerDestroyRaftGroup(final RaftGroupId groupId) {
        return invoke(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOp(groupId));
    }

    <T> ICompletableFuture<T> changeRaftGroupMembership(RaftGroupId groupId, RaftEndpointImpl endpoint,
                                                        MembershipChangeType changeType) {
        ChangeRaftGroupMembershipInvocationFuture<T> invocationFuture =
                new ChangeRaftGroupMembershipInvocationFuture<T>(groupId, NAN_MEMBERS_COMMIT_INDEX, endpoint, changeType);
        invocationFuture.invoke();
        return invocationFuture;
    }

    <T> ICompletableFuture<T> changeRaftGroupMembership(RaftGroupId groupId, long membersCommitIndex, RaftEndpointImpl endpoint,
                                                        MembershipChangeType changeType) {
        ChangeRaftGroupMembershipInvocationFuture<T> invocationFuture =
                new ChangeRaftGroupMembershipInvocationFuture<T>(groupId, membersCommitIndex, endpoint, changeType);
        invocationFuture.invoke();
        return invocationFuture;
    }

    public <T> ICompletableFuture<T> invoke(RaftGroupId groupId, RaftOp raftOp) {
        RaftInvocationFuture<T> invocationFuture = new RaftInvocationFuture<T>(groupId, raftOp);
        invocationFuture.invoke();
        return invocationFuture;
    }

    public <T> ICompletableFuture<T> query(RaftGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        RaftQueryInvocationFuture<T> invocationFuture = new RaftQueryInvocationFuture<T>(groupId, raftOp, queryPolicy);
        invocationFuture.invoke();
        return invocationFuture;
    }

    public <T> ICompletableFuture<T> queryOnLocal(RaftGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        RaftQueryOp queryOperation = new RaftQueryOp(groupId, raftOp);
        return nodeEngine.getOperationService().invokeOnTarget(RaftService.SERVICE_NAME,
                queryOperation.setQueryPolicy(queryPolicy), nodeEngine.getThisAddress());
    }

    public ICompletableFuture<Object> terminate(RaftGroupId groupId) {
        TerminateRaftGroupInvocationFuture invocationFuture = new TerminateRaftGroupInvocationFuture(groupId);
        invocationFuture.invoke();
        return invocationFuture;
    }

    private void resetKnownLeader(RaftGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    private void setKnownLeader(RaftGroupId groupId, RaftEndpointImpl leader) {
        logger.fine("Setting known leader for raft: " + groupId + " to " + leader);
        knownLeaders.put(groupId, leader);
    }

    private RaftEndpointImpl getKnownLeader(RaftGroupId groupId) {
        return knownLeaders.get(groupId);
    }

    private void updateKnownLeaderOnFailure(RaftGroupId groupId, Throwable cause) {
        if (cause instanceof RaftException) {
            RaftException e = (RaftException) cause;
            RaftEndpointImpl leader = (RaftEndpointImpl) e.getLeader();
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
            RaftEndpointImpl target = getTarget();
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

        void afterInvoke(RaftEndpointImpl target) {
        }

        private RaftEndpointImpl getTarget() {
            RaftEndpointImpl target = getKnownLeader(groupId);
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
            RaftEndpointImpl[] endpoints = raftGroupInfo != null ? raftGroupInfo.membersArray() : allEndpoints;
            return new EndpointCursor(endpoints);
        }
    }

    private class RaftInvocationFuture<T> extends AbstractRaftInvocationFuture<T, RaftReplicateOp> {

        private final RaftOp raftOp;
        private volatile RaftEndpointImpl lastInvocationEndpoint;

        RaftInvocationFuture(RaftGroupId groupId, RaftOp raftOp) {
            super(groupId);
            this.raftOp = raftOp;
        }

        @Override
        RaftReplicateOp createOp() {
            return new DefaultRaftReplicateOp(groupId, raftOp);
        }

        @Override
        String operationToString() {
            return raftOp.toString();
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
        void afterInvoke(RaftEndpointImpl target) {
            lastInvocationEndpoint = target;
        }
    }

    private class RaftQueryInvocationFuture<T> extends AbstractRaftInvocationFuture<T, RaftQueryOp> {

        private final RaftOp raftOp;
        private volatile QueryPolicy queryPolicy;

        RaftQueryInvocationFuture(RaftGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
            super(groupId);
            this.raftOp = raftOp;
            this.queryPolicy = queryPolicy;
        }

        @Override
        RaftQueryOp createOp() {
            RaftQueryOp op = new RaftQueryOp(groupId, raftOp);
            op.setQueryPolicy(queryPolicy);
            return op;
        }

        @Override
        String operationToString() {
            return raftOp.toString();
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
        private final RaftEndpointImpl endpoint;
        private final MembershipChangeType changeType;
        private volatile RaftEndpointImpl lastInvocationEndpoint;

        ChangeRaftGroupMembershipInvocationFuture(RaftGroupId groupId, long membersCommitIndex, RaftEndpointImpl endpoint,
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
        void afterInvoke(RaftEndpointImpl target) {
            lastInvocationEndpoint = target;
        }
    }

    private class TerminateRaftGroupInvocationFuture extends AbstractRaftInvocationFuture<Object, RaftReplicateOp> {

        private volatile RaftEndpointImpl lastInvocationEndpoint;

        TerminateRaftGroupInvocationFuture(RaftGroupId groupId) {
            super(groupId);
        }

        @Override
        RaftReplicateOp createOp() {
            return new TerminateRaftGroupOp(groupId);
        }

        @Override
        String operationToString() {
            return "TerminateRaftGroup{groupId=" + groupId + '}';
        }

        @Override
        public void onResponse(Object response) {
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
        void afterInvoke(RaftEndpointImpl target) {
            lastInvocationEndpoint = target;
        }
    }


    private static class EndpointCursor {
        private final RaftEndpointImpl[] endpoints;
        private int index = -1;

        private EndpointCursor(RaftEndpointImpl[] endpoints) {
            this.endpoints = endpoints;
        }

        boolean advance() {
            return ++index < endpoints.length;
        }

        RaftEndpointImpl get() {
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
