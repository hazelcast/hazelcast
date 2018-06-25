/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveRaftMembersOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.proxy.ChangeRaftGroupMembershipOp;
import com.hazelcast.raft.impl.service.proxy.DefaultRaftReplicateOp;
import com.hazelcast.raft.impl.service.proxy.DestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.proxy.RaftQueryOp;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocation;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;

import static com.hazelcast.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.raft.impl.service.MetadataRaftGroupManager.METADATA_GROUP_ID;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

/**
 * Performs invocations to create & destroy Raft groups, commit {@link RaftOp} to Raft groups, and run queries on Raft groups.
 */
@SuppressWarnings("unchecked")
public class RaftInvocationManager {

    private final NodeEngineImpl nodeEngine;
    private final OperationServiceImpl operationService;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftInvocationContext raftInvocationContext;
    private final long operationCallTimeout;
    private final int invocationMaxRetryCount;
    private final long invocationRetryPauseMillis;

    RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.raftInvocationContext = new RaftInvocationContext(logger, raftService);
        this.invocationMaxRetryCount = nodeEngine.getProperties().getInteger(GroupProperty.INVOCATION_MAX_RETRY_COUNT);
        this.invocationRetryPauseMillis = nodeEngine.getProperties().getMillis(GroupProperty.INVOCATION_RETRY_PAUSE);
        this.operationCallTimeout = nodeEngine.getProperties().getMillis(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS);
    }

    void reset() {
        raftInvocationContext.reset();
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String groupName) {
        return createRaftGroup(groupName, raftService.getConfig().getGroupSize());
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String groupName, int groupSize) {
        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        ILogger logger = nodeEngine.getLogger(getClass());
        SimpleCompletableFuture<RaftGroupId> resultFuture = new SimpleCompletableFuture<RaftGroupId>(executor, logger);
        invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
        return resultFuture;
    }

    private void invokeGetMembersToCreateRaftGroup(final String groupName, final int groupSize,
                                                   final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<List<RaftMemberImpl>> f = query(METADATA_GROUP_ID, new GetActiveRaftMembersOp(), LEADER_LOCAL);

        f.andThen(new ExecutionCallback<List<RaftMemberImpl>>() {
            @Override
            public void onResponse(List<RaftMemberImpl> members) {
                members = new ArrayList<RaftMemberImpl>(members);

                if (members.size() < groupSize) {
                    Exception result = new IllegalArgumentException("There are not enough active members to create raft group "
                            + groupName + ". Active members: " + members.size() + ", Requested count: " + groupSize);
                    resultFuture.setResult(result);
                    return;
                }

                Collections.shuffle(members);
                Collections.sort(members, new RaftMemberReachabilityComparator());
                members = members.subList(0, groupSize);
                invokeCreateRaftGroup(groupName, groupSize, members, resultFuture);
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.setResult(t);
            }
        });
    }

    private void invokeCreateRaftGroup(final String groupName, final int groupSize,
                                       final List<RaftMemberImpl> members,
                                       final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<RaftGroupId> f = invoke(METADATA_GROUP_ID, new CreateRaftGroupOp(groupName, members));

        f.andThen(new ExecutionCallback<RaftGroupId>() {
            @Override
            public void onResponse(RaftGroupId groupId) {
                resultFuture.setResult(groupId);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t.getCause() instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create raft group: " + groupName + " with members: " + members,
                            t.getCause());
                    invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
                    return;
                }

                resultFuture.setResult(t);
            }
        });
    }

    // TODO [basri] this operation should be here or somewhere else?
    public InternalCompletableFuture<RaftGroupId> triggerDestroy(RaftGroupId groupId) {
        return invoke(METADATA_GROUP_ID, new TriggerDestroyRaftGroupOp(groupId));
    }

    <T> InternalCompletableFuture<T> changeMembership(RaftGroupId groupId, long membersCommitIndex,
                                                      RaftMemberImpl member, MembershipChangeType changeType) {
        Operation operation = new ChangeRaftGroupMembershipOp(groupId, membersCommitIndex, member, changeType);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext, groupId,
                operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public <T> InternalCompletableFuture<T> invoke(RaftGroupId groupId, RaftOp raftOp) {
        Operation operation = new DefaultRaftReplicateOp(groupId, raftOp);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public <T> InternalCompletableFuture<T> query(RaftGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        RaftQueryOp operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public <T> InternalCompletableFuture<T> queryOnLocal(RaftGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        RaftQueryOp operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
        return nodeEngine.getOperationService().invokeOnTarget(RaftService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
    }

    public InternalCompletableFuture<Object> destroy(RaftGroupId groupId) {
        Operation operation = new DestroyRaftGroupOp(groupId);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public RaftInvocationContext getRaftInvocationContext() {
        return raftInvocationContext;
    }

    private class RaftMemberReachabilityComparator implements Comparator<RaftMemberImpl> {
        final ClusterService clusterService = nodeEngine.getClusterService();

        @Override
        public int compare(RaftMemberImpl o1, RaftMemberImpl o2) {
            boolean b1 = clusterService.getMember(o1.getAddress()) != null;
            boolean b2 = clusterService.getMember(o2.getAddress()) != null;
            return b1 == b2 ? 0 : (b1 ? -1 : 1);
        }
    }
}
