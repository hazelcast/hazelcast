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

package com.hazelcast.cp.internal;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.operation.ChangeRaftGroupMembershipOp;
import com.hazelcast.cp.internal.operation.DefaultRaftReplicateOp;
import com.hazelcast.cp.internal.operation.DestroyRaftGroupOp;
import com.hazelcast.cp.internal.operation.RaftQueryOp;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.internal.util.SimpleCompletedFuture;
import com.hazelcast.logging.ILogger;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

/**
 * Performs invocations to create & destroy Raft groups,
 * commits {@link RaftOp} and runs queries on Raft groups.
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
    private final boolean cpSubsystemEnabled;

    RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.raftInvocationContext = new RaftInvocationContext(logger, raftService);
        this.invocationMaxRetryCount = nodeEngine.getProperties().getInteger(GroupProperty.INVOCATION_MAX_RETRY_COUNT);
        this.invocationRetryPauseMillis = nodeEngine.getProperties().getMillis(GroupProperty.INVOCATION_RETRY_PAUSE);
        this.operationCallTimeout = nodeEngine.getProperties().getMillis(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS);
        this.cpSubsystemEnabled = raftService.getConfig().getCPMemberCount() > 0;
    }

    void reset() {
        raftInvocationContext.reset();
    }

    public InternalCompletableFuture<RaftGroupId> createRaftGroup(String groupName) {
        return createRaftGroup(groupName, raftService.getConfig().getGroupSize());
    }

    public InternalCompletableFuture<RaftGroupId> createRaftGroup(String groupName, int groupSize) {
        InternalCompletableFuture<RaftGroupId> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }

        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        SimpleCompletableFuture<RaftGroupId> resultFuture = new SimpleCompletableFuture<RaftGroupId>(executor, logger);
        invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
        return resultFuture;
    }

    private <V> InternalCompletableFuture<V> completeExceptionallyIfCPSubsystemNotAvailable() {
        // RU_COMPAT_3_11
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_12)) {
            return new SimpleCompletedFuture<V>(
                    new UnsupportedOperationException("CP Subsystem is not available before version 3.12!"));
        }
        if (!cpSubsystemEnabled) {
            return new SimpleCompletedFuture<V>(new HazelcastException("CP Subsystem is not enabled!"));
        }
        return null;
    }

    private void invokeGetMembersToCreateRaftGroup(final String groupName, final int groupSize,
                                                   final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        RaftOp op = new GetActiveCPMembersOp();
        ICompletableFuture<List<CPMemberInfo>> f = query(raftService.getMetadataGroupId(), op, LEADER_LOCAL);

        f.andThen(new ExecutionCallback<List<CPMemberInfo>>() {
            @Override
            public void onResponse(List<CPMemberInfo> members) {
                members = new ArrayList<CPMemberInfo>(members);

                if (members.size() < groupSize) {
                    Exception result = new IllegalArgumentException("There are not enough active members to create CP group "
                            + groupName + ". Active members: " + members.size() + ", Requested count: " + groupSize);
                    resultFuture.setResult(result);
                    return;
                }

                Collections.shuffle(members);
                Collections.sort(members, new CPMemberReachabilityComparator());
                members = members.subList(0, groupSize);
                invokeCreateRaftGroup(groupName, groupSize, members, resultFuture);
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.setResult(new ExecutionException(t));
            }
        });
    }

    private void invokeCreateRaftGroup(final String groupName, final int groupSize,
                                       final List<CPMemberInfo> members,
                                       final SimpleCompletableFuture<RaftGroupId> resultFuture) {
        ICompletableFuture<RaftGroupId> f = invoke(raftService.getMetadataGroupId(), new CreateRaftGroupOp(groupName, members));

        f.andThen(new ExecutionCallback<RaftGroupId>() {
            @Override
            public void onResponse(RaftGroupId groupId) {
                resultFuture.setResult(groupId);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create CP group: " + groupName + " with members: " + members,
                            t.getCause());
                    invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
                    return;
                }

                resultFuture.setResult(t);
            }
        });
    }

    <T> InternalCompletableFuture<T> changeMembership(CPGroupId groupId, long membersCommitIndex,
                                                      CPMemberInfo member, MembershipChangeMode membershipChangeMode) {
        InternalCompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        Operation operation = new ChangeRaftGroupMembershipOp(groupId, membersCommitIndex, member, membershipChangeMode);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext, groupId,
                operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public <T> InternalCompletableFuture<T> invoke(CPGroupId groupId, RaftOp raftOp) {
        InternalCompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        Operation operation = new DefaultRaftReplicateOp(groupId, raftOp);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public <T> InternalCompletableFuture<T> query(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        InternalCompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        RaftQueryOp operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public <T> InternalCompletableFuture<T> queryLocally(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        InternalCompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        RaftQueryOp operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
        return nodeEngine.getOperationService().invokeOnTarget(RaftService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
    }

    public InternalCompletableFuture<Object> destroy(CPGroupId groupId) {
        InternalCompletableFuture<Object> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        Operation operation = new DestroyRaftGroupOp(groupId);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke();
    }

    public RaftInvocationContext getRaftInvocationContext() {
        return raftInvocationContext;
    }

    private class CPMemberReachabilityComparator implements Comparator<CPMemberInfo> {
        final ClusterService clusterService = nodeEngine.getClusterService();

        @Override
        public int compare(CPMemberInfo o1, CPMemberInfo o2) {
            boolean b1 = clusterService.getMember(o1.getAddress()) != null;
            boolean b2 = clusterService.getMember(o2.getAddress()) != null;
            return b1 == b2 ? 0 : (b1 ? -1 : 1);
        }
    }
}
