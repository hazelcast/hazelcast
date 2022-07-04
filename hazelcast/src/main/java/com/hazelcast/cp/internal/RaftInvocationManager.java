/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.operation.ChangeRaftGroupMembershipOp;
import com.hazelcast.cp.internal.operation.DefaultRaftReplicateOp;
import com.hazelcast.cp.internal.operation.DestroyRaftGroupOp;
import com.hazelcast.cp.internal.operation.RaftQueryOp;
import com.hazelcast.cp.internal.operation.unsafe.AbstractUnsafeRaftOp;
import com.hazelcast.cp.internal.operation.unsafe.UnsafeRaftQueryOp;
import com.hazelcast.cp.internal.operation.unsafe.UnsafeRaftReplicateOp;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftNodeOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupIdsOp;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocation;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.cp.internal.RaftService.CP_SUBSYSTEM_EXECUTOR;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.spi.impl.operationservice.InvocationBuilder.DEFAULT_DESERIALIZE_RESULT;
import static java.util.Collections.shuffle;

/**
 * Performs invocations to create &amp; destroy Raft groups,
 * commits {@link RaftOp} and runs queries on Raft groups.
 */
@SuppressWarnings({"unchecked", "checkstyle:classdataabstractioncoupling"})
public class RaftInvocationManager {

    private static final int RANDOM_RAFT_GROUP_INDEX_RANGE = 10000;

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
        this.invocationMaxRetryCount = nodeEngine.getProperties().getInteger(ClusterProperty.INVOCATION_MAX_RETRY_COUNT);
        this.invocationRetryPauseMillis = nodeEngine.getProperties().getMillis(ClusterProperty.INVOCATION_RETRY_PAUSE);
        this.operationCallTimeout = nodeEngine.getProperties().getMillis(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS);
        this.cpSubsystemEnabled = raftService.isCpSubsystemEnabled();
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

        InternalCompletableFuture<RaftGroupId> resultFuture = new InternalCompletableFuture<>();
        invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
        return resultFuture;
    }

    private <V> InternalCompletableFuture<V> completeExceptionallyIfCPSubsystemNotAvailable() {
        if (!cpSubsystemEnabled) {
            InternalCompletableFuture<V> future = new InternalCompletableFuture<>();
            future.completeExceptionally(new HazelcastException("CP Subsystem is not enabled!"));
            return future;
        }
        return null;
    }

    private void invokeGetMembersToCreateRaftGroup(String groupName, int groupSize,
                                                   InternalCompletableFuture<RaftGroupId> resultFuture) {
        RaftGroupId metadataGroupId = raftService.getMetadataGroupId();
        InternalCompletableFuture<List<CPMemberInfo>> f1 = query(metadataGroupId, new GetActiveCPMembersOp(), LINEARIZABLE);
        InternalCompletableFuture<Collection<RaftGroupId>> f2 = query(metadataGroupId, new GetRaftGroupIdsOp(), LINEARIZABLE);

        f1.thenCombineAsync(f2, (cpMembers, groupIds) -> {
            if (cpMembers.size() < groupSize) {
                Exception e = new IllegalArgumentException("There are not enough active CP members to create CP group "
                        + groupName + ". Active CP members: " + cpMembers.size() + ", Requested CP group size: " + groupSize);
                resultFuture.completeExceptionally(e);
                return null;
            }

            long groupIndex = generateRandomGroupIndex(groupIds);
            List<RaftEndpoint> groupEndpoints = generateRandomGroupMembers(cpMembers, groupSize);
            invokeCreateRaftGroup(groupName, groupSize, groupIndex, groupEndpoints, resultFuture);
            return null;
        }).exceptionally(t -> {
            resultFuture.completeExceptionally(t);
            return null;
        });
    }

    private long generateRandomGroupIndex(Collection<RaftGroupId> groupIds) {
        long groupIndex = RandomPicker.getInt(RANDOM_RAFT_GROUP_INDEX_RANGE);
        if (groupIds.size() > 0) {
            groupIndex += groupIds.stream().mapToLong(RaftGroupId::getId).max().getAsLong();
        }

        return groupIndex;
    }

    private List<RaftEndpoint> generateRandomGroupMembers(List<CPMemberInfo> cpMembers, int groupSize) {
        cpMembers = new ArrayList<>(cpMembers);
        shuffle(cpMembers);

        return cpMembers.stream()
                        .sorted(new CPMemberReachabilityComparator())
                        .limit(groupSize)
                        .map(CPMemberInfo::toRaftEndpoint)
                        .collect(Collectors.toList());
    }

    private void invokeCreateRaftGroup(String groupName, int groupSize, long groupIndex, List<RaftEndpoint> members,
                                       InternalCompletableFuture<RaftGroupId> resultFuture) {
        InternalCompletableFuture<CPGroupSummary> f =
                invoke(raftService.getMetadataGroupId(), new CreateRaftGroupOp(groupName, members, groupIndex));

        f.whenCompleteAsync((group, t) -> {
            if (t == null) {
                resultFuture.complete((RaftGroupId) group.id());
                triggerRaftNodeCreation(group);
            } else {
                if (t instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create CP group: " + groupName + " with members: " + members
                                    + " and group index: " + groupIndex, t.getCause());
                    invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
                    return;
                }
                resultFuture.completeExceptionally(t);
            }
        });
    }

    void triggerRaftNodeCreation(CPGroupSummary group) {
        for (CPMember groupMember : group.members()) {
            if (groupMember.equals(raftService.getLocalCPMember())) {
                ExecutionService executionService = nodeEngine.getExecutionService();
                executionService.execute(CP_SUBSYSTEM_EXECUTOR,
                        () -> raftService.createRaftNode(group.id(), group.initialMembers()));
            } else {
                Operation op = new CreateRaftNodeOp(group.id(), group.initialMembers());
                OperationService operationService = nodeEngine.getOperationService();
                operationService.send(op, groupMember.getAddress());
            }
        }
    }

    <T> InternalCompletableFuture<T> changeMembership(CPGroupId groupId, long membersCommitIndex,
                                                      RaftEndpoint member, MembershipChangeMode membershipChangeMode) {
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
        return invoke(groupId, raftOp, DEFAULT_DESERIALIZE_RESULT);
    }

    public <T> InternalCompletableFuture<T> invoke(CPGroupId groupId, RaftOp raftOp, boolean deserializeResponse) {
        if (cpSubsystemEnabled) {
            Operation operation = new DefaultRaftReplicateOp(groupId, raftOp);
            Invocation invocation =
                    new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext, groupId, operation,
                            invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout, deserializeResponse);
            return invocation.invoke();
        }
        return invokeOnPartition(new UnsafeRaftReplicateOp(groupId, raftOp), deserializeResponse);
    }

    public <T> InternalCompletableFuture<T> invokeOnPartition(AbstractUnsafeRaftOp operation) {
        return invokeOnPartition(operation, DEFAULT_DESERIALIZE_RESULT);
    }

    public <T> InternalCompletableFuture<T> invokeOnPartition(AbstractUnsafeRaftOp operation, boolean deserializeResponse) {
        int partitionId = raftService.getCPGroupPartitionId(operation.getGroupId());
        return nodeEngine.getOperationService().createInvocationBuilder(operation.getServiceName(), operation, partitionId)
                .setCallTimeout(operationCallTimeout)
                .setTryCount(invocationMaxRetryCount)
                .setTryPauseMillis(invocationRetryPauseMillis)
                .setResultDeserialized(deserializeResponse).invoke();
    }

    public <T> InternalCompletableFuture<T> query(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        return query(groupId, raftOp, queryPolicy, DEFAULT_DESERIALIZE_RESULT);
    }

    public <T> InternalCompletableFuture<T> query(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy,
            boolean deserializeResponse) {
        if (cpSubsystemEnabled) {
            RaftQueryOp operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
            Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                    groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis,
                    operationCallTimeout, deserializeResponse);
            return invocation.invoke();
        }
        return invokeOnPartition(new UnsafeRaftQueryOp(groupId, raftOp), deserializeResponse);
    }

    public <T> InternalCompletableFuture<T> queryLocally(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        Operation operation;
        if (cpSubsystemEnabled) {
            operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
        } else {
            operation = new UnsafeRaftQueryOp(groupId, raftOp);
        }
        operation.setPartitionId(raftService.getCPGroupPartitionId(groupId));
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

    CPMember getCPMember(RaftEndpoint endpoint) {
        return endpoint != null ? raftInvocationContext.getCPMember(endpoint.getUuid()) : null;
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
