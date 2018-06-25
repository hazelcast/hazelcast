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

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.service.operation.NotifyTermChangeOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendFailureResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.AppendSuccessResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.InstallSnapshotOp;
import com.hazelcast.raft.impl.service.operation.integration.PreVoteRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.PreVoteResponseOp;
import com.hazelcast.raft.impl.service.operation.integration.VoteRequestOp;
import com.hazelcast.raft.impl.service.operation.integration.VoteResponseOp;
import com.hazelcast.raft.impl.service.operation.snapshot.RestoreSnapshotOp;
import com.hazelcast.raft.impl.util.PartitionSpecificRunnableAdaptor;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftNodeStatus.TERMINATED;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

/**
 * The integration point of the Raft algorithm implementation and Hazelcast system
 * Replicates Raft RPCs via Hazelcast operations and executes committed Raft operations.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
final class NodeEngineRaftIntegration implements RaftIntegration {

    private final NodeEngineImpl nodeEngine;
    private final RaftGroupId groupId;
    private final InternalOperationService operationService;
    private final TaskScheduler taskScheduler;
    private final int partitionId;
    private final int threadId;

    NodeEngineRaftIntegration(NodeEngineImpl nodeEngine, RaftGroupId groupId) {
        this.nodeEngine = nodeEngine;
        this.groupId = groupId;
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        this.operationService = operationService;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(groupId);
        OperationExecutorImpl operationExecutor = (OperationExecutorImpl) operationService.getOperationExecutor();
        this.threadId = operationExecutor.toPartitionThreadIndex(partitionId);
        this.taskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
    }

    @Override
    public void execute(Runnable task) {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof PartitionOperationThread
                && ((PartitionOperationThread) currentThread).getThreadId() == threadId) {
            task.run();
        } else {
            operationService.execute(new PartitionSpecificRunnableAdaptor(task, partitionId));
        }
    }

    @Override
    public void schedule(final Runnable task, long delay, TimeUnit timeUnit) {
        taskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                execute(task);
            }
        }, delay, timeUnit);
    }

    @Override
    public SimpleCompletableFuture newCompletableFuture() {
        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        return new SimpleCompletableFuture(executor, nodeEngine.getLogger(getClass()));
    }

    @Override
    public Object getAppendedEntryOnLeaderElection() {
        return new NotifyTermChangeOp();
    }

    @Override
    public ILogger getLogger(String name) {
        return nodeEngine.getLogger(name);
    }

    @Override
    public boolean isReady() {
        return nodeEngine.getClusterService().isJoined();
    }

    @Override
    public boolean isReachable(RaftMember member) {
        return nodeEngine.getClusterService().getMember(((RaftMemberImpl) member).getAddress()) != null;
    }

    @Override
    public boolean send(PreVoteRequest request, RaftMember target) {
        return send(new PreVoteRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(PreVoteResponse response, RaftMember target) {
        return send(new PreVoteResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(VoteRequest request, RaftMember target) {
        return send(new VoteRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(VoteResponse response, RaftMember target) {
        return send(new VoteResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(AppendRequest request, RaftMember target) {
        return send(new AppendRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(AppendSuccessResponse response, RaftMember target) {
        return send(new AppendSuccessResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(AppendFailureResponse response, RaftMember target) {
        return send(new AppendFailureResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(InstallSnapshot request, RaftMember target) {
        return send(new InstallSnapshotOp(groupId, request), target);
    }

    @Override
    public Object runOperation(Object op, long commitIndex) {
        RaftOp operation = (RaftOp) op;
        operation.setNodeEngine(nodeEngine);
        try {
            return operation.run(groupId, commitIndex);
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public Object takeSnapshot(long commitIndex) {
        try {
            List<RestoreSnapshotOp> snapshotOps = new ArrayList<RestoreSnapshotOp>();
            for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(SnapshotAwareService.class)) {
                SnapshotAwareService service = serviceInfo.getService();
                Object snapshot = service.takeSnapshot(groupId, commitIndex);
                if (snapshot != null) {
                    snapshotOps.add(new RestoreSnapshotOp(serviceInfo.getName(), snapshot));
                }
            }

            return snapshotOps;
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public void restoreSnapshot(Object op, long commitIndex) {
        ILogger logger = nodeEngine.getLogger(this.getClass());
        List<RestoreSnapshotOp> snapshotOps = (List<RestoreSnapshotOp>) op;
        for (RestoreSnapshotOp snapshotOp : snapshotOps) {
            Object result = runOperation(snapshotOp, commitIndex);
            if (result instanceof Throwable) {
                logger.severe("Restore of " + snapshotOp + " failed...", (Throwable) result);
            }
        }
    }

    private boolean send(Operation operation, RaftMember target) {
        return operationService.send(operation.setPartitionId(partitionId), ((RaftMemberImpl) target).getAddress());
    }

    @Override
    public void onNodeStatusChange(RaftNodeStatus status) {
        if (status == TERMINATED) {
            Collection<RaftGroupLifecycleAwareService> services = nodeEngine.getServices(RaftGroupLifecycleAwareService.class);
            for (RaftGroupLifecycleAwareService service : services) {
                service.onGroupDestroy(groupId);
            }
        }
    }
}
