package com.hazelcast.raft.impl.service;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
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
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

/**
 * TODO: Javadoc Pending...
 *
 */
final class NodeEngineRaftIntegration implements RaftIntegration {

    private final NodeEngineImpl nodeEngine;
    private final RaftGroupId raftGroupId;
    private final InternalOperationService operationService;
    private final TaskScheduler taskScheduler;
    private final int partitionId;
    private final int threadId;

    NodeEngineRaftIntegration(NodeEngineImpl nodeEngine, RaftGroupId groupId) {
        this.nodeEngine = nodeEngine;
        this.raftGroupId = groupId;
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
    public ILogger getLogger(String name) {
        return nodeEngine.getLogger(name);
    }

    @Override
    public boolean isReady() {
        return nodeEngine.getClusterService().isJoined();
    }

    @Override
    public boolean isReachable(RaftEndpoint endpoint) {
        return nodeEngine.getClusterService().getMember(((RaftEndpointImpl) endpoint).getAddress()) != null;
    }

    @Override
    public boolean send(PreVoteRequest request, RaftEndpoint target) {
        return send(new PreVoteRequestOp(raftGroupId, request), target);
    }

    @Override
    public boolean send(PreVoteResponse response, RaftEndpoint target) {
        return send(new PreVoteResponseOp(raftGroupId, response), target);
    }

    @Override
    public boolean send(VoteRequest request, RaftEndpoint target) {
        return send(new VoteRequestOp(raftGroupId, request), target);
    }

    @Override
    public boolean send(VoteResponse response, RaftEndpoint target) {
        return send(new VoteResponseOp(raftGroupId, response), target);
    }

    @Override
    public boolean send(AppendRequest request, RaftEndpoint target) {
        return send(new AppendRequestOp(raftGroupId, request), target);
    }

    @Override
    public boolean send(AppendSuccessResponse response, RaftEndpoint target) {
        return send(new AppendSuccessResponseOp(raftGroupId, response), target);
    }

    @Override
    public boolean send(AppendFailureResponse response, RaftEndpoint target) {
        return send(new AppendFailureResponseOp(raftGroupId, response), target);
    }

    @Override
    public boolean send(InstallSnapshot request, RaftEndpoint target) {
        return send(new InstallSnapshotOp(raftGroupId, request), target);
    }

    @Override
    public Object runOperation(Object op, long commitIndex) {
        RaftOp operation = (RaftOp) op;
        operation.setCommitIndex(commitIndex).setNodeEngine(nodeEngine);
        OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
        try {
            operation.beforeRun();
            operation.run();
            operation.afterRun();
            return operation.getResponse();
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
                Object snapshot = service.takeSnapshot(raftGroupId, commitIndex);
                if (snapshot != null) {
                    snapshotOps.add(new RestoreSnapshotOp(serviceInfo.getName(), raftGroupId, commitIndex, snapshot));
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

    private boolean send(Operation operation, RaftEndpoint target) {
        return nodeEngine.getOperationService().send(operation.setPartitionId(partitionId), ((RaftEndpointImpl) target).getAddress());
    }
}
