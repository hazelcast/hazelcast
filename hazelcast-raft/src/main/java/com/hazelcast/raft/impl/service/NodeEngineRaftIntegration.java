package com.hazelcast.raft.impl.service;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.operation.AppendFailureResponseOp;
import com.hazelcast.raft.impl.operation.AppendRequestOp;
import com.hazelcast.raft.impl.operation.AppendSuccessResponseOp;
import com.hazelcast.raft.impl.operation.VoteRequestOp;
import com.hazelcast.raft.impl.operation.VoteResponseOp;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.TaskScheduler;

import java.util.concurrent.Executor;

import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;

/**
 * TODO: Javadoc Pending...
 *
 */
final class NodeEngineRaftIntegration implements RaftIntegration {

    private final NodeEngine nodeEngine;

    private final String raftName;

    NodeEngineRaftIntegration(NodeEngine nodeEngine, String raftName) {
        this.nodeEngine = nodeEngine;
        this.raftName = raftName;
    }

    @Override
    public TaskScheduler getTaskScheduler() {
        return nodeEngine.getExecutionService().getGlobalTaskScheduler();
    }

    @Override
    public Executor getExecutor() {
        return nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
    }

    @Override
    public ILogger getLogger(String name) {
        return nodeEngine.getLogger(name);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return nodeEngine.getLogger(clazz);
    }

    @Override
    public boolean isJoined() {
        return nodeEngine.getClusterService().isJoined();
    }

    @Override
    public boolean isReachable(RaftEndpoint endpoint) {
        return nodeEngine.getClusterService().getMember(endpoint.getAddress()) != null;
    }

    @Override
    public boolean send(VoteRequest request, RaftEndpoint target) {
        return send(new VoteRequestOp(raftName, request), target);
    }

    @Override
    public boolean send(VoteResponse response, RaftEndpoint target) {
        return send(new VoteResponseOp(raftName, response), target);
    }

    @Override
    public boolean send(AppendRequest request, RaftEndpoint target) {
        return send(new AppendRequestOp(raftName, request), target);
    }

    @Override
    public boolean send(AppendSuccessResponse response, RaftEndpoint target) {
        return send(new AppendSuccessResponseOp(raftName, response), target);
    }

    @Override
    public boolean send(AppendFailureResponse response, RaftEndpoint target) {
        return send(new AppendFailureResponseOp(raftName, response), target);
    }

    @Override
    public Object runOperation(RaftOperation operation, int commitIndex) {
        operation.setCommitIndex(commitIndex).setNodeEngine(nodeEngine);
        // TODO do we need this ???
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

    private boolean send(Operation operation, RaftEndpoint target) {
        return nodeEngine.getOperationService().send(operation, target.getAddress());
    }
}
