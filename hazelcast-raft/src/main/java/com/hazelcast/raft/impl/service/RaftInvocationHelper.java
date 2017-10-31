package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.LeaderDemotedException;
import com.hazelcast.raft.NotLeaderException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.proxy.RaftReplicatingOperation;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.Executor;

import static com.hazelcast.raft.impl.service.RaftService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 */
public final class RaftInvocationHelper {

    public static ICompletableFuture invokeOnLeader(NodeEngine nodeEngine,
            Supplier<RaftReplicatingOperation> operationSupplier, String raftName) {

        RaftService raftService = nodeEngine.getService(SERVICE_NAME);
        RaftNode raftNode = raftService.getRaftNode(raftName);
        if (raftNode == null) {
            throw new IllegalArgumentException(raftName + " raft group does not exist!");
        }

        ILogger logger = raftNode.getLogger(RaftInvocationHelper.class);
        OperationService operationService = nodeEngine.getOperationService();
        Executor executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);

        RaftInvocationFuture invocationFuture = new RaftInvocationFuture(raftNode, operationService, operationSupplier,
                executor, logger);
        invocationFuture.invoke();
        return invocationFuture;
    }

    private static class RaftInvocationFuture extends AbstractCompletableFuture implements ExecutionCallback {

        private final Supplier<RaftReplicatingOperation> operationSupplier;
        private final OperationService operationService;
        private final RaftNode raftNode;
        private final ILogger logger;

        RaftInvocationFuture(RaftNode raftNode, OperationService operationService,
                Supplier<RaftReplicatingOperation> operationSupplier, Executor executor, ILogger logger) {
            super(executor, logger);
            this.raftNode = raftNode;
            this.operationService = operationService;
            this.operationSupplier = operationSupplier;
            this.logger = logger;
        }

        @Override
        public void onResponse(Object response) {
            setResult(response);
        }

        @Override
        public void onFailure(Throwable cause) {
            logger.warning(cause);
            if (cause instanceof NotLeaderException
                    || cause instanceof LeaderDemotedException
                    || cause instanceof MemberLeftException
                    || cause instanceof CallerNotMemberException
                    || cause instanceof TargetNotMemberException) {

                try {
                    // TODO: needs a back-off strategy
                    invoke();
                } catch (Exception e) {
                    logger.warning(e);
                    setResult(e);
                }
            } else {
                setResult(cause);
            }
        }

        void invoke() {
            RaftEndpoint leader = raftNode.getLeader();
            InternalCompletableFuture future =
                    operationService.invokeOnTarget(SERVICE_NAME, operationSupplier.get(), leader.getAddress());
            future.andThen(this);
        }
    }
}
