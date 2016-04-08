package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.TargetNotMemberException;

public class ShutdownRequestOperation extends AbstractOperation implements MigrationCycleOperation {

    public ShutdownRequestOperation() {
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        final ILogger logger = getLogger();
        final Address caller = getCallerAddress();

        final NodeEngine nodeEngine = getNodeEngine();
        final ClusterService clusterService = nodeEngine.getClusterService();

        if (clusterService.isMaster()) {
            if (clusterService.getMember(caller) != null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Received shutdown request from " + caller);
                }
                partitionService.onShutdownRequest(caller);
            } else {
                logger.warning("Ignoring shutdown request from " + caller + " because it is not a member");
            }
        } else {
            // TODO this may be ok during master changes so we can convert the log level into finest
            logger.warning("Received shutdown request from " + caller + " but this node is not master.");
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

}
