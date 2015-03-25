package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionTracingService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TraceableOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.operations.IsStillExecutingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.TraceableIsStillExecutingOperation;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Checks if an Operation is still running. This is useful when an operation isn't executed (e.g. got lost due to an
 * OOME on the remote side) and no response is ever send.
 *
 * The behavior needs to be cleaned up.
 *
 * Currently we only detect if an operation is actually running, not if it is in a queue somewhere. So it can mean that you
 * assume an operation isn't running, even though it is queued. Another issue is that the actual IsStillRunningOperation
 * isn't executed quick enough and then the system still decides that the operation isn't running.
 *
 * Instead of letting every invocation ask the remote system if an operation is still running, let the remote system
 * send a batch of all the call-id's for a particular member of the operations that are running.
 */
public class IsStillRunningService {

    private static final int IS_EXECUTING_CALL_TIMEOUT = 5000;

    private final ILogger logger;
    //private final OperationServiceImpl operationService;
    private final NodeEngineImpl nodeEngine;
    private final OperationExecutor operationExecutor;

    public IsStillRunningService(OperationExecutor operationExecutor, NodeEngineImpl nodeEngine, ILogger logger) {
        this.operationExecutor = operationExecutor;
        this.logger = logger;
        this.nodeEngine = nodeEngine;
    }

    public boolean isOperationExecuting(Invocation invocation) {
        // ask if op is still being executed?
        Boolean executing = Boolean.FALSE;
        try {
            Operation isStillExecuting = createCheckOperation(invocation);

            Invocation inv = new TargetInvocation(
                    invocation.nodeEngine, invocation.serviceName, isStillExecuting,
                    invocation.getTarget(), 0, 0, IS_EXECUTING_CALL_TIMEOUT, null, true);
            Future f = inv.invoke();
            invocation.logger.warning("Asking if operation execution has been started: " + toString());
            executing = (Boolean) invocation.nodeEngine.toObject(f.get(IS_EXECUTING_CALL_TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            invocation.logger.warning("While asking 'is-executing': " + toString(), e);
        }
        invocation.logger.warning("'is-executing': " + executing + " -> " + toString());
        return executing;
    }

    private Operation createCheckOperation(Invocation invocation) {
        Operation op = invocation.op;
        if (op instanceof TraceableOperation) {
            TraceableOperation traceable = (TraceableOperation) op;
            return new TraceableIsStillExecutingOperation(invocation.serviceName, traceable.getTraceIdentifier());
        } else {
            return new IsStillExecutingOperation(op.getCallId(), op.getPartitionId());
        }
    }

    public boolean isOperationExecuting(Address callerAddress, String callerUuid, String serviceName, Object identifier) {
        Object service = nodeEngine.getService(serviceName);
        if (service == null) {
            logger.severe("Not able to find operation execution info. Invalid service: " + serviceName);
            return false;
        }
        if (service instanceof ExecutionTracingService) {
            return ((ExecutionTracingService) service).isOperationExecuting(callerAddress, callerUuid, identifier);
        }
        logger.severe("Not able to find operation execution info. Invalid service: " + service);
        return false;
    }

    /**
     * Checks if an operation is still running.
     * <p/>
     * If the partition id is set, then it is super cheap since it just involves some volatiles reads since the right worker
     * thread can be found and in the worker-thread the current operation is stored in a volatile field.
     * <p/>
     * If the partition id isn't set, then we iterate over all generic-operationthread and check if one of them is running
     * the given operation. So this is a more expensive, but in most cases this should not be an issue since most of the data
     * is hot in cache.
     */
    public boolean isOperationExecuting(Address callerAddress, int partitionId, long operationCallId) {
        if (partitionId < 0) {
            return isGenericOperationExecuting(callerAddress, operationCallId);
        } else {
            return isPartitionSpecificOperationExecuting(callerAddress, partitionId, operationCallId);
        }
    }

    private boolean isPartitionSpecificOperationExecuting(Address callerAddress, int partitionId, long operationCallId) {
        OperationRunner[] partitionOperationRunners = operationExecutor.getPartitionOperationRunners();
        OperationRunner operationRunner = partitionOperationRunners[partitionId];
        Object task = operationRunner.currentTask();
        if (!(task instanceof Operation)) {
            return false;
        }
        Operation op = (Operation) task;
        return matches(op, callerAddress, operationCallId);
    }

    private boolean isGenericOperationExecuting(Address callerAddress, long operationCallId) {
        OperationRunner[] genericOperationRunners = operationExecutor.getGenericOperationRunners();
        for (OperationRunner genericOperationRunner : genericOperationRunners) {
            Object task = genericOperationRunner.currentTask();
            if (!(task instanceof Operation)) {
                continue;
            }
            Operation op = (Operation) task;
            if (matches(op, callerAddress, operationCallId)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(Operation op, Address callerAddress, long operationCallId) {
        if (op == null) {
            return false;
        }

        if (op.getCallId() != operationCallId) {
            return false;
        }

        if (!op.getCallerAddress().equals(callerAddress)) {
            return false;
        }

        return true;
    }
}
