package com.hazelcast.spi.impl.operationexecutor;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

/**
 * The OperationExecutor is responsible for scheduling work (packets/operations) to be executed. It can be compared
 * to a {@link java.util.concurrent.Executor} with the big difference that it is designed for assigning packets,
 * operations and PartitionSpecificRunnable to a thread instead of only runnables.
 *
 * It depends on the implementation if an operation is executed on the calling thread or not. For example the
 * {@link com.hazelcast.spi.impl.operationexecutor.classic.ClassicOperationExecutor} will always offload a partition specific
 * Operation to the correct partition-operation-thread.
 *
 * The actual processing of a operation-packet, Operation, or a PartitionSpecificRunnable is forwarded to the
 * {@link OperationRunner}.
 *
 * In case of a response packet, the {@link ResponsePacketHandler} is used to handle the response.
 */
public interface OperationExecutor {

    // Will be replaced by the black-box
    @Deprecated
    int getRunningOperationCount();

    // Will be replaced by the black-box
    @Deprecated
    int getOperationExecutorQueueSize();

    // Will be replaced by the black-box
    @Deprecated
    int getPriorityOperationExecutorQueueSize();

    // Will be replaced by the black-box
    @Deprecated
    int getResponseQueueSize();

    // Will be replaced by the black-box
    @Deprecated
    int getPartitionOperationThreadCount();

    // Will be replaced by the black-box
    @Deprecated
    int getGenericOperationThreadCount();

    // Will be replaced by the black-box
    @Deprecated
    void dumpPerformanceMetrics(StringBuffer sb);

    /**
     * Gets all the operation handlers for the partitions. Each partition will have its own operation handler. So if
     * there are 271 partitions, then the size of the array will be 271.
     * <p/>
     * Don't modify the content of the array!
     *
     * @return the operation handlers.
     */
    OperationRunner[] getPartitionOperationRunners();

    /**
     * Gets all the generic operation handlers. The number of generic operation handlers depends on the number of
     * generic threads.
     * <p/>
     * Don't modify the content of the array!
     *
     * @return the generic operation handlers.
     */
    OperationRunner[] getGenericOperationRunners();

    /**
     * Executes an Operation.
     *
     * @param op the operation to execute.
     * @throws java.lang.NullPointerException if op is null.
     */
    void execute(Operation op);

    /**
     * Executes a PartitionSpecificRunnable.
     *
     * @param task the task the execute.
     * @throws java.lang.NullPointerException if task is null.
     */
    void execute(PartitionSpecificRunnable task);

    /**
     * Executes a Operation/Response-packet.
     *
     * @param packet the packet to execute.
     * @throws java.lang.NullPointerException if packet is null
     */
    void execute(Packet packet);

    /**
     * Runs the operation on the calling thread.
     *
     * @param op the operation to run.
     * @throws java.lang.NullPointerException if op is null.
     * @throws IllegalThreadStateException    if the operation is not allowed to be run on the calling thread.
     */
    void runOnCallingThread(Operation op);

    /**
     * Tries to run the operation on the calling thread if possible. Otherwise offload it to a different thread.
     *
     * @param op the operation to run.
     * @throws java.lang.NullPointerException if op is null.
     */
    void runOnCallingThreadIfPossible(Operation op);

    /**
     * Checks if the operation is allowed to run on the current thread.
     *
     * @param op the Operation to check
     * @return true if it is allowed, false otherwise.
     * @throws java.lang.NullPointerException if op is null.
     */
    @Deprecated
    boolean isAllowedToRunInCurrentThread(Operation op);

    /**
     * Checks if the current thread is an operation thread.
     *
     * @return true if is an operation thread, false otherwise.
     * @deprecated it should not matter if a thread is an operation thread or not; this is something operationExecutor specific.
     */
    boolean isOperationThread();

    /**
     * Checks this operation can be invoked from the current thread. Invoking means that the operation can
     * be executed on another thread, but that one is going to block for completion. Blocking for completion
     * can cause problems, e.g. when you hog a partition thread.
     *
     * @param op the Operation to check
     * @return true if allowed, false otherwise.
     */
    boolean isInvocationAllowedFromCurrentThread(Operation op);

    /**
     * Shuts down this OperationExecutor.
     */
    void shutdown();

}
