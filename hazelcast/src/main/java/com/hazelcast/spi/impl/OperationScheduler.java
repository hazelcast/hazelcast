package com.hazelcast.spi.impl;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;

/**
 * The OperationScheduler is responsible for scheduling work (packets/operations) to be executed. Scheduling in this
 * case means that a thread is looked up to execute the work. The thread could be some kind of pooled thread, but it
 * could also be the calling thread. This is something implementation specific.
 * <p/>
 * So in case of an Operation or a packet containing an operation, and OperationHandler is looked up and a thread (could
 * be the calling thread) and one of the process methods is called.
 * <p/>
 * In case of a response packet, the {@link ResponsePacketHandler} is used to handle the
 * response.
 */
public interface OperationScheduler {

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
    OperationHandler[] getPartitionOperationHandlers();

    /**
     * Gets all the generic operation handlers. The number of generic operation handlers depends on the number of
     * generic threads.
     * <p/>
     * Don't modify the content of the array!
     *
     * @return the generic operation handlers.
     */
    OperationHandler[] getGenericOperationHandlers();

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
    void runOperationOnCallingThread(Operation op);

    /**
     * Checks if the operation is allowed to run on the current thread.
     *
     * @param op the Operation to check
     * @return true if it is allowed, false otherwise.
     * @throws java.lang.NullPointerException if op is null.
     */
    boolean isAllowedToRunInCurrentThread(Operation op);

    /**
     * Checks if the current thread is an operation thread.
     *
     * @return true if is an operation thread, false otherwise.
     * @deprecated it should not matter if a thread is an operation thread or not; this is something scheduler specific.
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
     * Shuts down this OperationScheduler.
     */
    void shutdown();
}
