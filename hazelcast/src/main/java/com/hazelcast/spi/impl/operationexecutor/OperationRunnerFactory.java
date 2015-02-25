package com.hazelcast.spi.impl.operationexecutor;

/**
 * A Factory for creating {@link OperationRunner} instances.
 */
public interface OperationRunnerFactory {

    /**
     * Creates an OperationRunner for a given partition. This OperationRunner will only execute generic operations
     * and operations for that given partition.
     *
     * @param partitionId the id of the partition.
     * @return the created OperationRunner.
     */
    OperationRunner createPartitionRunner(int partitionId);

    /**
     * Creates an OperationRunner to execute generic Operations.
     *
     * @return the created OperationRunner
     */
    OperationRunner createGenericRunner();

    /**
     * Creates an ad hoc generic OperationRunner.
     *
     * Why do we need ad-hoc operation runners; why not use the generic ones? The problem is that within Operations
     * can be executed directly on the calling thread and totally bypassing the generic threads. The problem is that
     * for these kinds of 'ad hoc' executions, you need to have a OperationRunner.
     *
     * The ad hoc OperationRunner can be used for these ad hoc executions. It is immutable and won't expose the
     * {@link OperationRunner#currentTask()}. Therefor it is save to be shared between threads without running
     * into problems.
     *
     * @return the created ad hoc OperationRunner.
     * @see com.hazelcast.spi.OperationService#runOperationOnCallingThread(com.hazelcast.spi.Operation)
     */
    OperationRunner createAdHocRunner();
}
