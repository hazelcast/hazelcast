package com.hazelcast.spi.impl.operationexecutor.progressive;

enum PartitionQueueState {

    /**
     * The initial headUnparked.
     * <p/>
     * There is no work and the partition-buffer is not scheduled at the work-buffer
     */
    Parked,

    /**
     * The partition buffer has pending work and is registered using an unpark at the schedule-buffer.
     *
     * None of the working has a priority.
     */
    Unparked,

    /**
     * Executing means that the partition-buffer currently is being executed by the partition-thread
     * (so the rightful owner of a partition).
     * <p/>
     * After executing, the partition-buffer will end back up in empty headUnparked.
     */
    Executing,

    /**
     * TODO:
     *
     * We need to take care of when an PriorityExecution is happening and a normal add or an runOrAdd is done, that
     * the state is switched to Executing.
     */
    ExecutingPriority,

    /**
     * Stolen means that the partition-buffer currently is being executed by a thief and isn't scheduled at
     * the work-buffer. A thief can steal a partition-buffer to help the partition-thread out.
     * <p/>
     * It can be that the buffer was not scheduled when it got stolen (there was no work and a user thread
     * wants to process its own operation).
     * <p/>
     * But it can also be that the buffer was registered, then got stolen (so the headUnparked changes to StolenUnparked)
     * and while processing the partition-thread skips this partition-buffer (because it is stolen). Then the
     * partition-thread will inform the thief that the partition-buffer isn't scheduled anymore by setting to
     * headUnparked to Stolen. The user thread will see this and than can decide to put the headUnparked back to Empty.
     * Otherwise the final headUnparked will be EmptyScheduled.
     */
    Stolen,

    /**
     *
     */
    StolenUnparked



    //delete this
    ,UnparkedPriority,


}
