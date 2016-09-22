package com.hazelcast.spi;

public enum OperationReturnStatus {

    /**
     * A nil response indicates that the operation will never return a response. This will be the case for operations that
     * have a 0 call id and therefor there is no invocation needing a response. These are typically internal operations.
     *
     * So once an operation has executed with a NIL_RESPONSE, the operation can be discarded and that is the end of it.
     */
    NIL_RESPONSE,

    /**
     * A response is ready to be send. For example when a simple Map get is executed and read value is ready to be returned.
     *
     * In this case the {@link Operation#getResponse()} is called to obtain the result of the {@link Operation#run()} and will
     * send the response back to the caller.
     */
    RESPONSE_READY,

    BLOCKED,

    /**
     * Not yet used.
     *
     * The MORE_RUNNING_NEEDED can be used to provide operation interleaving in case of 'batch' operations like EntryProcessor
     * operations that runs on a whole partition. It could run for e.g. 10ms and then give up its thread and return
     * MORE_RUNNING_NEEDED to indicate it has more work to do and wants to be rescheduled. Eventually when all data has been
     * processed, it probably returns RESPONSE_READY.
     */
    MORE_RUNNING_NEEDED,

    /**
     * A response is not ready to be send, but at some point it will be. For example the Operation spawns a set of partition
     * operations and when all these partition iterating operations have completed, a response will be send. This will happen
     * at some point in the future.
     */
    DOING_IT_MYSELF
}
