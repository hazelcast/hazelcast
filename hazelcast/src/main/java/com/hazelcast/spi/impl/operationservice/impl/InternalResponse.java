package com.hazelcast.spi.impl.operationservice.impl;

final class InternalResponse {

    /**
     * A response indicating the 'null' value.
     */
    static final InternalResponse NULL_RESPONSE = new InternalResponse("Invocation::NULL_RESPONSE");
    /**
     * A response indicating that the operation should be executed again. E.g. because an operation
     * was send to the wrong machine.
     */
    static final InternalResponse RETRY_RESPONSE = new InternalResponse("Invocation::RETRY_RESPONSE");
    /**
     * A response indicating that a timeout has happened.
     */
    static final InternalResponse TIMEOUT_RESPONSE = new InternalResponse("Invocation::TIMEOUT_RESPONSE");
    /**
     * A response indicating that the operation execution was interrupted
     */
    static final InternalResponse INTERRUPTED_RESPONSE = new InternalResponse("Invocation::INTERRUPTED_RESPONSE");
    /**
     * A response indicating that the operation execution was interrupted
     */
    static final InternalResponse BACKPRESSURE_RESPONSE = new InternalResponse("Invocation::FORCED_SYNC_RESPONSE");

    /**
     * Indicating that there currently is no 'result' available. An example is some kind of blocking
     * operation like ILock.lock. If this lock isn't available at the moment, the wait response
     * is returned.
     */
    static final InternalResponse WAIT_RESPONSE = new InternalResponse("Invocation::WAIT_RESPONSE");

    private final String toString;

    InternalResponse(String toString) {
        this.toString = toString;
    }

    @Override
    public String toString() {
        return toString;
    }
}
