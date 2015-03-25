package com.hazelcast.spi.impl.operationservice.impl;

final class InternalResponse {

    /**
     * A response indicating the 'null' value.
     */
    static final Object NULL_RESPONSE = new InternalResponse("Invocation::NULL_RESPONSE");

     /**
     * Indicating that there currently is no 'result' available. An example is some kind of blocking
     * operation like ILock.lock. If this lock isn't available at the moment, the wait response
     * is returned.
     */
    static final Object WAIT_RESPONSE = new InternalResponse("Invocation::WAIT_RESPONSE");

    /**
     * A response indicating that a timeout has happened.
     */
    static final Object TIMEOUT_RESPONSE = new InternalResponse("Invocation::TIMEOUT_RESPONSE");

    /**
     * A response indicating that the operation execution was interrupted
     */
    static final Object INTERRUPTED_RESPONSE = new InternalResponse("Invocation::INTERRUPTED_RESPONSE");

    private String toString;

    InternalResponse(String toString) {
        this.toString = toString;
    }

    @Override
    public String toString() {
        return toString;
    }
}
