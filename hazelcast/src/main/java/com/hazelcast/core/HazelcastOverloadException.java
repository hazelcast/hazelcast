package com.hazelcast.core;

/**
 * A {@link com.hazelcast.core.HazelcastInstance} that is thrown when the system won't handle more load due to
 * an overload.
 *
 * This exception is thrown when backpressure is enabled. For more information see
 * {@link com.hazelcast.instance.GroupProperties#BACKPRESSURE_ENABLED}.
 */
public class HazelcastOverloadException extends HazelcastException {

    public HazelcastOverloadException(String message) {
        super(message);
    }
}
