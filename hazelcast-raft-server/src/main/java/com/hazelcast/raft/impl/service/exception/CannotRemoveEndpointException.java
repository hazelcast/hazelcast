package com.hazelcast.raft.impl.service.exception;

import com.hazelcast.core.HazelcastException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CannotRemoveEndpointException extends HazelcastException {

    public CannotRemoveEndpointException(String message) {
        super(message, null);
    }

}
