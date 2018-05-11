package com.hazelcast.raft.impl.service.exception;

import com.hazelcast.core.HazelcastException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CannotRemoveMemberException extends HazelcastException {

    public CannotRemoveMemberException(String message) {
        super(message, null);
    }

}
