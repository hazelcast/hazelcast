package com.hazelcast.raft.impl.service.exception;

import com.hazelcast.core.HazelcastException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CannotCreateRaftGroupException extends HazelcastException {
    public CannotCreateRaftGroupException(String message) {
        super(message);
    }
}
