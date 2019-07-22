package com.hazelcast.cp.internal.raft.exception;

import com.hazelcast.core.HazelcastException;

public class LogValidationException extends HazelcastException {
    public LogValidationException(String message) {
        super(message);
    }
}
