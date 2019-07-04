package com.hazelcast.cp.internal.raft.exception;

import java.io.IOException;

public class LogValidationException extends IOException {
    public LogValidationException(String message) {
        super(message);
    }
}
