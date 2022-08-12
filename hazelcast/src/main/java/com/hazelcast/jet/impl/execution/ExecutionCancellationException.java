package com.hazelcast.jet.impl.execution;

import java.util.concurrent.CancellationException;

public class ExecutionCancellationException extends CancellationException {
    private final Throwable cause;

    public ExecutionCancellationException(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public synchronized Throwable getCause() {
        return cause;
    }
}
