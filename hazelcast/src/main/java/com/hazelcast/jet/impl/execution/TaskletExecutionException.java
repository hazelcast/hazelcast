package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.JetException;
import com.hazelcast.spi.exception.SilentException;

class TaskletExecutionException extends JetException implements SilentException {
    TaskletExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
