package com.hazelcast.spi.impl.operationexecutor.progressive;

import java.util.concurrent.TimeoutException;

public class GetTimeoutException extends TimeoutException {

    public GetTimeoutException(String message) {
        super(message);
    }
}
