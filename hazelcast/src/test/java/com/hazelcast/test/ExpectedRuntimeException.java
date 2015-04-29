package com.hazelcast.test;

public class ExpectedRuntimeException extends RuntimeException {
    public ExpectedRuntimeException() {
    }

    public ExpectedRuntimeException(String msg) {
        super(msg);
    }
}
