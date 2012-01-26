package com.hazelcast.core;

public class DuplicateInstanceNameException extends RuntimeException {

    public DuplicateInstanceNameException() {
        super();
    }

    public DuplicateInstanceNameException(String message) {
        super(message);
    }
}
