package com.hazelcast.test;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class ExceptionThrowingCallable implements Callable, Serializable {

    @Override
    public Object call() throws Exception {
        throw new ExpectedRuntimeException();
    }
}
