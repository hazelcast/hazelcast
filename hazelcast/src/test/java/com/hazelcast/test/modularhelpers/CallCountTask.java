package com.hazelcast.test.modularhelpers;

import java.io.Serializable;
import java.util.concurrent.Callable;

class CallCountTask implements Serializable, Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        return 1;
    }
}