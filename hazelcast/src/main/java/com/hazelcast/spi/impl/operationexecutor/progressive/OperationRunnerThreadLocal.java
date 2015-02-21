package com.hazelcast.spi.impl.operationexecutor.progressive;


import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

public final class OperationRunnerThreadLocal {

    private static ThreadLocal<Holder> threadLocal = new ThreadLocal<Holder>() {
        @Override
        protected Holder initialValue() {
            return new Holder();
        }
    };

    private OperationRunnerThreadLocal() {
    }

    public static OperationRunner getThreadLocalOperationRunner() {
        return threadLocal.get().runner;
    }

    public static Holder getThreadLocalOperationRunnerHolder(){
        return threadLocal.get();
    }

    public static class Holder {
        public OperationRunner runner;
    }
}
