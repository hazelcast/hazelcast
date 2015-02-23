package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;

public abstract class PartitionSpecificCallable<E> implements PartitionSpecificRunnable {
    public static final Object NO_RESPONSE = new Object() {
        @Override
        public String toString() {
            return "NO_RESPONSE";
        }
    };

    private int partitionId;
    private volatile E result;
    private volatile Throwable throwable;

    public PartitionSpecificCallable() {
    }

    public PartitionSpecificCallable(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void run() {
        try {
            result = call();
        } catch (Throwable e) {

        }
    }

    public E getResult() {
        if (throwable != null) {
            throw new RuntimeException("An error was encountered", throwable);
        }
        return result;
    }

    public Throwable getThrowable() {
        if (throwable == null) {
            throw new RuntimeException("Can't getThrowable if there is no throwable. Found value: " + result);
        }
        return throwable;
    }

    public boolean completed() {
        return result != NO_RESPONSE || throwable != null;
    }

    public abstract E call();
}
