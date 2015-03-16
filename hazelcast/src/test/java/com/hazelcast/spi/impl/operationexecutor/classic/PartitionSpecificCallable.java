package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.util.ExceptionUtil;

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
    private volatile Thread callingThread;

    public PartitionSpecificCallable() {
    }

    public PartitionSpecificCallable(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    public Thread getCallingThread(){
        return callingThread;
    }

    public E get() throws Throwable {
        synchronized (this){
            while (result == NO_RESPONSE && throwable == null){
                wait();
            }
        }

        if(result != NO_RESPONSE){
            return result;
        }

        ExceptionUtil.fixRemoteStackTrace(throwable, Thread.currentThread().getStackTrace());
        throw throwable;
    }

    @Override
    public void run() {
        System.out.println("Run");
        callingThread = Thread.currentThread();

        try {
            result = call();
        } catch (Throwable cause) {
            throwable = cause;
        }

        synchronized (this){
            notifyAll();
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
