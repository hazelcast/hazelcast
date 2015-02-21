package com.hazelcast.spi.impl.operationexecutor.progressive;

public abstract class StressThread extends Thread {
    public Throwable throwable;

    public StressThread(String name) {
        super(name);
    }

    public void run() {
        try {
            doRun();
        } catch (Throwable t) {
            t.printStackTrace();
            throwable = t;
        }
    }

    protected abstract void doRun() throws Throwable;
}
