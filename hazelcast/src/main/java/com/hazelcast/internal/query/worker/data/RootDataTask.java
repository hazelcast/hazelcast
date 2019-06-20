package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.exec.RootExec;

public class RootDataTask implements DataTask {

    private final int thread;
    private final RootExec rootExec;

    public RootDataTask(int thread, RootExec rootExec) {
        this.thread = thread;
        this.rootExec = rootExec;
    }

    @Override
    public int getThread() {
        return thread;
    }

    public RootExec getRootExec() {
        return rootExec;
    }
}
