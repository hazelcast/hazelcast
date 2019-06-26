package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.exec.RootExec;

/**
 * The task to get more results from the root executor. Submitted from user thread.
 */
public class RootDataTask implements DataTask {
    /** Root executor. */
    private final RootExec rootExec;

    public RootDataTask(RootExec rootExec) {
        this.rootExec = rootExec;
    }

    RootExec getRootExec() {
        return rootExec;
    }

    @Override
    public int getThread() {
        return rootExec.getThread();
    }
}
