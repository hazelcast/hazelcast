package com.hazelcast.raft;

import com.hazelcast.spi.Operation;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class RaftOperation extends Operation {

    private static final int NA_COMMIT_INDEX = 0;

    private int commitIndex = NA_COMMIT_INDEX;

    private Object response;

    public abstract Object doRun(int commitIndex);

    public final RaftOperation setCommitIndex(int commitIndex) {
        checkTrue(commitIndex > NA_COMMIT_INDEX, "Cannot set commit index:" + commitIndex);
        checkTrue(this.commitIndex == NA_COMMIT_INDEX, "cannot set commit index: " + commitIndex
                + " because it is already set to: " + this.commitIndex);
        this.commitIndex = commitIndex;
        return this;
    }

    @Override
    public final void run() throws Exception {
        response = doRun(commitIndex);
    }

    @Override
    public final boolean returnsResponse() {
        return true;
    }

    @Override
    public final Object getResponse() {
        return response;
    }

}
