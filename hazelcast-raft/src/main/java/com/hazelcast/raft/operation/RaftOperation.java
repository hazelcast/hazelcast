package com.hazelcast.raft.operation;

import com.hazelcast.spi.Operation;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Base operation class for operations to be replicated to and executed on
 * Raft group members.
 * <p>
 * {@code RaftOperation} is stored in Raft log by leader and replicated to followers.
 * When at least majority of the members append it to their logs,
 * the log entry which it belongs is committed and {@code RaftOperation} is executed eventually on each member.
 * <p>
 * Note that, implementations of {@code RaftOperation} must be deterministic.
 * They should perform the same action and produce the same result always,
 * independent of where and when they are executed.
 * <p>
 * {@link #doRun(long)} method must be implemented by subclasses.
 */
public abstract class RaftOperation extends Operation {

    private static final int NA_COMMIT_INDEX = 0;

    private long commitIndex = NA_COMMIT_INDEX;

    private Object response;

    /**
     * Contains actual Raft operation logic. State change represented by this operation should be applied
     * and execution result should be returned to the caller.
     *
     * @param commitIndex commitIndex of the log entry containing this operation
     * @return result of the operation execution
     */
    protected abstract Object doRun(long commitIndex);

    public final RaftOperation setCommitIndex(long commitIndex) {
        checkTrue(commitIndex > NA_COMMIT_INDEX, "Cannot set commit index:" + commitIndex);
        checkTrue(this.commitIndex == NA_COMMIT_INDEX,
                "cannot set commit index: " + commitIndex + " because it is already set to: " + this.commitIndex
                        + " -> " + this);
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
