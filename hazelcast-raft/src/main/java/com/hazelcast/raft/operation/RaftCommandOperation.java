package com.hazelcast.raft.operation;

/**
 * Internal {@link RaftOperation} to handle Raft group management tasks.
 */
public abstract class RaftCommandOperation extends RaftOperation {

    @Override
    protected final Object doRun(long commitIndex) {
        throw new UnsupportedOperationException();
    }

}
