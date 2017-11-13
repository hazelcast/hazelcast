package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

/**
 * TODO: Javadoc Pending...
 */
public class LocalGetOperation extends AbstractAtomicLongOperation {

    public LocalGetOperation() {
        super();
    }

    public LocalGetOperation(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    public Object doRun(long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        if (atomic.commitIndex() < commitIndex) {
            throw new IllegalArgumentException("");
        }
        return atomic.value();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.LOCAL_GET_OP;
    }
}
