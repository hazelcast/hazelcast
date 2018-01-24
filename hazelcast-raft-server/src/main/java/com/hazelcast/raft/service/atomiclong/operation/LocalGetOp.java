package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;

/**
 * TODO: Javadoc Pending...
 */
public class LocalGetOp extends AbstractAtomicLongOp {

    public LocalGetOp() {
        super();
    }

    public LocalGetOp(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    @Override
    public Object doRun(long commitIndex) {
        return getAtomicLong().value();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.LOCAL_GET_OP;
    }
}
