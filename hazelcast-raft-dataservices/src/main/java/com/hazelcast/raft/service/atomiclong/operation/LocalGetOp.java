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

    public LocalGetOp(String name) {
        super(name);
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        return getAtomicLong(groupId).value();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.LOCAL_GET_OP;
    }
}
