package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class AddAndGetOp extends AbstractAtomicLongOp {

    private long delta;

    public AddAndGetOp() {
    }

    public AddAndGetOp(RaftGroupId groupId, String name, long delta) {
        super(groupId, name);
        this.delta = delta;
    }

    @Override
    public Object doRun(long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        return atomic.addAndGet(delta);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(delta);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        delta = in.readLong();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.ADD_AND_GET_OP;
    }
}
