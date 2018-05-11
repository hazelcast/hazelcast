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

    public AddAndGetOp(String name, long delta) {
        super(name);
        this.delta = delta;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong(groupId);
        return atomic.addAndGet(delta);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(delta);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        delta = in.readLong();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.ADD_AND_GET_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", delta=").append(delta);
    }
}
