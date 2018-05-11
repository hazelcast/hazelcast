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
public class CompareAndSetOp extends AbstractAtomicLongOp {

    private long currentValue;
    private long newValue;

    public CompareAndSetOp() {
    }

    public CompareAndSetOp(String name, long currentValue, long newValue) {
        super(name);
        this.currentValue = currentValue;
        this.newValue = newValue;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong(groupId);
        return atomic.compareAndSet(currentValue, newValue);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(currentValue);
        out.writeLong(newValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        currentValue = in.readLong();
        newValue = in.readLong();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.COMPARE_AND_SET_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", currentValue=").append(currentValue);
        sb.append(", newValue=").append(newValue);
    }
}
